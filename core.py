import io
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pandas as pd
import requests

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json

import config_store
import state_store
from mapping_defaults import DEFAULT_MAPPINGS, get_effective_mapping

logger = logging.getLogger("core")

S3_BUCKET = "aws-micemaster-dbamazon-109116442685-eu-west-2-an"
S3_REGION = "eu-west-2"
IMAGE_UPLOAD_WORKERS = 10

TABLES = {
    "scrapped_hotels": {
        "airtable_table_id": "tbluk7V7jTKggdwCj",
        "rds_table": "lexis_hotels",
        "is_lexis_hotels": True,
        "upsert_keys": ["unique_row_id"],
        "last_modified_field": "Last Modified",
        "amadeus_field": "Scrapped Amadeus Code",
        "image_field": "Hotel Photo Uploads / Captions",
        "s3_path_template": "uploads/Hotels/{amadeus_code}/Hotel_Photo/",
        "label": "Scrapped Hotels",
    },
    "room_category": {
        "airtable_table_id": "tblnZGtNudaIWF2U5",
        "rds_table": "lexis_rooms",
        "is_lexis_rooms": True,
        "upsert_keys": ["amadeus_code", "room_name"],
        "last_modified_field": "Last Modified",
        "amadeus_field": "Amadeus Code (from Hotel)",
        "image_field": "Image Uploads / Captions",
        "s3_path_template": "uploads/scrapped_rooms/{amadeus_code}/{room_name}/",
        "label": "Room Category",
    },
}


def get_db_connection():
    pg_config = config_store.load_config()["pg_config"]
    return psycopg2.connect(**pg_config)


def ensure_sync_state_table(cursor):
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_sync_state (
                table_name TEXT PRIMARY KEY,
                last_loaded_at TIMESTAMPTZ NOT NULL
            )
            """
        )
        cursor.connection.commit()
        return True
    except Exception as e:
        cursor.connection.rollback()
        logger.warning(
            "Could not create/verify etl_sync_state in RDS (%s). "
            "Falling back to local sync_state.json for the watermark. "
            "Ask a DB admin to run the grant script in rds_watermark_setup.sql "
            "to store the watermark in RDS as preferred.", e,
        )
        return False


def get_last_loaded_at(cursor, table_name):
    if ensure_sync_state_table(cursor):
        try:
            cursor.execute(
                "SELECT last_loaded_at FROM etl_sync_state WHERE table_name = %s",
                (table_name,),
            )
            row = cursor.fetchone()
            if row:
                return pd.to_datetime(row[0])
            return None
        except Exception as e:
            cursor.connection.rollback()
            logger.warning("Falling back to local watermark for %s: %s", table_name, e)

    iso_value = state_store.get_last_loaded_at(table_name)
    return pd.to_datetime(iso_value) if iso_value else None


def set_last_loaded_at(cursor, table_name, value):
    if ensure_sync_state_table(cursor):
        try:
            cursor.execute(
                """
                INSERT INTO etl_sync_state (table_name, last_loaded_at)
                VALUES (%s, %s)
                ON CONFLICT (table_name) DO UPDATE SET last_loaded_at = EXCLUDED.last_loaded_at
                """,
                (table_name, value),
            )
            cursor.connection.commit()
            return
        except Exception as e:
            cursor.connection.rollback()
            logger.warning("Falling back to local watermark for %s: %s", table_name, e)

    state_store.set_last_loaded_at(table_name, value.isoformat())


def normalize_amadeus_code(code):
    if not code or not isinstance(code, str):
        return code
    return code.replace(" ", "").strip()


def normalize_link(value):
    if isinstance(value, list):
        value = value[0] if value else None
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


def truncate(value, max_length):
    if isinstance(value, str) and len(value) > max_length:
        return value[:max_length]
    return value


def col(df, field_name, default=None):
    if field_name and field_name in df.columns:
        return df[field_name]
    return pd.Series([default] * len(df), index=df.index)


def test_airtable_connection(api_key, base_id=None):
    base_id = base_id or config_store.load_config()["airtable_base_id"]
    headers = {"Authorization": f"Bearer {api_key}"}
    results = {}
    ok = True
    for name, cfg in TABLES.items():
        url = f"https://api.airtable.com/v0/{base_id}/{cfg['airtable_table_id']}"
        try:
            resp = requests.get(url, headers=headers, params={"pageSize": 1}, timeout=15)
            if resp.status_code == 200:
                results[name] = {"ok": True, "message": "connected"}
            else:
                ok = False
                results[name] = {"ok": False, "message": f"HTTP {resp.status_code}: {resp.text[:200]}"}
        except Exception as e:
            ok = False
            results[name] = {"ok": False, "message": str(e)}
    return {"ok": ok, "tables": results}


def get_airtable_fields(api_key, base_id, table_id):
    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
    resp = requests.get(url, headers=headers, params={"pageSize": 20}, timeout=20)
    resp.raise_for_status()
    records = resp.json().get("records", [])
    fields = set()
    for rec in records:
        fields.update(rec.get("fields", {}).keys())
    fields.add("airtable_record_id")
    return sorted(fields)


def get_rds_columns(rds_table):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (rds_table,),
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{"name": r[0], "type": r[1]} for r in rows]


def fetch_airtable_records(api_key, base_id, table_id, last_modified_field, since=None, progress_cb=None):
    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"

    params = {"pageSize": 100}
    if since is not None:
        since_str = since.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        params["filterByFormula"] = (
            f"IS_AFTER({{{last_modified_field}}}, DATETIME_PARSE('{since_str}'))"
        )

    records = []
    offset = None
    while True:
        req_params = dict(params)
        if offset:
            req_params["offset"] = offset
        resp = requests.get(url, headers=headers, params=req_params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if progress_cb:
            progress_cb(fetched=len(records))
        if not offset:
            break

    return records


def records_to_dataframe(records):
    rows = []
    for rec in records:
        row = dict(rec.get("fields", {}))
        row["airtable_record_id"] = rec.get("id")
        rows.append(row)
    return pd.DataFrame(rows)


class UidLookups:
    def __init__(self):
        self.city_cache = {}
        self.country_cache = {}
        self.global_hotel_chain_cache = {}
        self.hotel_brand_cache = {}
        self.hotel_chain_cache = {}

    def _lookup(self, cache, value, cursor, source_table, source_id_col, source_name_col,
                target_table, target_name_col, target_uid_col="UID"):
        value = normalize_link(value)
        if not value:
            return (None, None)
        if value in cache:
            return cache[value]
        try:
            cursor.execute(
                sql.SQL('SELECT {} FROM {} WHERE {} = %s').format(
                    sql.Identifier(source_name_col), sql.Identifier(source_table),
                    sql.Identifier(source_id_col),
                ),
                (value,),
            )
            row = cursor.fetchone()
            if not row:
                cache[value] = (value, None)
                return cache[value]
            name = row[0]
            cursor.execute(
                sql.SQL('SELECT {} FROM {} WHERE {} = %s LIMIT 1').format(
                    sql.Identifier(target_uid_col), sql.Identifier(target_table),
                    sql.Identifier(target_name_col),
                ),
                (name,),
            )
            row = cursor.fetchone()
            uid = row[0] if row else value
            cache[value] = (uid, name)
            return cache[value]
        except Exception:
            cursor.connection.rollback()
            cache[value] = (value, None)
            return cache[value]

    def city(self, value, cursor):
        return self._lookup(self.city_cache, value, cursor,
                             "itravelle3_cities", "airtable_record_id", "City Name",
                             "Cities", "City Name")

    def country(self, value, cursor):
        return self._lookup(self.country_cache, value, cursor,
                             "itravelle3_countries", "airtable_record_id", "Country Name",
                             "Countries", "Country Name")

    def global_hotel_chain(self, value, cursor):
        return self._lookup(self.global_hotel_chain_cache, value, cursor,
                             "itravelle3_global_hotel_chain", "airtable_record_id", "Hotel Chain",
                             "Global_Hotel_Group", "Group Name")

    def hotel_brand(self, value, cursor):
        return self._lookup(self.hotel_brand_cache, value, cursor,
                             "itravelle3_hotel_brand", "airtable_record_id", "Brand Name",
                             "Hotel_Brands", "Brand Name")

    def hotel_chain(self, value, cursor):
        return self._lookup(self.hotel_chain_cache, value, cursor,
                             "itravelle3_hotel_chain", "airtable_record_id", "Chain Name",
                             "Hotel_Chains", "Chain Name")


def to_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "yes", "1", "checked")
    return bool(value) if value else False


def transform_scrapped_hotels_to_lexis(df, uid_lookups, cursor, mapping):
    if df.empty:
        return df

    amadeus_field = mapping["amadeus_code"]
    df = df.copy()
    df[amadeus_field] = col(df, amadeus_field).apply(normalize_amadeus_code)
    df = df[col(df, amadeus_field).notna() & (col(df, amadeus_field) != "")]
    if df.empty:
        return df

    country_lookup = col(df, mapping["country_id"]).apply(lambda v: uid_lookups.country(v, cursor))
    city_lookup = col(df, mapping["city_id"]).apply(lambda v: uid_lookups.city(v, cursor))
    group_lookup = col(df, mapping["hotel_group_id"]).apply(lambda v: uid_lookups.global_hotel_chain(v, cursor))
    chain_lookup = col(df, mapping["hotel_chain_id"]).apply(lambda v: uid_lookups.hotel_chain(v, cursor))
    brand_lookup = col(df, mapping["hotel_brand_id"]).apply(lambda v: uid_lookups.hotel_brand(v, cursor))

    out = pd.DataFrame(index=df.index)
    out["hotel_id"] = col(df, mapping["hotel_id"])
    out["hotel_name"] = col(df, mapping["hotel_name"])
    out["hotel_location"] = city_lookup.apply(lambda t: t[1])
    out["hotel_coordinates"] = df.apply(
        lambda r: f"{r.get('Latitude')},{r.get('Longtitude')}"
        if pd.notna(r.get("Latitude")) and pd.notna(r.get("Longtitude")) else None,
        axis=1,
    )
    out["website"] = col(df, mapping["website"])
    out["country_id"] = country_lookup.apply(lambda t: t[0])
    out["city_id"] = city_lookup.apply(lambda t: t[0])
    out["verification"] = col(df, mapping["verification"])
    out["amadeus_code"] = col(df, amadeus_field)
    out["hotel_category"] = col(df, mapping["hotel_category"])
    out["room_count"] = pd.to_numeric(col(df, mapping["room_count"]), errors="coerce")
    out["meeting_room_count"] = pd.to_numeric(col(df, mapping["meeting_room_count"]), errors="coerce")
    out["max_capacity"] = pd.to_numeric(col(df, mapping["max_capacity"]), errors="coerce")
    out["cvent_link"] = col(df, mapping["cvent_link"])
    out["hotel_group_id"] = group_lookup.apply(lambda t: t[0])
    out["hotel_group_name"] = group_lookup.apply(lambda t: t[1])
    out["hotel_chain_id"] = chain_lookup.apply(lambda t: t[0])
    out["hotel_chain_name"] = chain_lookup.apply(lambda t: t[1])
    out["hotel_chain_type"] = None
    out["hotel_brand_id"] = brand_lookup.apply(lambda t: t[0])
    out["hotel_brand_name"] = brand_lookup.apply(lambda t: t[1])
    out["hotel_loyalty_program"] = None
    out["aditional_emails"] = None
    out["restaurant_id"] = None
    out["attraction_id"] = None
    out["contract_id"] = None
    out["rating"] = None
    out["internal_comments"] = None
    out["create_time"] = pd.to_datetime(col(df, mapping["create_time"]), errors="coerce")
    out["modify_time"] = pd.to_datetime(col(df, mapping["modify_time"]), errors="coerce")
    out["create_by"] = col(df, mapping["create_by"])
    out["modify_by"] = col(df, mapping["modify_by"])
    out["sublime_preferred_partner"] = False
    out["itravelle_preferred_partner"] = False
    out["source_table"] = "scrapped_hotels"
    out["priority"] = None
    out["unique_row_id"] = out["amadeus_code"]
    out["secondary_city_id"] = col(df, mapping["secondary_city_id"]).apply(normalize_link)
    out["hotel_desc"] = col(df, mapping["hotel_desc"])
    out["hotel_amenities"] = col(df, mapping["hotel_amenities"])
    out["hotel_address"] = col(df, mapping["hotel_address"])
    out["no_of_floors"] = pd.to_numeric(col(df, mapping["no_of_floors"]), errors="coerce")
    out["no_of_restaurants"] = pd.to_numeric(col(df, mapping["no_of_restaurants"]), errors="coerce")
    out["no_of_pools"] = pd.to_numeric(col(df, mapping["no_of_pools"]), errors="coerce")
    out["is_gym"] = col(df, mapping["is_gym"]).apply(to_bool)
    out["phone"] = col(df, mapping["phone"])

    out = out.replace({pd.NA: None})
    out = out.drop_duplicates(subset=["unique_row_id"], keep="last")
    return out


def parse_size_sqm_sqft(raw_size):
    if not raw_size or not isinstance(raw_size, str):
        return (None, None)

    sqm_match = re.search(r"([\d.,]+)\s*(?:sq\s*m|sqm|m2|m²)", raw_size, re.IGNORECASE)
    sqft_match = re.search(r"([\d.,]+)\s*(?:sq\s*ft|sqft|ft2|ft²)", raw_size, re.IGNORECASE)

    def to_float(m):
        if not m:
            return None
        try:
            return float(m.group(1).replace(",", ""))
        except ValueError:
            return None

    return (to_float(sqm_match), to_float(sqft_match))


def transform_room_category_to_lexis_rooms(df, images_by_row, mapping):
    if df.empty:
        return df

    amadeus_field = mapping["amadeus_code"]
    room_name_field = mapping["room_name"]

    df = df.copy()
    df[amadeus_field] = col(df, amadeus_field).apply(normalize_amadeus_code)
    df = df[col(df, amadeus_field).notna() & (col(df, amadeus_field) != "")]
    df = df[col(df, room_name_field).notna() & (col(df, room_name_field) != "")]
    if df.empty:
        return df

    size_parsed = col(df, mapping["raw_size"]).apply(parse_size_sqm_sqft)

    out = pd.DataFrame(index=df.index)
    out["hotel_id"] = col(df, mapping["hotel_id"]).apply(normalize_link)
    out["amadeus_code"] = col(df, amadeus_field)
    out["room_name"] = col(df, room_name_field)
    out["view_type"] = col(df, mapping["view_type"])
    out["hotel_room_code"] = col(df, mapping["hotel_room_code"])
    out["gds_room_code"] = col(df, mapping["gds_room_code"])
    out["beds_layout"] = col(df, mapping["beds_layout"])
    out["bed_type"] = col(df, mapping["bed_type"])
    out["bed_count"] = pd.to_numeric(col(df, mapping["bed_count"]), errors="coerce")
    out["extra_beds"] = col(df, mapping["extra_beds"])
    out["occupancy_adults"] = pd.to_numeric(col(df, mapping["occupancy_adults"]), errors="coerce")
    out["occupancy_children"] = pd.to_numeric(col(df, mapping["occupancy_children"]), errors="coerce")
    out["occupancy_infants"] = pd.to_numeric(col(df, mapping["occupancy_infants"]), errors="coerce")
    out["raw_size"] = col(df, mapping["raw_size"])
    out["size_sqm"] = size_parsed.apply(lambda t: t[0])
    out["size_sqft"] = size_parsed.apply(lambda t: t[1])
    out["bathroom"] = col(df, mapping["bathroom"])
    out["in_room_features"] = col(df, mapping["in_room_features"])
    out["in_room_amenities"] = col(df, mapping["in_room_amenities"])
    out["photos"] = [images_by_row.get(idx, []) for idx in df.index]
    out["hotel_name"] = None
    out["chain_brand"] = col(df, mapping["chain_brand"]).apply(normalize_link)
    out["verified"] = col(df, mapping["verified"]).apply(normalize_link)
    out["image_uploads_captions"] = col(df, mapping["image_uploads_captions"]).apply(
        lambda atts: ", ".join(a.get("filename", "") for a in atts) if isinstance(atts, list) else None
    )
    out["airtable_record_id"] = col(df, "airtable_record_id")
    out["source_id"] = col(df, "airtable_record_id")
    out["create_time"] = pd.to_datetime(col(df, mapping["create_time"]), errors="coerce")
    out["create_by"] = col(df, mapping["create_by"])
    out["modify_time"] = pd.to_datetime(col(df, mapping["modify_time"]), errors="coerce")
    out["modify_by"] = col(df, mapping["modify_by"])

    out = out.replace({pd.NA: None})
    out = out.drop_duplicates(subset=["amadeus_code", "room_name"], keep="last")
    return out


def get_column_length_limits(cursor, rds_table):
    cursor.execute(
        """
        SELECT column_name, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
          AND character_maximum_length IS NOT NULL
        """,
        (rds_table,),
    )
    return {row[0]: row[1] for row in cursor.fetchall()}


def upsert_lexis_hotels(cursor, conn, df, rds_table, upsert_keys):
    df_filtered = df.copy()
    length_limits = get_column_length_limits(cursor, rds_table)
    for c, max_len in length_limits.items():
        if c in df_filtered.columns and max_len:
            df_filtered[c] = df_filtered[c].apply(lambda v, m=max_len: truncate(v, m))

    df_filtered = df_filtered.drop_duplicates(subset=upsert_keys, keep="last")
    if df_filtered.empty:
        return 0

    columns = list(df_filtered.columns)
    insert_columns = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    placeholders = sql.SQL(", ").join(sql.Placeholder() * len(columns))

    query = sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
    ).format(
        sql.Identifier(rds_table),
        insert_columns,
        placeholders,
        sql.SQL(", ").join(sql.Identifier(k) for k in upsert_keys),
        sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in columns if c not in upsert_keys
        ),
    )

    records = [tuple(row) for row in df_filtered[columns].values]
    upserted = 0
    for record in records:
        try:
            cursor.execute(query, record)
            conn.commit()
            upserted += 1
        except Exception as e:
            conn.rollback()
            logger.warning("Skipping record in %s due to error: %s", rds_table, e)

    return upserted


def upsert_lexis_rooms(cursor, conn, df):
    if df.empty:
        return 0

    columns = list(df.columns)
    upserted = 0

    for _, row in df.iterrows():
        values = {c: (Json(row[c]) if c == "photos" else row[c]) for c in columns}
        try:
            cursor.execute(
                """
                SELECT id FROM lexis_rooms
                WHERE amadeus_code = %s AND lower(btrim(room_name)) = lower(btrim(%s))
                LIMIT 1
                """,
                (row["amadeus_code"], row["room_name"]),
            )
            existing = cursor.fetchone()

            if existing:
                room_id = existing[0]
                update_cols = [c for c in columns if c not in ("amadeus_code", "room_name")]
                set_clause = sql.SQL(", ").join(
                    sql.SQL("{} = %s").format(sql.Identifier(c)) for c in update_cols
                )
                cursor.execute(
                    sql.SQL("UPDATE lexis_rooms SET {} WHERE id = %s").format(set_clause),
                    [values[c] for c in update_cols] + [room_id],
                )
            else:
                insert_columns = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
                placeholders = sql.SQL(", ").join(sql.Placeholder() * len(columns))
                cursor.execute(
                    sql.SQL("INSERT INTO lexis_rooms ({}) VALUES ({})").format(insert_columns, placeholders),
                    [values[c] for c in columns],
                )
            conn.commit()
            upserted += 1
        except Exception as e:
            conn.rollback()
            logger.warning("Skipping room record (amadeus_code=%s, room=%s) due to error: %s",
                            row.get("amadeus_code"), row.get("room_name"), e)

    return upserted


def s3_url(key):
    return f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{key}"


def upload_single_image(s3_client, attachment, s3_path_prefix, session):
    url = attachment.get("url")
    filename = attachment.get("filename", "image.jpg")
    if not url:
        return None, "SKIPPED (no url)"

    s3_key = f"{s3_path_prefix}{filename}"

    existing = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_key, MaxKeys=1)
    if existing.get("KeyCount", 0) > 0:
        return s3_key, f"SKIPPED (exists): {s3_key}"

    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    s3_client.upload_fileobj(
        io.BytesIO(resp.content), S3_BUCKET, s3_key,
        ExtraArgs={"ContentType": attachment.get("type", "image/jpeg")},
    )
    return s3_key, f"UPLOADED: {s3_key}"


def upload_images_for_table(df, config, mapping, progress_cb=None):
    image_field = mapping.get("photos") or config["image_field"]
    amadeus_field = mapping["amadeus_code"]
    room_name_field = mapping.get("room_name")
    stats = {"uploaded": 0, "skipped": 0, "failed": 0}
    images_by_row = {}

    if image_field not in df.columns:
        return stats, images_by_row

    s3_client = boto3.client("s3")
    session = requests.Session()
    tasks = []

    for idx, row in df.iterrows():
        attachments = row.get(image_field)
        if not isinstance(attachments, list) or not attachments:
            continue

        amadeus_code = row.get(amadeus_field)
        if not amadeus_code:
            continue

        if "room_name" in config["s3_path_template"]:
            room_name = str(row.get(room_name_field) or "unknown").strip().replace("/", "-")
            s3_path = config["s3_path_template"].format(amadeus_code=amadeus_code, room_name=room_name)
        else:
            s3_path = config["s3_path_template"].format(amadeus_code=amadeus_code)

        for attachment in attachments:
            tasks.append((idx, attachment, s3_path))

    total = len(tasks)
    if not tasks:
        return stats, images_by_row

    done = 0
    with ThreadPoolExecutor(max_workers=IMAGE_UPLOAD_WORKERS) as executor:
        futures = {
            executor.submit(upload_single_image, s3_client, att, path, session): idx
            for idx, att, path in tasks
        }
        for future in as_completed(futures):
            idx = futures[future]
            done += 1
            try:
                s3_key, result = future.result()
                if result.startswith("UPLOADED"):
                    stats["uploaded"] += 1
                elif result.startswith("SKIPPED") and s3_key:
                    stats["skipped"] += 1
                if s3_key:
                    images_by_row.setdefault(idx, []).append(s3_url(s3_key))
            except Exception:
                stats["failed"] += 1
            if progress_cb:
                progress_cb(images_done=done, images_total=total, **stats)

    return stats, images_by_row


def sync_table(table_name, config_dict, uid_lookups, progress_cb=None):
    cfg = TABLES[table_name]
    api_key = config_dict["airtable_api_key"]
    base_id = config_dict["airtable_base_id"]
    overrides = config_dict.get("mapping_overrides", {}).get(cfg["rds_table"], {})
    mapping = get_effective_mapping(cfg["rds_table"], overrides)

    def report(**kwargs):
        if progress_cb:
            progress_cb(table_name, **kwargs)

    report(stage="connecting")
    conn = get_db_connection()
    cursor = conn.cursor()

    since = get_last_loaded_at(cursor, table_name)
    report(stage="fetching", since=str(since) if since else None)

    records = fetch_airtable_records(
        api_key, base_id, cfg["airtable_table_id"], cfg["last_modified_field"], since,
        progress_cb=lambda **kw: report(stage="fetching", **kw),
    )

    if not records:
        report(stage="done", fetched=0, upserted=0, images_uploaded=0)
        cursor.close()
        conn.close()
        return

    report(stage="fetched", fetched=len(records))

    raw_df = records_to_dataframe(records)
    raw_df[cfg["last_modified_field"]] = pd.to_datetime(
        raw_df.get(cfg["last_modified_field"]), errors="coerce"
    )
    amadeus_field = mapping["amadeus_code"]
    if amadeus_field in raw_df.columns:
        raw_df[amadeus_field] = raw_df[amadeus_field].apply(normalize_amadeus_code)

    report(stage="uploading_images")
    _, images_by_row = upload_images_for_table(
        raw_df, cfg, mapping,
        progress_cb=lambda **kw: report(stage="uploading_images", **kw),
    )

    report(stage="transforming")
    if cfg.get("is_lexis_hotels"):
        df = transform_scrapped_hotels_to_lexis(raw_df, uid_lookups, cursor, mapping)
    else:
        df = transform_room_category_to_lexis_rooms(raw_df, images_by_row, mapping)

    if df.empty:
        report(stage="done", fetched=len(records), upserted=0, images_uploaded=0)
        cursor.close()
        conn.close()
        return

    report(stage="upserting", to_upsert=len(df))
    if cfg.get("is_lexis_rooms"):
        upserted = upsert_lexis_rooms(cursor, conn, df)
    else:
        upserted = upsert_lexis_hotels(cursor, conn, df, cfg["rds_table"], cfg["upsert_keys"])

    max_modified = raw_df[cfg["last_modified_field"]].max()
    if pd.notna(max_modified):
        if max_modified.tzinfo is None:
            max_modified = max_modified.tz_localize("UTC")
        set_last_loaded_at(cursor, table_name, max_modified.to_pydatetime())

    report(stage="done", fetched=len(records), upserted=upserted)

    cursor.close()
    conn.close()


def run_full_sync(progress_cb=None, tables=None):
    config_dict = config_store.load_config()
    uid_lookups = UidLookups()
    table_names = tables if tables else list(TABLES.keys())
    for table_name in table_names:
        if table_name not in TABLES:
            continue
        try:
            sync_table(table_name, config_dict, uid_lookups, progress_cb=progress_cb)
        except Exception as e:
            logger.exception("Failed to sync table %s", table_name)
            if progress_cb:
                progress_cb(table_name, stage="error", error=str(e))
