import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from urllib.parse import quote, unquote

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import numpy as np
import pandas as pd
import requests

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, execute_batch, execute_values

import config_store
import state_store
from mapping_defaults import get_effective_mapping

logger = logging.getLogger("core")

S3_BUCKET = "aws-micemaster-dbamazon-109116442685-eu-west-2-an"
S3_REGION = "eu-west-2"
# Existing photo URLs in the DB are served through this CloudFront distribution.
DEFAULT_CDN_BASE_URL = "https://d3p41uxtf016au.cloudfront.net"
IMAGE_UPLOAD_WORKERS = 10
DB_WRITE_CHUNK = 200
# Re-read a few minutes before the watermark: overlap is free because unchanged
# records are skipped, and it guards against clock skew / same-second edits.
WATERMARK_OVERLAP = pd.Timedelta(minutes=10)
# Session-level advisory lock so two real syncs never run against one DB at once
# (lexis_rooms has no unique constraint, so concurrent runs could duplicate rows).
SYNC_ADVISORY_LOCK_ID = 74210931
SCRAPPED_SOURCE = "scrapped_hotels"
# Placeholder codes seen in the data; never used to match or create records.
PLACEHOLDER_AMADEUS = {"", "N/A", "NA", "NONE", "NULL", "-", "TBA", "TBC", "TBD", "0"}
# Only hotels whose "Verified" single select is exactly this value (and rooms whose
# linked hotel(s) all have it) are synced. Other values: "Ready for Verification",
# "Not Yet Verified".
VERIFIED_STATUS = "Verified"
# Airtable formula length is limited, so hotel codes are looked up in batches.
CATCHUP_CODES_PER_REQUEST = 50

TABLES = {
    "scrapped_hotels": {
        "airtable_table_id": "tbluk7V7jTKggdwCj",
        "rds_table": "lexis_hotels",
        "is_lexis_hotels": True,
        "upsert_keys": ["unique_row_id"],
        "last_modified_field": "Last Modified",
        "amadeus_field": "Scrapped Amadeus Code",
        "image_field": "Hotel Photo Uploads / Captions",
        "verified_mapping_key": "verification",  # single select "Verified"
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
        "verified_mapping_key": "verified",  # lookup of the hotel's "Verified"
        "label": "Room Category",
    },
}


@dataclass
class SyncOptions:
    dry_run: bool = False
    full_resync: bool = False


def get_db_connection():
    pg_config = config_store.load_config()["pg_config"]
    return psycopg2.connect(**pg_config)


def db_identity():
    pg = config_store.load_config()["pg_config"]
    return f"{pg.get('host')}:{pg.get('port')}/{pg.get('database')}"


# --------------------------------------------------------------------------
# Watermark (last successful sync time per table)
# --------------------------------------------------------------------------

class Watermarks:
    """Stores the watermark in RDS (etl_sync_state, created if permitted), falling
    back to sync_state.json keyed by DB so a local run never affects a prod run."""

    def __init__(self, conn, dry_run=False):
        self.conn = conn
        self.dry_run = dry_run
        self.db_key = db_identity()
        self.use_rds = self._prepare()

    def _prepare(self):
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT to_regclass('public.etl_sync_state')")
            if cur.fetchone()[0]:
                return True
            if self.dry_run:
                logger.info("etl_sync_state does not exist; a real run will create it.")
                return False
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.etl_sync_state (
                    table_name TEXT PRIMARY KEY,
                    last_loaded_at TIMESTAMPTZ NOT NULL
                )
                """
            )
            self.conn.commit()
            logger.info("Created etl_sync_state table to store the sync watermark.")
            return True
        except Exception as e:
            self.conn.rollback()
            logger.warning(
                "Cannot use etl_sync_state in RDS (%s); storing the watermark locally in "
                "sync_state.json for %s instead.", e, self.db_key,
            )
            return False
        finally:
            cur.close()

    def get(self, table_name):
        if self.use_rds:
            cur = self.conn.cursor()
            try:
                cur.execute(
                    "SELECT last_loaded_at FROM etl_sync_state WHERE table_name = %s",
                    (table_name,),
                )
                row = cur.fetchone()
                if row:
                    return _to_utc(row[0])
            except Exception as e:
                self.conn.rollback()
                logger.warning("Reading watermark from RDS failed for %s: %s", table_name, e)
            finally:
                cur.close()
        iso_value = state_store.get_last_loaded_at(self.db_key, table_name)
        return _to_utc(iso_value) if iso_value else None

    def set(self, table_name, value):
        if self.dry_run:
            return
        if self.use_rds:
            cur = self.conn.cursor()
            try:
                cur.execute(
                    """
                    INSERT INTO etl_sync_state (table_name, last_loaded_at)
                    VALUES (%s, %s)
                    ON CONFLICT (table_name) DO UPDATE SET last_loaded_at = EXCLUDED.last_loaded_at
                    """,
                    (table_name, value.to_pydatetime()),
                )
                self.conn.commit()
                return
            except Exception as e:
                self.conn.rollback()
                logger.warning("Writing watermark to RDS failed for %s (%s); saving locally.", table_name, e)
            finally:
                cur.close()
        state_store.set_last_loaded_at(self.db_key, table_name, value.isoformat())


# --------------------------------------------------------------------------
# Value helpers
# --------------------------------------------------------------------------

def _to_utc(value):
    if value is None:
        return None
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        return None
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def is_newer(airtable_ts, db_ts):
    """True when Airtable's modified time is later than the DB's (second precision)."""
    at = _to_utc(airtable_ts)
    if at is None:
        return False
    db = _to_utc(db_ts)
    if db is None:
        return True
    return at.floor("s") > db.floor("s")


def normalize_amadeus_code(code):
    # Lookup fields such as "Amadeus Code (from Hotel)" come back from Airtable
    # as lists (e.g. ["LON ABC"]); unwrap to the first value.
    if isinstance(code, list):
        code = next((c for c in code if c), None)
    if not code or not isinstance(code, str):
        return code
    return code.replace(" ", "").strip()


def amadeus_key(code):
    """Matching key for an Amadeus code, or None for empty / placeholder codes."""
    code = normalize_amadeus_code(code)
    if not isinstance(code, str):
        return None
    code = code.strip().upper()
    return None if code in PLACEHOLDER_AMADEUS else code


def room_name_key(name):
    if isinstance(name, list):
        name = next((n for n in name if n), None)
    if not isinstance(name, str):
        return None
    key = " ".join(name.split()).lower()
    return key or None


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


def to_sql_value(value):
    """Convert pandas/Airtable values into something psycopg2 can adapt.

    - Missing values (None/NaN/NaT/pd.NA) -> None; numpy scalars -> Python
    - Collaborator fields (Created By / Last Modified By) -> name (or email)
    - AI / computed fields shaped like {"value": ...} -> the inner value
    - Lists of plain values -> comma-separated text; other dicts/lists -> JSON
    """
    if value is None:
        return None
    if isinstance(value, dict):
        if "name" in value or "email" in value:
            return value.get("name") or value.get("email")
        if "value" in value:
            return to_sql_value(value["value"])
        return Json(value)
    if isinstance(value, (list, tuple)):
        flattened = [to_sql_value(v) for v in value]
        if all(v is None or isinstance(v, (str, int, float, bool)) for v in flattened):
            return ", ".join(str(v) for v in flattened if v is not None and v != "")
        return Json(list(value))
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.to_pydatetime()
    if isinstance(value, np.generic):
        value = value.item()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def truncate(value, max_length):
    if isinstance(value, str) and len(value) > max_length:
        return value[:max_length]
    return value


def col(df, field_name, default=None):
    if field_name and field_name in df.columns:
        return df[field_name]
    return pd.Series([default] * len(df), index=df.index, dtype=object)


def _unique(items):
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


# --------------------------------------------------------------------------
# Airtable
# --------------------------------------------------------------------------

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


def _airtable_get(session, url, headers, params, retries=6):
    """GET with backoff on Airtable rate limiting (429) and transient 5xx errors."""
    for attempt in range(retries):
        resp = session.get(url, headers=headers, params=params, timeout=60)
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = min(30, 2 ** attempt)
            logger.info("Airtable returned %s; retrying in %ss", resp.status_code, wait)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    return resp.json()


def _formula_field(name):
    # Airtable field references are {Name}; a literal } inside the name is escaped.
    return "{" + name.replace("}", "\\}") + "}"


def _formula_string(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _since_formula(last_modified_field, since):
    since_str = _to_utc(since).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return f"IS_AFTER({_formula_field(last_modified_field)}, DATETIME_PARSE('{since_str}'))"


def verified_formula(cfg, mapping):
    """Airtable formula matching only verified records (mirrors is_verified)."""
    field_ref = _formula_field(mapping[cfg["verified_mapping_key"]])
    if cfg.get("is_lexis_rooms"):
        # Lookup of the linked hotel(s): every linked hotel must be Verified.
        return (f"REGEX_MATCH(ARRAYJOIN({field_ref}, '|'), "
                f"'^{VERIFIED_STATUS}(\\\\|{VERIFIED_STATUS})*$')")
    return f"{field_ref} = {_formula_string(VERIFIED_STATUS)}"


def is_verified(value):
    """True for "Verified" (single select) or a non-empty lookup list whose values
    are all "Verified". Anything else ("Not Yet Verified", blank, ...) is False."""
    if isinstance(value, dict) and "name" in value:
        value = value["name"]
    if isinstance(value, (list, tuple)):
        return bool(value) and all(is_verified(v) for v in value)
    return isinstance(value, str) and value.strip().lower() == VERIFIED_STATUS.lower()


def _and_formulas(*formulas):
    parts = [f for f in formulas if f]
    if not parts:
        return None
    return parts[0] if len(parts) == 1 else "AND(" + ", ".join(parts) + ")"


def fetch_airtable_records(api_key, base_id, table_id, last_modified_field, since=None, progress_cb=None,
                           extra_formula=None, fields=None):
    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"

    params = {"pageSize": 100}
    if fields:
        params["fields[]"] = list(fields)
    formula = _and_formulas(
        _since_formula(last_modified_field, since) if since is not None else None,
        extra_formula,
    )
    if formula:
        params["filterByFormula"] = formula

    session = requests.Session()
    records = []
    offset = None
    while True:
        req_params = dict(params)
        if offset:
            req_params["offset"] = offset
        data = _airtable_get(session, url, headers, req_params)
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if progress_cb:
            progress_cb(fetched=len(records))
        if not offset:
            break

    return records


def fetch_rooms_of_newly_verified_hotels(config_dict, room_cfg, room_mapping, since, progress_cb=None):
    """Rooms only see their hotel's status through the "Verified (from Hotel)" lookup,
    and changing a lookup does NOT bump the room's "Last Modified". So when a hotel is
    verified after its rooms were last edited, an incremental (watermark) fetch would
    never pick those rooms up. Find verified hotels modified since the watermark and
    fetch all of their verified rooms regardless of the rooms' own modify time."""
    api_key = config_dict["airtable_api_key"]
    base_id = config_dict["airtable_base_id"]
    hotel_cfg = TABLES["scrapped_hotels"]
    hotel_overrides = config_dict.get("mapping_overrides", {}).get(hotel_cfg["rds_table"], {})
    hotel_mapping = get_effective_mapping(hotel_cfg["rds_table"], hotel_overrides)
    hotel_code_field = hotel_mapping["amadeus_code"]

    hotels = fetch_airtable_records(
        api_key, base_id, hotel_cfg["airtable_table_id"], hotel_cfg["last_modified_field"], since,
        extra_formula=verified_formula(hotel_cfg, hotel_mapping), fields=[hotel_code_field],
    )
    # Raw values (not normalized) so they match the rooms' lookup text exactly.
    codes = _unique(
        c for c in (h.get("fields", {}).get(hotel_code_field) for h in hotels)
        if isinstance(c, str) and normalize_amadeus_code(c)
    )
    if not codes:
        return []

    room_code_ref = _formula_field(room_mapping["amadeus_code"])
    room_verified = verified_formula(room_cfg, room_mapping)
    records = []
    for start in range(0, len(codes), CATCHUP_CODES_PER_REQUEST):
        chunk = codes[start:start + CATCHUP_CODES_PER_REQUEST]
        match_any = "OR(" + ", ".join(
            f"FIND({_formula_string('|' + c + '|')}, '|' & ARRAYJOIN({room_code_ref}, '|') & '|')"
            for c in chunk
        ) + ")"
        records.extend(fetch_airtable_records(
            api_key, base_id, room_cfg["airtable_table_id"], room_cfg["last_modified_field"], None,
            extra_formula=_and_formulas(room_verified, match_any),
        ))
        if progress_cb:
            progress_cb(catchup_rooms=len(records))
    logger.info("%d verified hotel(s) changed since the watermark; re-checked %d of their rooms.",
                len(codes), len(records))
    return records


def drop_unverified(raw_df, cfg, mapping):
    """Safety net in case the Airtable formula filter lets anything through."""
    field_name = mapping[cfg["verified_mapping_key"]]
    mask = col(raw_df, field_name).apply(is_verified).astype(bool)
    return raw_df[mask], int((~mask).sum())


def records_to_dataframe(records):
    rows = []
    for rec in records:
        row = dict(rec.get("fields", {}))
        row["airtable_record_id"] = rec.get("id")
        rows.append(row)
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------
# Lookups and transforms
# --------------------------------------------------------------------------

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
    out["source_table"] = SCRAPPED_SOURCE
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


def transform_room_category_to_lexis_rooms(df, mapping):
    """Photos are filled in later from the image resolver (placeholder [] here)."""
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
    out["photos"] = [[] for _ in df.index]
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


# --------------------------------------------------------------------------
# Planning: compare Airtable records with what already exists in the DB
# --------------------------------------------------------------------------

@dataclass
class SyncPlan:
    inserts: list = field(default_factory=list)       # raw_df indexes
    updates: list = field(default_factory=list)       # (raw_df index, existing db row)
    photo_repairs: list = field(default_factory=list)  # (raw_df index, existing db row)
    unchanged: int = 0
    skipped_other_source: int = 0
    invalid: int = 0
    duplicates_in_source: int = 0


def _modified_series(raw_df, mapping, cfg):
    field_name = mapping.get("modify_time") or cfg["last_modified_field"]
    return pd.to_datetime(col(raw_df, field_name), errors="coerce", utc=True)


def _dedupe_by_key(entries, modified):
    """entries: [(idx, key, ...)] -> keep the most recently modified per key."""
    floor = pd.Timestamp("1900-01-01", tz="UTC")
    ordered = sorted(
        entries,
        key=lambda e: modified[e[0]] if pd.notna(modified[e[0]]) else floor,
        reverse=True,
    )
    best = {}
    for entry in ordered:
        best.setdefault(entry[1], entry)
    return list(best.values()), len(entries) - len(best)


def _attachments(raw_df, idx, image_field):
    if image_field not in raw_df.columns:
        return []
    value = raw_df.at[idx, image_field]
    if not isinstance(value, list):
        return []
    return [a for a in value if isinstance(a, dict) and a.get("url")]


def _distinct_photo_count(attachments):
    return len({_norm_filename(a.get("filename") or "image.jpg") for a in attachments})


def load_room_index(cursor):
    cursor.execute(
        """
        SELECT id, airtable_record_id, amadeus_code, room_name, modify_time,
               CASE WHEN jsonb_typeof(photos) = 'array' THEN jsonb_array_length(photos) ELSE 0 END
        FROM lexis_rooms
        """
    )
    by_recid, by_key = {}, {}
    total = 0
    for rid, recid, amadeus, name, modified, photo_count in cursor.fetchall():
        total += 1
        row = {"id": rid, "recid": recid, "amadeus": amadeus_key(amadeus),
               "name": room_name_key(name), "modify_time": modified, "photo_count": photo_count}
        if recid:
            by_recid.setdefault(recid, []).append(row)
        if row["amadeus"] and row["name"]:
            by_key.setdefault((row["amadeus"], row["name"]), []).append(row)
    dup_keys = sum(1 for rows in by_key.values() if len(rows) > 1)
    return by_recid, by_key, total, dup_keys


def plan_rooms(cursor, raw_df, mapping, cfg, image_field):
    plan = SyncPlan()
    modified = _modified_series(raw_df, mapping, cfg)
    am_col = col(raw_df, mapping["amadeus_code"])
    name_col = col(raw_df, mapping["room_name"])

    entries = []
    for idx in raw_df.index:
        am, name = amadeus_key(am_col[idx]), room_name_key(name_col[idx])
        if not am or not name:
            plan.invalid += 1
            continue
        entries.append((idx, (am, name)))
    entries, plan.duplicates_in_source = _dedupe_by_key(entries, modified)

    by_recid, by_key, total, dup_keys = load_room_index(cursor)
    logger.info("lexis_rooms has %d rows (%d hotel+room-name keys already duplicated; left untouched).",
                total, dup_keys)

    claimed = set()
    matches = {}
    # Pass 1: match on Airtable record id (survives room renames).
    unmatched = []
    for idx, key in entries:
        recid = raw_df.at[idx, "airtable_record_id"]
        candidates = [r for r in by_recid.get(recid, []) if r["id"] not in claimed]
        if candidates:
            row = max(candidates, key=lambda r: ((r["amadeus"], r["name"]) == key, r["id"]))
            claimed.add(row["id"])
            matches[idx] = row
        else:
            unmatched.append((idx, key))
    # Pass 2: match on hotel Amadeus code + room name (case/whitespace-insensitive).
    for idx, key in unmatched:
        candidates = [r for r in by_key.get(key, []) if r["id"] not in claimed]
        if candidates:
            row = max(candidates, key=lambda r: r["id"])
            claimed.add(row["id"])
            matches[idx] = row
        else:
            matches[idx] = None

    for idx, row in matches.items():
        if row is None:
            plan.inserts.append(idx)
        elif is_newer(modified[idx], row["modify_time"]):
            plan.updates.append((idx, row))
        elif _distinct_photo_count(_attachments(raw_df, idx, image_field)) > (row["photo_count"] or 0):
            # Unchanged in Airtable but the DB is missing photos (e.g. an earlier
            # upload failed or old data had none): only refresh the photos column.
            plan.photo_repairs.append((idx, row))
        else:
            plan.unchanged += 1
    return plan


def load_hotel_index(cursor):
    cursor.execute(
        "SELECT unique_row_id, hotel_id, amadeus_code, source_table, modify_time::timestamptz FROM lexis_hotels"
    )
    by_uid, scrapped_by_recid, scrapped_by_amadeus, other_amadeus = {}, {}, {}, set()
    for uid, hotel_id, amadeus, source, modified in cursor.fetchall():
        row = {"uid": uid, "hotel_id": hotel_id, "amadeus": amadeus_key(amadeus),
               "source": source, "modify_time": modified}
        by_uid[str(uid).upper()] = row
        if source == SCRAPPED_SOURCE:
            if hotel_id:
                scrapped_by_recid[hotel_id] = row
            if row["amadeus"]:
                scrapped_by_amadeus.setdefault(row["amadeus"], row)
        elif row["amadeus"]:
            other_amadeus.add(row["amadeus"])
    return by_uid, scrapped_by_recid, scrapped_by_amadeus, other_amadeus


def plan_hotels(cursor, raw_df, mapping, cfg):
    plan = SyncPlan()
    modified = _modified_series(raw_df, mapping, cfg)
    am_col = col(raw_df, mapping["amadeus_code"])

    entries = []
    for idx in raw_df.index:
        am = amadeus_key(am_col[idx])
        if not am:
            plan.invalid += 1
            continue
        entries.append((idx, am))
    entries, plan.duplicates_in_source = _dedupe_by_key(entries, modified)

    by_uid, scrapped_by_recid, scrapped_by_amadeus, other_amadeus = load_hotel_index(cursor)
    claimed = set()
    for idx, am in entries:
        recid = raw_df.at[idx, "airtable_record_id"]
        row = by_uid.get(am)
        if row and row["source"] != SCRAPPED_SOURCE:
            plan.skipped_other_source += 1
            continue
        row = row or scrapped_by_recid.get(recid) or scrapped_by_amadeus.get(am)
        if row:
            if row["uid"] in claimed:
                plan.duplicates_in_source += 1
                continue
            claimed.add(row["uid"])
            if is_newer(modified[idx], row["modify_time"]):
                plan.updates.append((idx, row))
            else:
                plan.unchanged += 1
        elif am in other_amadeus:
            # The hotel already exists from another source (main_hotels, manual, ...).
            plan.skipped_other_source += 1
        else:
            plan.inserts.append(idx)
    return plan


# --------------------------------------------------------------------------
# S3 images
# --------------------------------------------------------------------------

def get_s3_client(config_dict):
    """Build an S3 client from config.json's optional "aws" section, falling back
    to boto3's default credential chain (~/.aws/credentials, env vars, IAM role)."""
    aws = config_dict.get("aws") or {}
    kwargs = {"region_name": aws.get("region") or S3_REGION}
    if aws.get("access_key_id") and aws.get("secret_access_key"):
        kwargs["aws_access_key_id"] = aws["access_key_id"]
        kwargs["aws_secret_access_key"] = aws["secret_access_key"]
        if aws.get("session_token"):
            kwargs["aws_session_token"] = aws["session_token"]
    elif aws.get("profile"):
        return boto3.Session(profile_name=aws["profile"]).client("s3", region_name=kwargs["region_name"])
    return boto3.client("s3", **kwargs)


def check_s3_access(s3_client):
    """Fail fast with a readable error instead of silently failing every upload."""
    try:
        s3_client.list_objects_v2(Bucket=S3_BUCKET, MaxKeys=1)
    except NoCredentialsError:
        raise RuntimeError(
            "No AWS credentials found, so images cannot be uploaded to S3. "
            "Add an \"aws\" section (access_key_id / secret_access_key) to config.json "
            "or run 'aws configure'."
        )
    except ClientError as e:
        raise RuntimeError(f"Cannot access S3 bucket {S3_BUCKET}: {e}")


def cdn_url(cdn_base, key):
    return f"{cdn_base.rstrip('/')}/{quote(key, safe='/')}"


def _norm_filename(name):
    return unquote(str(name)).strip().replace(" ", "_").lower()


def _filename_variants(filename):
    name = str(filename).replace("/", "-")
    return _unique([name, name.replace(" ", "_")])


def room_image_folders(amadeus_code, room_name):
    """(folder for new uploads, all folders that may already hold this room's images).

    The existing pipeline stores rooms under underscored names (the ones referenced
    by CloudFront URLs in lexis_rooms); earlier runs of this tool used the raw name
    with spaces. Both are checked so existing images are reused, never duplicated.
    """
    name = str(room_name).strip()
    base = f"uploads/scrapped_rooms/{amadeus_code}/"
    canonical = base + name.replace(" ", "_").replace("/", "-") + "/"
    return canonical, _unique([
        canonical,
        base + name.replace(" ", "_") + "/",   # existing pipeline nests on "/"
        base + name.replace("/", "-") + "/",   # earlier versions of this tool
    ])


def hotel_image_folders(record_id, amadeus_code):
    canonical = f"uploads/Hotels/{record_id}/Hotel_Photo/"
    return canonical, _unique([canonical, f"uploads/Hotels/{amadeus_code}/Hotel_Photo/"])


@dataclass
class ImageJob:
    idx: object
    attachments: list
    canonical_folder: str
    folders: list
    underscore_filenames: bool
    existing_urls: list = field(default_factory=list)
    record_id: str = None
    # S3 prefixes to list so that every candidate folder is covered. Rooms list the
    # whole hotel folder once (shared by all its rooms) instead of one call per room.
    list_prefixes: list = field(default_factory=list)


def _list_keys(s3_client, prefix):
    """{key: size} for every object under prefix (one listing, no downloads)."""
    objects = {}
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects[obj["Key"]] = obj.get("Size")
    return objects


def _object_exists(s3_client, key):
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


_AIRTABLE_EXPIRY_RE = re.compile(r"/(\d{13})/")
LINK_EXPIRY_MARGIN_MS = 120_000


def _link_expired(url):
    """Airtable attachment URLs embed their expiry (epoch ms) in the path and stop
    working ~2 hours after the records were fetched."""
    m = _AIRTABLE_EXPIRY_RE.search(url or "")
    return bool(m) and int(m.group(1)) - LINK_EXPIRY_MARGIN_MS < time.time() * 1000


class AttachmentLinks:
    """Hands out a working download URL for an attachment, re-fetching its record
    from Airtable when the URL from the original fetch has expired (long runs,
    or the computer having slept mid-run)."""

    def __init__(self, api_key, base_id, table_id, image_field):
        self._url = f"https://api.airtable.com/v0/{base_id}/{table_id}/"
        self._headers = {"Authorization": f"Bearer {api_key}"}
        self._field = image_field
        self._session = requests.Session()
        self._cache = {}  # record id -> {attachment id: url}
        self._lock = threading.Lock()

    def url(self, record_id, attachment, force_refresh=False):
        current = attachment.get("url")
        if not force_refresh and not _link_expired(current):
            return current
        if not record_id or not attachment.get("id"):
            return current
        with self._lock:
            cached = self._cache.get(record_id, {}).get(attachment["id"])
            if cached and cached != current and not _link_expired(cached):
                return cached
            data = _airtable_get(self._session, self._url + record_id, self._headers, {})
            fresh = {a.get("id"): a.get("url") for a in (data.get("fields", {}).get(self._field) or [])
                     if isinstance(a, dict)}
            self._cache[record_id] = fresh
        if attachment["id"] not in fresh:
            raise RuntimeError(f"attachment {attachment['id']} no longer on record {record_id}")
        return fresh[attachment["id"]]


def _download(session, links, record_id, attachment, retries=4):
    """Download an attachment, refreshing an expired link and retrying transient
    failures (5xx, dropped connections)."""
    force_refresh = False
    for attempt in range(retries):
        url = links.url(record_id, attachment, force_refresh) if links else attachment["url"]
        try:
            resp = session.get(url, timeout=60)
        except requests.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)
            continue
        if resp.ok:
            return resp.content, resp.headers.get("Content-Type")
        if resp.status_code in (401, 403, 404, 410):
            force_refresh = True  # expired or revoked link; fetch a fresh one
        elif resp.status_code < 500 and resp.status_code != 429:
            resp.raise_for_status()
        if attempt == retries - 1:
            resp.raise_for_status()
        time.sleep(2 ** attempt)


def _process_image(s3_client, session, links, record_id, attachment, target_key, dry_run):
    """Upload an image that isn't in S3 yet. Never overwrites an existing object.
    Returns (status, key) with status in reused_s3 | uploaded | would_upload."""
    if dry_run:
        return "would_upload", target_key
    if _object_exists(s3_client, target_key):
        return "reused_s3", target_key
    content, content_type = _download(session, links, record_id, attachment)
    s3_client.put_object(
        Bucket=S3_BUCKET, Key=target_key, Body=content,
        ContentType=attachment.get("type") or content_type or "image/jpeg",
    )
    return "uploaded", target_key


def resolve_images(jobs, s3_client, cdn_base, dry_run, progress_cb=None, links=None):
    """Return ({stats}, {idx: (urls, had_failure)}).

    For each attachment, in order: reuse the URL already stored in the DB row; else
    reuse an object with a matching filename in the record's folders; else reuse an
    object of exactly the same byte size in those folders (earlier uploaders saved
    some images under other names), then anywhere in the hotel's folder; else upload it to the canonical folder (not in
    dry-run). Only images that get uploaded are downloaded. Nothing is overwritten.
    """
    stats = {"reused_db": 0, "reused_s3": 0, "uploaded": 0, "would_upload": 0, "failed": 0}
    by_name = by_size = by_hotel = 0
    slots_by_job = {}
    failed_jobs = set()
    pending = []

    for job in jobs:
        db_urls = {}
        for url in job.existing_urls or []:
            if isinstance(url, str) and url:
                db_urls.setdefault(_norm_filename(url.split("?")[0].rsplit("/", 1)[-1]), url)
        slots = []
        for att in job.attachments:
            filename = att.get("filename") or "image.jpg"
            hit = db_urls.get(_norm_filename(filename))
            if hit:
                stats["reused_db"] += 1
            else:
                pending.append((job, len(slots), att, filename))
            slots.append(hit)
        slots_by_job[job.idx] = slots

    prefixes = _unique(p for job, *_ in pending for p in (job.list_prefixes or job.folders))
    listings = {}
    if prefixes:
        if progress_cb:
            progress_cb(stage="checking_images", folders_total=len(prefixes))
        with ThreadPoolExecutor(max_workers=IMAGE_UPLOAD_WORKERS) as executor:
            futures = {executor.submit(_list_keys, s3_client, p): p for p in prefixes}
            for future in as_completed(futures):
                prefix = futures[future]
                try:
                    listings[prefix] = future.result()
                except Exception as e:
                    listings[prefix] = None
                    logger.warning("Could not list S3 folder %s: %s", prefix, e)

    # Index every listed object once: key -> size, and folder -> its direct children.
    sizes, children, by_prefix_size = {}, {}, {}
    for prefix, listing in listings.items():
        for key, size in (listing or {}).items():
            sizes[key] = size
            children.setdefault(key[:key.rfind("/") + 1], []).append(key)
            if "/_thumbs/" not in key:
                by_prefix_size.setdefault((prefix, size), []).append(key)
    for keys in list(children.values()) + list(by_prefix_size.values()):
        keys.sort()

    claimed = {}  # job idx -> keys already used by that record (one key per attachment)
    work = {}     # target key -> (record id, attachment, [(job idx, slot)])
    for job, slot, att, filename in pending:
        job_listings = [listings.get(p) for p in (job.list_prefixes or job.folders)]
        if any(objects is None for objects in job_listings):
            # Can't prove the image is absent, so don't risk uploading a duplicate.
            stats["failed"] += 1
            failed_jobs.add(job.idx)
            continue
        used = claimed.setdefault(job.idx, set())

        found = next(
            (f + v for f in job.folders for v in _filename_variants(filename)
             if f + v in sizes and f + v not in used),
            None,
        )
        if found:
            by_name += 1
        else:
            size = att.get("size")
            if size:
                # Folders are in priority order (canonical first); only direct children,
                # so thumbnails and other rooms' nested folders never match.
                found = next(
                    (key for f in job.folders for key in children.get(f, ())
                     if sizes[key] == size and key not in used),
                    None,
                )
                if found:
                    by_size += 1
                else:
                    # Same image already stored for another room of this hotel (e.g. a
                    # renamed room, or a shared photo). Prefer one with the same filename.
                    norm = _norm_filename(filename)
                    candidates = [k for p in (job.list_prefixes or job.folders)
                                  for k in by_prefix_size.get((p, size), ()) if k not in used]
                    found = next((k for k in candidates if _norm_filename(k.rsplit("/", 1)[-1]) == norm),
                                 candidates[0] if candidates else None)
                    if found:
                        by_hotel += 1
        if found:
            used.add(found)
            slots_by_job[job.idx][slot] = cdn_url(cdn_base, found)
            stats["reused_s3"] += 1
            continue

        name = filename.replace("/", "-")
        if job.underscore_filenames:
            name = name.replace(" ", "_")
        target = job.canonical_folder + name
        if target not in work:
            work[target] = (job.record_id, att, [])
        work[target][2].append((job.idx, slot))

    if pending:
        logger.info("S3 matches: %d by filename, %d by identical size in the record's folder, "
                    "%d by identical size elsewhere in the hotel's folder", by_name, by_size, by_hotel)

    if work:
        session = requests.Session()
        total, done = len(work), 0
        with ThreadPoolExecutor(max_workers=IMAGE_UPLOAD_WORKERS) as executor:
            futures = {
                executor.submit(_process_image, s3_client, session, links, record_id, att, key, dry_run): key
                for key, (record_id, att, _) in work.items()
            }
            for future in as_completed(futures):
                targets = work[futures[future]][2]
                done += 1
                try:
                    status, key = future.result()
                    stats[status] += 1
                    for idx, slot in targets:
                        slots_by_job[idx][slot] = cdn_url(cdn_base, key)
                except Exception as e:
                    stats["failed"] += 1
                    failed_jobs.update(idx for idx, _ in targets)
                    logger.warning("Image processing failed for %s: %s", futures[future], e)
                if progress_cb and (done % 50 == 0 or done == total):
                    progress_cb(stage="uploading_images", images_done=done, images_total=total,
                                images_uploaded=stats["uploaded"])

    results = {
        idx: (_unique(u for u in slots if u), idx in failed_jobs)
        for idx, slots in slots_by_job.items()
    }
    return stats, results


# --------------------------------------------------------------------------
# Writing
# --------------------------------------------------------------------------

def _execute_chunk(cursor, query, params_list, values_mode):
    if values_mode:
        execute_values(cursor, query, params_list, page_size=len(params_list))
    else:
        execute_batch(cursor, query, params_list, page_size=100)


def write_batches(conn, cursor, query, rows, values_mode, dry_run):
    """rows: [(label, params)]. Commits in chunks; a failing chunk is retried row by
    row so one bad record doesn't block the rest. Returns (ok, failed)."""
    if not rows:
        return 0, 0
    if dry_run:
        return len(rows), 0
    ok = failed = 0
    for start in range(0, len(rows), DB_WRITE_CHUNK):
        chunk = rows[start:start + DB_WRITE_CHUNK]
        try:
            _execute_chunk(cursor, query, [p for _, p in chunk], values_mode)
            conn.commit()
            ok += len(chunk)
        except Exception as e:
            conn.rollback()
            logger.warning("Batch write failed (%s); retrying %d rows individually.", e, len(chunk))
            for label, params in chunk:
                try:
                    _execute_chunk(cursor, query, [params], values_mode)
                    conn.commit()
                    ok += 1
                except Exception as row_error:
                    conn.rollback()
                    failed += 1
                    logger.warning("Skipping %s: %s", label, row_error)
    return ok, failed


def _update_rows(conn, cursor, rds_table, where_col, rows, dry_run):
    """rows: [(label, {col: value}, where_value)], grouped by column set."""
    groups = {}
    for label, values, where_value in rows:
        cols = tuple(values.keys())
        groups.setdefault(cols, []).append((label, tuple(values[c] for c in cols) + (where_value,)))
    ok = failed = 0
    for cols, group_rows in groups.items():
        query = sql.SQL("UPDATE {} SET {} WHERE {} = %s").format(
            sql.Identifier(rds_table),
            sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(c)) for c in cols),
            sql.Identifier(where_col),
        )
        o, f = write_batches(conn, cursor, query, group_rows, values_mode=False, dry_run=dry_run)
        ok += o
        failed += f
    return ok, failed


def _insert_query(rds_table, columns, on_conflict=None):
    query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
        sql.Identifier(rds_table),
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
    )
    if on_conflict:
        query = query + sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(sql.Identifier(on_conflict))
    return query


def _room_label(row):
    return f"room (amadeus_code={row.get('amadeus_code')}, room={row.get('room_name')})"


def write_rooms(conn, cursor, df, plan, image_results, dry_run):
    columns = list(df.columns)
    insert_rows = []
    for idx in plan.inserts:
        if idx not in df.index:
            continue
        row = df.loc[idx]
        urls, _ = image_results.get(idx, ([], False))
        values = [Json(urls) if c == "photos" else to_sql_value(row[c]) for c in columns]
        insert_rows.append((_room_label(row), tuple(values)))
    inserted, insert_failed = write_batches(
        conn, cursor, _insert_query("lexis_rooms", columns), insert_rows, values_mode=True, dry_run=dry_run,
    )

    update_rows = []
    for kind, pairs in (("update", plan.updates), ("photos", plan.photo_repairs)):
        for idx, existing in pairs:
            if idx not in df.index:
                continue
            row = df.loc[idx]
            urls, failed = image_results.get(idx, (None, False))
            values = {}
            if kind == "update":
                # source_id may come from an older system, so it's only set on insert.
                values = {c: to_sql_value(row[c]) for c in columns
                          if c not in ("photos", "image_uploads_captions", "source_id")}
            # Never replace stored photos with a shorter list caused by failed uploads,
            # and keep existing photos when Airtable has no attachments.
            if urls and (not failed or len(urls) > (existing["photo_count"] or 0)):
                values["photos"] = Json(urls)
                values["image_uploads_captions"] = to_sql_value(row["image_uploads_captions"])
            if values:
                update_rows.append((_room_label(row), values, existing["id"]))
    updated, update_failed = _update_rows(conn, cursor, "lexis_rooms", "id", update_rows, dry_run)
    return inserted, updated, insert_failed + update_failed


def write_hotels(conn, cursor, df, plan, rds_table, dry_run):
    df = df.copy()
    for c, max_len in get_column_length_limits(cursor, rds_table).items():
        if c in df.columns and max_len:
            df[c] = df[c].apply(lambda v, m=max_len: truncate(v, m))
    columns = list(df.columns)

    insert_rows = []
    for idx in plan.inserts:
        if idx in df.index:
            row = df.loc[idx]
            insert_rows.append((f"hotel {row['unique_row_id']}", tuple(to_sql_value(row[c]) for c in columns)))
    inserted, insert_failed = write_batches(
        conn, cursor, _insert_query(rds_table, columns, on_conflict="unique_row_id"),
        insert_rows, values_mode=True, dry_run=dry_run,
    )

    update_rows = []
    for idx, existing in plan.updates:
        if idx in df.index:
            row = df.loc[idx]
            values = {c: to_sql_value(row[c]) for c in columns if c != "unique_row_id"}
            update_rows.append((f"hotel {existing['uid']}", values, existing["uid"]))
    updated, update_failed = _update_rows(conn, cursor, rds_table, "unique_row_id", update_rows, dry_run)
    return inserted, updated, insert_failed + update_failed


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def _build_image_jobs(raw_df, plan, cfg, mapping, image_field, existing_photos):
    jobs = []
    if cfg.get("is_lexis_rooms"):
        targets = plan.inserts + [i for i, _ in plan.updates] + [i for i, _ in plan.photo_repairs]
    else:
        targets = plan.inserts + [i for i, _ in plan.updates]
    for idx in targets:
        attachments = _attachments(raw_df, idx, image_field)
        if not attachments:
            continue
        amadeus = normalize_amadeus_code(raw_df.at[idx, mapping["amadeus_code"]])
        if cfg.get("is_lexis_rooms"):
            canonical, folders = room_image_folders(amadeus, raw_df.at[idx, mapping["room_name"]])
            underscore = True
            list_prefixes = [f"uploads/scrapped_rooms/{amadeus}/"]
        else:
            canonical, folders = hotel_image_folders(raw_df.at[idx, "airtable_record_id"], amadeus)
            underscore = False
            list_prefixes = folders
        jobs.append(ImageJob(idx, attachments, canonical, folders, underscore,
                             existing_photos.get(idx, []), raw_df.at[idx, "airtable_record_id"],
                             list_prefixes))
    return jobs


def _load_existing_photos(cursor, plan):
    pairs = plan.updates + plan.photo_repairs
    if not pairs:
        return {}
    idx_by_id = {row["id"]: idx for idx, row in pairs}
    cursor.execute("SELECT id, photos FROM lexis_rooms WHERE id = ANY(%s)", (list(idx_by_id),))
    out = {}
    for rid, photos in cursor.fetchall():
        if isinstance(photos, list):
            out[idx_by_id[rid]] = photos
    return out


def sync_table(table_name, config_dict, uid_lookups, watermarks, options, progress_cb=None):
    cfg = TABLES[table_name]
    api_key = config_dict["airtable_api_key"]
    base_id = config_dict["airtable_base_id"]
    cdn_base = config_dict.get("cdn_base_url") or DEFAULT_CDN_BASE_URL
    overrides = config_dict.get("mapping_overrides", {}).get(cfg["rds_table"], {})
    mapping = get_effective_mapping(cfg["rds_table"], overrides)
    image_field = mapping.get("photos") or cfg["image_field"]
    dry_run = options.dry_run
    prefix = "[DRY RUN] " if dry_run else ""

    def report(**kwargs):
        if progress_cb:
            progress_cb(table_name, **kwargs)

    report(stage="connecting", dry_run=dry_run)
    s3_client = get_s3_client(config_dict)
    check_s3_access(s3_client)
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        since = None if options.full_resync else watermarks.get(table_name)
        fetch_since = since - WATERMARK_OVERLAP if since is not None else None
        if fetch_since is None:
            logger.info("%s%s: no watermark (or full resync) - comparing every Airtable record with the DB.",
                        prefix, table_name)
        report(stage="fetching", since=str(fetch_since) if fetch_since is not None else None)

        # Only verified hotels / rooms of verified hotels are fetched at all.
        records = fetch_airtable_records(
            api_key, base_id, cfg["airtable_table_id"], cfg["last_modified_field"], fetch_since,
            progress_cb=lambda **kw: report(stage="fetching", **kw),
            extra_formula=verified_formula(cfg, mapping),
        )
        if cfg.get("is_lexis_rooms") and fetch_since is not None:
            seen = {r.get("id") for r in records}
            catchup = fetch_rooms_of_newly_verified_hotels(
                config_dict, cfg, mapping, fetch_since,
                progress_cb=lambda **kw: report(stage="fetching", **kw),
            )
            records.extend(r for r in catchup if r.get("id") not in seen)
        if not records:
            report(stage="done", fetched=0, upserted=0)
            return

        raw_df = records_to_dataframe(records)
        raw_df, unverified = drop_unverified(raw_df, cfg, mapping)
        if unverified:
            logger.warning("%s%s: ignored %d record(s) that are not verified.", prefix, table_name, unverified)
        if raw_df.empty:
            report(stage="done", fetched=len(records), upserted=0)
            return
        lm_field = cfg["last_modified_field"]
        raw_df[lm_field] = pd.to_datetime(col(raw_df, lm_field), errors="coerce", utc=True)
        amadeus_field = mapping["amadeus_code"]
        if amadeus_field in raw_df.columns:
            raw_df[amadeus_field] = raw_df[amadeus_field].apply(normalize_amadeus_code)

        report(stage="comparing", fetched=len(records))
        if cfg.get("is_lexis_rooms"):
            plan = plan_rooms(cursor, raw_df, mapping, cfg, image_field)
        else:
            plan = plan_hotels(cursor, raw_df, mapping, cfg)
        logger.info(
            "%s%s: fetched %d | new %d | changed %d | photo repairs %d | unchanged %d | "
            "exists under another source %d | no valid Amadeus code/room name %d | duplicates in Airtable %d",
            prefix, table_name, len(records), len(plan.inserts), len(plan.updates), len(plan.photo_repairs),
            plan.unchanged, plan.skipped_other_source, plan.invalid, plan.duplicates_in_source,
        )
        report(stage="compared", to_insert=len(plan.inserts), to_update=len(plan.updates),
               photo_repairs=len(plan.photo_repairs), unchanged=plan.unchanged,
               skipped_other_source=plan.skipped_other_source, invalid=plan.invalid)

        existing_photos = _load_existing_photos(cursor, plan) if cfg.get("is_lexis_rooms") else {}
        jobs = _build_image_jobs(raw_df, plan, cfg, mapping, image_field, existing_photos)
        report(stage="checking_images")
        links = AttachmentLinks(api_key, base_id, cfg["airtable_table_id"], image_field)
        image_stats, image_results = resolve_images(
            jobs, s3_client, cdn_base, dry_run,
            progress_cb=lambda **kw: report(**kw), links=links,
        )
        logger.info(
            "%s%s images: reused from DB %d | already in S3 %d | uploaded %d | would upload %d | failed %d",
            prefix, table_name, image_stats["reused_db"], image_stats["reused_s3"],
            image_stats["uploaded"], image_stats["would_upload"], image_stats["failed"],
        )

        report(stage="transforming")
        write_idx = plan.inserts + [i for i, _ in plan.updates] + [i for i, _ in plan.photo_repairs]
        subset = raw_df.loc[write_idx]
        if cfg.get("is_lexis_hotels"):
            # Hotels have no photos column, so a hotel whose images failed is held
            # back entirely and retried on the next run (watermark won't advance).
            held_back = {idx for idx, (_, failed) in image_results.items() if failed}
            if held_back:
                logger.warning("%s%s: holding back %d hotel(s) with failed image uploads.",
                               prefix, table_name, len(held_back))
                plan.inserts = [i for i in plan.inserts if i not in held_back]
                plan.updates = [(i, r) for i, r in plan.updates if i not in held_back]
                subset = subset.drop(index=[i for i in held_back if i in subset.index])
            df = transform_scrapped_hotels_to_lexis(subset, uid_lookups, cursor, mapping)
        else:
            df = transform_room_category_to_lexis_rooms(subset, mapping)

        report(stage="writing")
        if df.empty:
            inserted = updated = write_failed = 0
        elif cfg.get("is_lexis_rooms"):
            inserted, updated, write_failed = write_rooms(conn, cursor, df, plan, image_results, dry_run)
        else:
            inserted, updated, write_failed = write_hotels(conn, cursor, df, plan, cfg["rds_table"], dry_run)

        failures = image_stats["failed"] + write_failed
        max_modified = raw_df[lm_field].max()
        if dry_run:
            pass
        elif failures:
            logger.warning(
                "%s: %d failure(s); not advancing the sync watermark so they are retried next run.",
                table_name, failures,
            )
        elif pd.notna(max_modified):
            new_mark = _to_utc(max_modified)
            if since is not None and new_mark < since:
                new_mark = since
            watermarks.set(table_name, new_mark)

        logger.info("%s%s: inserted %d | updated %d | write failures %d",
                    prefix, table_name, inserted, updated, write_failed)
        report(stage="done", fetched=len(records), inserted=inserted, updated=updated,
               upserted=inserted + updated, write_failed=write_failed,
               images_uploaded=image_stats["uploaded"],
               images_reused=image_stats["reused_db"] + image_stats["reused_s3"],
               images_would_upload=image_stats["would_upload"], images_failed=image_stats["failed"])
    finally:
        cursor.close()
        conn.close()


def run_full_sync(progress_cb=None, tables=None, dry_run=False, full_resync=False):
    config_dict = config_store.load_config()
    options = SyncOptions(dry_run=dry_run, full_resync=full_resync)
    uid_lookups = UidLookups()
    table_names = [t for t in (tables or list(TABLES.keys())) if t in TABLES]

    control = get_db_connection()
    try:
        if not dry_run:
            cur = control.cursor()
            cur.execute("SELECT pg_try_advisory_lock(%s)", (SYNC_ADVISORY_LOCK_ID,))
            got_lock = cur.fetchone()[0]
            control.commit()
            cur.close()
            if not got_lock:
                message = "Another sync is already running against this database."
                logger.error(message)
                for table_name in table_names:
                    if progress_cb:
                        progress_cb(table_name, stage="error", error=message)
                return
        watermarks = Watermarks(control, dry_run=dry_run)
        for table_name in table_names:
            try:
                sync_table(table_name, config_dict, uid_lookups, watermarks, options, progress_cb=progress_cb)
            except Exception as e:
                logger.exception("Failed to sync table %s", table_name)
                if progress_cb:
                    progress_cb(table_name, stage="error", error=str(e))
    finally:
        control.close()  # also releases the advisory lock
