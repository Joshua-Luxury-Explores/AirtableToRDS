import logging
import threading
import time
import webbrowser
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError

from flask import Flask, jsonify, render_template, request

import config_store
import core
from mapping_defaults import DEFAULT_MAPPINGS

app = Flask(__name__)

EXTERNAL_CALL_POOL = ThreadPoolExecutor(max_workers=4)
EXTERNAL_CALL_TIMEOUT = 15


def call_with_timeout(fn, *args, timeout=EXTERNAL_CALL_TIMEOUT, **kwargs):
    future = EXTERNAL_CALL_POOL.submit(fn, *args, **kwargs)
    try:
        return future.result(timeout=timeout)
    except FutureTimeoutError:
        raise TimeoutError(
            f"Timed out after {timeout}s — check your network/VPN access to Airtable or RDS"
        )

RDS_TABLE_TO_AIRTABLE_KEY = {
    cfg["rds_table"]: name for name, cfg in core.TABLES.items()
}

JOB_LOCK = threading.Lock()
JOB_STATE = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "tables": {
        name: {"stage": "idle", "fetched": 0, "upserted": 0,
               "images_uploaded": 0, "images_total": 0, "images_done": 0, "error": None}
        for name in core.TABLES
    },
    "logs": [],
}


class UiLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
        except Exception:
            msg = record.getMessage()
        with JOB_LOCK:
            JOB_STATE["logs"].append(msg)
            if len(JOB_STATE["logs"]) > 400:
                JOB_STATE["logs"] = JOB_STATE["logs"][-400:]


handler = UiLogHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
logging.getLogger("core").addHandler(handler)
logging.getLogger("core").setLevel(logging.INFO)


def progress_callback(table_name, **kwargs):
    with JOB_LOCK:
        JOB_STATE["tables"][table_name].update(kwargs)


def run_migration_job(tables=None):
    table_names = tables if tables else list(core.TABLES.keys())
    with JOB_LOCK:
        JOB_STATE["running"] = True
        JOB_STATE["started_at"] = time.time()
        JOB_STATE["finished_at"] = None
        for name in table_names:
            JOB_STATE["tables"][name] = {
                "stage": "queued", "fetched": 0, "upserted": 0,
                "images_uploaded": 0, "images_total": 0, "images_done": 0, "error": None,
            }
        JOB_STATE["logs"] = []

    try:
        core.run_full_sync(progress_cb=progress_callback, tables=table_names)
    except Exception as e:
        logging.getLogger("core").exception("Migration job crashed: %s", e)
    finally:
        with JOB_LOCK:
            JOB_STATE["running"] = False
            JOB_STATE["finished_at"] = time.time()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/config", methods=["GET"])
def get_config():
    cfg = config_store.load_config()
    return jsonify({
        "airtable_api_key": cfg.get("airtable_api_key", ""),
        "airtable_base_id": cfg.get("airtable_base_id"),
        "mapping_overrides": cfg.get("mapping_overrides", {}),
        "tables": {name: c["label"] for name, c in core.TABLES.items()},
    })


@app.route("/api/config/api-key", methods=["POST"])
def set_api_key():
    data = request.get_json(force=True)
    api_key = (data or {}).get("api_key", "").strip()
    if not api_key:
        return jsonify({"ok": False, "message": "API key cannot be empty"}), 400
    config_store.update_config({"airtable_api_key": api_key})
    return jsonify({"ok": True})


@app.route("/api/test-airtable", methods=["POST"])
def test_airtable():
    data = request.get_json(silent=True) or {}
    cfg = config_store.load_config()
    api_key = data.get("api_key") or cfg.get("airtable_api_key")
    base_id = data.get("base_id") or cfg.get("airtable_base_id")
    if not api_key:
        return jsonify({"ok": False, "message": "No API key set"}), 400
    try:
        result = call_with_timeout(core.test_airtable_connection, api_key, base_id, timeout=20)
    except TimeoutError as e:
        return jsonify({"ok": False, "message": str(e)}), 504
    return jsonify(result)


@app.route("/api/mapping/<rds_table>", methods=["GET"])
def get_mapping(rds_table):
    if rds_table not in DEFAULT_MAPPINGS:
        return jsonify({"ok": False, "message": "Unknown table"}), 404

    cfg = config_store.load_config()
    overrides = cfg.get("mapping_overrides", {}).get(rds_table, {})

    airtable_fields = []
    error = None
    try:
        table_name = RDS_TABLE_TO_AIRTABLE_KEY[rds_table]
        table_cfg = core.TABLES[table_name]
        airtable_fields = call_with_timeout(
            core.get_airtable_fields,
            cfg["airtable_api_key"], cfg["airtable_base_id"], table_cfg["airtable_table_id"],
        )
    except Exception as e:
        error = str(e)

    rds_columns = []
    try:
        rds_columns = call_with_timeout(core.get_rds_columns, rds_table)
    except Exception as e:
        error = error or str(e)

    return jsonify({
        "ok": True,
        "default_mapping": DEFAULT_MAPPINGS[rds_table],
        "overrides": overrides,
        "airtable_fields": airtable_fields,
        "rds_columns": rds_columns,
        "warning": error,
    })


@app.route("/api/mapping/<rds_table>", methods=["POST"])
def save_mapping(rds_table):
    if rds_table not in DEFAULT_MAPPINGS:
        return jsonify({"ok": False, "message": "Unknown table"}), 404
    data = request.get_json(force=True) or {}
    overrides = data.get("overrides", {})
    config_store.set_mapping_override(rds_table, overrides)
    return jsonify({"ok": True})


@app.route("/api/migration/start", methods=["POST"])
def start_migration():
    data = request.get_json(silent=True) or {}
    requested = data.get("tables")

    if requested:
        invalid = [t for t in requested if t not in core.TABLES]
        if invalid:
            return jsonify({"ok": False, "message": f"Unknown table(s): {', '.join(invalid)}"}), 400
        table_names = requested
    else:
        table_names = list(core.TABLES.keys())

    with JOB_LOCK:
        if JOB_STATE["running"]:
            return jsonify({"ok": False, "message": "Migration already running"}), 409
    thread = threading.Thread(target=run_migration_job, args=(table_names,), daemon=True)
    thread.start()
    return jsonify({"ok": True, "tables": table_names})


@app.route("/api/migration/status", methods=["GET"])
def migration_status():
    with JOB_LOCK:
        return jsonify(dict(JOB_STATE))


if __name__ == "__main__":
    threading.Timer(1.2, lambda: webbrowser.open("http://127.0.0.1:5057")).start()
    app.run(host="127.0.0.1", port=5057, debug=False, threaded=True)
