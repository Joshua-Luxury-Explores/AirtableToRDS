"""Local fallback for the sync watermark, used only when etl_sync_state can't be
used in the target DB. Watermarks are keyed per database ("host:port/db") so a run
against one DB (e.g. local) never makes a run against another (e.g. prod) skip records."""

import json
import os
import threading

STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sync_state.json")

_lock = threading.Lock()


def _load():
    if not os.path.exists(STATE_PATH):
        return {}
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_last_loaded_at(db_key, table_name):
    with _lock:
        per_db = _load().get(db_key)
    # Entries from the old flat format aren't tied to a DB, so they're ignored.
    return per_db.get(table_name) if isinstance(per_db, dict) else None


def set_last_loaded_at(db_key, table_name, iso_timestamp):
    with _lock:
        data = _load()
        per_db = data.get(db_key)
        if not isinstance(per_db, dict):
            per_db = data[db_key] = {}
        per_db[table_name] = iso_timestamp
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
