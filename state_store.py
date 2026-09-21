import json
import os
import threading

STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sync_state.json")

_lock = threading.Lock()


def _load():
    with _lock:
        if not os.path.exists(STATE_PATH):
            return {}
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)


def _save(data):
    with _lock:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)


def get_last_loaded_at(table_name):
    data = _load()
    value = data.get(table_name)
    return value


def set_last_loaded_at(table_name, iso_timestamp):
    data = _load()
    data[table_name] = iso_timestamp
    _save(data)
