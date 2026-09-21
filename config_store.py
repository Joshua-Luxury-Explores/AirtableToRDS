import json
import os
import threading

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

DEFAULT_CONFIG = {
    "airtable_api_key": "",
    "airtable_base_id": "appJEYT1T46uTRpls",
    "pg_config": {
        "host": "",
        "database": "",
        "user": "",
        "password": "",
        "port": 5432,
        "connect_timeout": 30,
    },
    "mapping_overrides": {
        "lexis_hotels": {},
        "lexis_rooms": {},
    },
}

_lock = threading.RLock()


def load_config():
    with _lock:
        if not os.path.exists(CONFIG_PATH):
            save_config(DEFAULT_CONFIG)
            return dict(DEFAULT_CONFIG)
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        merged = dict(DEFAULT_CONFIG)
        merged.update(data)
        merged["mapping_overrides"] = {
            **DEFAULT_CONFIG["mapping_overrides"],
            **data.get("mapping_overrides", {}),
        }
        merged["pg_config"] = {
            **DEFAULT_CONFIG["pg_config"],
            **data.get("pg_config", {}),
        }
        return merged


def save_config(config):
    with _lock:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)


def update_config(patch):
    config = load_config()
    config.update(patch)
    save_config(config)
    return config


def set_mapping_override(table_name, overrides):
    config = load_config()
    config.setdefault("mapping_overrides", {})[table_name] = overrides
    save_config(config)
    return config
