"""Persistent JSON user data store — auto-loads all files on import."""
import json
import uuid
from pathlib import Path
from datetime import datetime, timezone
from core import config

_ROOT = Path(config.USER_DATA_ROOT)

_FILES = [
    "saved_queries.json",
    "glossary.json",
    "favorites.json",
    "workspace_pins.json",
    "thresholds.json",
    "session_history.json",
    "settings.json",
    "registry.json",
    "reconciliation_ignores.json",
    "name_mappings.json",
]

_DEFAULTS = {
    "saved_queries.json": [],
    "glossary.json": {},
    "favorites.json": [],
    "workspace_pins.json": {},
    "thresholds.json": {
        "task_warning_seconds": 300,
        "task_critical_seconds": 600,
        "success_rate_warning": 0.9,
        "success_rate_critical": 0.7,
    },
    "session_history.json": [],
    "settings.json": {},
    "registry.json": [],
    "reconciliation_ignores.json": {},
    "name_mappings.json": {},
}

_cache: dict = {}


def _path(filename: str) -> Path:
    return _ROOT / filename


def _default(filename: str):
    return _DEFAULTS.get(filename, {})


def load(filename: str):
    p = _path(filename)
    if not p.exists():
        return _default(filename)
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return _default(filename)


def save(filename: str, data) -> None:
    _path(filename).parent.mkdir(parents=True, exist_ok=True)
    with open(_path(filename), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    _cache[filename] = data


def _cached(filename: str):
    if filename not in _cache:
        _cache[filename] = load(filename)
    return _cache[filename]


def get_saved_queries() -> list:
    return _cached("saved_queries.json")


def save_query(obj: dict) -> None:
    obj.setdefault("id", str(uuid.uuid4()))
    obj.setdefault("created_at", datetime.now(timezone.utc).isoformat())
    queries = list(get_saved_queries())
    queries.append(obj)
    _cache["saved_queries.json"] = queries
    save("saved_queries.json", queries)


def get_glossary() -> dict:
    return _cached("glossary.json")


def update_glossary(term: str, definition: str) -> None:
    g = dict(get_glossary())
    g[term] = definition
    _cache["glossary.json"] = g
    save("glossary.json", g)


def get_favorites() -> list:
    return _cached("favorites.json")


def save_favorite(obj: dict) -> None:
    obj.setdefault("id", str(uuid.uuid4()))
    obj.setdefault("created_at", datetime.now(timezone.utc).isoformat())
    favs = list(get_favorites())
    favs.append(obj)
    _cache["favorites.json"] = favs
    save("favorites.json", favs)


def get_workspace_pins() -> dict:
    return _cached("workspace_pins.json")


def save_workspace_pins(obj: dict) -> None:
    _cache["workspace_pins.json"] = obj
    save("workspace_pins.json", obj)


def get_thresholds() -> dict:
    return _cached("thresholds.json")


def get_session_history() -> list:
    return _cached("session_history.json")


def append_session_history(entry: dict) -> None:
    hist = list(get_session_history())
    hist.append(entry)
    if len(hist) > 500:
        hist = hist[-500:]
    _cache["session_history.json"] = hist
    save("session_history.json", hist)


def get_registry() -> list:
    return _cached("registry.json")


def save_registry(data: list) -> None:
    _cache["registry.json"] = data
    save("registry.json", data)


def get_reconciliation_ignores() -> dict:
    return _cached("reconciliation_ignores.json")


def save_reconciliation_ignores(data: dict) -> None:
    _cache["reconciliation_ignores.json"] = data
    save("reconciliation_ignores.json", data)


def get_name_mappings() -> dict:
    return _cached("name_mappings.json")


# Auto-load all files on import
for _f in _FILES:
    _cached(_f)
