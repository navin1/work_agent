"""User preference and workspace management tools."""
import json
from core.json_utils import safe_json
import time

from langchain.tools import tool

from core import persistence
from core.audit import log_audit
from core.workspace import get_pinned_workspace, set_pinned_workspace


@tool
def save_query(name: str, sql: str, source: str, description: str = None, tags: str = None) -> str:
    """Save a SQL query. source: 'bigquery'|'mysql'|'duckdb'.
    tags: comma-separated string. Returns confirmation with saved ID."""
    try:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
        obj = {"name": name, "sql": sql, "source": source, "description": description, "tags": tag_list}
        persistence.save_query(obj)
        log_audit("user_tools", source, f"save_query:{name}", user_action="save_query")
        return json.dumps({"status": "saved", "id": obj.get("id"), "name": name})
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def get_saved_queries(search: str = None, source: str = None) -> str:
    """Get saved queries, optionally filtered by search term or source.
    Returns JSON list."""
    try:
        queries = persistence.get_saved_queries()
        if source:
            queries = [q for q in queries if q.get("source") == source]
        if search:
            sl = search.lower()
            queries = [q for q in queries if sl in q.get("name", "").lower()
                       or sl in q.get("sql", "").lower()
                       or sl in q.get("description", "").lower()
                       or any(sl in t for t in q.get("tags", []))]
        log_audit("user_tools", "persistence", "get_saved_queries", row_count=len(queries))
        return safe_json(queries)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def update_glossary(term: str, definition: str) -> str:
    """Add or update a glossary term. Used to expand domain-specific terminology in all future prompts.
    Returns confirmation."""
    try:
        persistence.update_glossary(term, definition)
        log_audit("user_tools", "glossary", f"update_glossary:{term}", user_action="update_glossary")
        return json.dumps({"status": "saved", "term": term, "definition": definition})
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def get_glossary() -> str:
    """Return all current glossary terms and definitions as JSON."""
    try:
        g = persistence.get_glossary()
        log_audit("user_tools", "glossary", "get_glossary", row_count=len(g))
        return json.dumps(g)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def pin_workspace(composer_env: str = None, dag_id: str = None, bq_project: str = None) -> str:
    """Pin workspace context. Pinned values are injected into all subsequent agent prompts as defaults.
    Pass null to clear a specific pin. Returns confirmation."""
    try:
        set_pinned_workspace(composer_env=composer_env, dag_id=dag_id, bq_project=bq_project)
        current = get_pinned_workspace()
        log_audit("user_tools", "workspace", "pin_workspace", user_action=str(current))
        return json.dumps({"status": "pinned", "workspace": current})
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def save_favorite(name: str, sql: str, source: str, tags: str = None) -> str:
    """Save a favorite query to favorites.json. Persists across restarts.
    Returns confirmation with saved ID."""
    try:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
        obj = {"name": name, "sql": sql, "source": source, "tags": tag_list}
        persistence.save_favorite(obj)
        log_audit("user_tools", source, f"save_favorite:{name}", user_action="save_favorite")
        return json.dumps({"status": "saved", "id": obj.get("id"), "name": name})
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def get_favorites(search: str = None) -> str:
    """Get saved favorites, optionally filtered by search. Returns JSON list."""
    try:
        favs = persistence.get_favorites()
        if search:
            sl = search.lower()
            favs = [f for f in favs if sl in f.get("name", "").lower() or sl in f.get("sql", "").lower()]
        log_audit("user_tools", "persistence", "get_favorites", row_count=len(favs))
        return safe_json(favs)
    except Exception as exc:
        return json.dumps({"error": str(exc)})
