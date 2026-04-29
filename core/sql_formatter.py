"""sqlglot-based SQL formatter and Airflow rendered-field SQL extractor."""
import re

try:
    import sqlglot
    _HAS_SQLGLOT = True
except ImportError:
    _HAS_SQLGLOT = False

_SQL_RE = re.compile(r'\b(SELECT|WITH|INSERT|MERGE|UPDATE|DELETE|CREATE)\b', re.IGNORECASE)


def strip_jinja(sql: str) -> str:
    """Replace Jinja2 templates with SQL-safe placeholders for AST parsing.
    Does NOT modify the original string returned to the UI — call this only
    before passing SQL to sqlglot for structural analysis."""
    sql = re.sub(r"\{\{.*?\}\}", "'__JINJA__'", sql, flags=re.DOTALL)
    sql = re.sub(r"\{%-?.*?-?%\}", " ", sql, flags=re.DOTALL)
    return sql


def extract_sql(obj, _depth: int = 0) -> str | None:
    """Recursively extract SQL from an Airflow renderedFields / task-definition dict.

    Priority:
      1. Top-level keys: sql, query, bql
      2. BigQueryInsertJobOperator nesting: configuration.query.query
      3. Any string value containing a SQL keyword (len > 20)
    """
    if _depth > 6:
        return None
    if isinstance(obj, str):
        s = obj.strip()
        return s if len(s) > 20 and _SQL_RE.search(s) else None
    if isinstance(obj, dict):
        for field in ("sql", "query", "bql"):
            result = extract_sql(obj.get(field), _depth + 1)
            if result:
                return result
        cfg = obj.get("configuration")
        if isinstance(cfg, dict):
            q_block = cfg.get("query")
            if isinstance(q_block, dict):
                result = extract_sql(q_block.get("query"), _depth + 1)
                if result:
                    return result
        for key, val in obj.items():
            if key in ("sql", "query", "bql", "configuration"):
                continue
            result = extract_sql(val, _depth + 1)
            if result:
                return result
    if isinstance(obj, list):
        for item in obj:
            result = extract_sql(item, _depth + 1)
            if result:
                return result
    return None


def format_sql(sql: str, dialect: str = "bigquery") -> str:
    if not sql or not sql.strip():
        return sql
        
    # Globally sanitize non-breaking spaces before formatting
    sql = sql.replace("\xa0", " ").replace("\\xa0", " ")
    
    if not _HAS_SQLGLOT:
        return sql
    try:
        formatted = sqlglot.transpile(sql, read=dialect, write=dialect, pretty=True)[0]
    except Exception:
        try:
            formatted = sqlglot.transpile(sql, pretty=True)[0]
        except Exception:
            formatted = sql
            
    # Catch any residual literals that sqlglot might have preserved
    return formatted.replace("\xa0", " ").replace("\\xa0", " ")


def is_ddl_dml(sql: str) -> bool:
    if not sql:
        return False
    upper = sql.strip().upper()
    forbidden = ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "MERGE", "TRUNCATE", "ALTER")
    for kw in forbidden:
        if upper.startswith(kw):
            return True
    return False
