"""sqlglot-based SQL formatter."""
try:
    import sqlglot
    _HAS_SQLGLOT = True
except ImportError:
    _HAS_SQLGLOT = False


def format_sql(sql: str, dialect: str = "bigquery") -> str:
    if not sql or not sql.strip():
        return sql
    if not _HAS_SQLGLOT:
        return sql
    try:
        return sqlglot.transpile(sql, read=dialect, write=dialect, pretty=True)[0]
    except Exception:
        try:
            return sqlglot.transpile(sql, pretty=True)[0]
        except Exception:
            return sql


def is_ddl_dml(sql: str) -> bool:
    if not sql:
        return False
    upper = sql.strip().upper()
    forbidden = ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "MERGE", "TRUNCATE", "ALTER")
    for kw in forbidden:
        if upper.startswith(kw):
            return True
    return False
