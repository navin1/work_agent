"""Singleton DuckDB connection manager."""
import threading
import duckdb
import pandas as pd
import pyarrow as pa


class _DuckDBManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._conn = duckdb.connect(database=":memory:")
        self._tables: dict[str, dict] = {}

    def register_table(self, name: str, arrow_table: pa.Table) -> None:
        with self._lock:
            self._conn.register(name, arrow_table)
            self._tables[name] = {
                "row_count": len(arrow_table),
                "schema": {
                    field.name: str(field.type) for field in arrow_table.schema
                },
            }

    def execute(self, sql: str) -> pd.DataFrame:
        with self._lock:
            return self._conn.execute(sql).df()

    def list_tables(self) -> list[str]:
        with self._lock:
            return list(self._tables.keys())

    def get_schema(self, table_name: str) -> dict:
        with self._lock:
            return self._tables.get(table_name, {}).get("schema", {})

    def get_table_info(self, table_name: str) -> dict:
        with self._lock:
            return self._tables.get(table_name, {})


_instance: _DuckDBManager | None = None
_init_lock = threading.Lock()


def get_manager() -> _DuckDBManager:
    global _instance
    if _instance is None:
        with _init_lock:
            if _instance is None:
                _instance = _DuckDBManager()
    return _instance
