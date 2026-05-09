"""ExcelSkill — primitive skill for Excel file ingestion and column role detection.

Finds a mapping file in the DuckDB registry, ensures it's loaded, resolves which
columns serve as target/source/logic/bq_table, and returns that config for use
by domain skills (e.g. MappingSkill).
"""
from __future__ import annotations

import asyncio
from pathlib import Path

from pydantic import Field

from base import BaseInput, BaseOutput, BaseSkill


class ExcelIngestInput(BaseInput):
    file_name: str = Field(..., description="Excel mapping file name (.xlsx or stem)")
    column_config_override: dict = Field(
        default_factory=dict,
        description="Explicit column role overrides (keys: target, source, logic, bq_table, rule_id).",
    )


class ExcelIngestOutput(BaseOutput):
    table_name: str                    # DuckDB table name
    file_stem: str
    col_config: dict                   # resolved column roles
    bq_tables: list[str]               # configured BQ target tables
    dag_names: list[str]               # configured DAG names from registry/config
    actual_columns: list[str]
    row_count: int
    error: str | None = None


class ExcelSkill(BaseSkill):
    name = "ExcelSkill"
    description = "Excel ingestion and column config — primitive, not a dispatch target."
    InputModel = ExcelIngestInput
    OutputModel = ExcelIngestOutput

    async def execute(self, input: ExcelIngestInput) -> ExcelIngestOutput:
        return await asyncio.to_thread(self._run, input)

    def _run(self, input: ExcelIngestInput) -> ExcelIngestOutput:
        from core import persistence
        from core.duckdb_manager import get_manager
        from tools.mapping_validation_tools import _resolve_column_config

        stem_filter = (
            input.file_name.lower().replace(".xlsx", "").replace(".xls", "")
        )

        # ── Locate file in registry ───────────────────────────────────────────
        registry = persistence.get_registry()
        entry = None
        for e in registry:
            stem = Path(e.get("file_path", "")).stem.lower()
            if stem_filter in stem or stem in stem_filter or stem_filter in e.get("table_name", "").lower():
                entry = e
                break

        if not entry:
            return ExcelIngestOutput(
                table_name="", file_stem="", col_config={}, bq_tables=[],
                dag_names=[], actual_columns=[], row_count=0,
                error=f"No Excel file matching '{input.file_name}' found in registry.",
            )

        table_name = entry["table_name"]
        file_stem = Path(entry["file_path"]).stem

        # ── Ensure table is loaded into DuckDB ────────────────────────────────
        db = get_manager()
        if table_name not in db.list_tables():
            from tools.excel_tools import ingest_excel_files
            ingest_excel_files()

        try:
            df = db.execute(f"SELECT * FROM {table_name}")
        except Exception as exc:
            return ExcelIngestOutput(
                table_name=table_name, file_stem=file_stem, col_config={}, bq_tables=[],
                dag_names=[], actual_columns=[], row_count=0,
                error=f"DuckDB query failed: {exc}",
            )

        if df.empty:
            return ExcelIngestOutput(
                table_name=table_name, file_stem=file_stem, col_config={}, bq_tables=[],
                dag_names=[], actual_columns=[], row_count=0,
                error=f"Table {table_name} is empty.",
            )

        actual_cols = list(df.columns)

        # ── Resolve column roles ──────────────────────────────────────────────
        excel_map = persistence.get_excel_mapping()
        file_config = (
            excel_map.get(file_stem) or excel_map.get(file_stem.lower()) or {}
        )
        configured_cols = {**file_config.get("mapping_columns", {}), **input.column_config_override}
        col_config = _resolve_column_config(actual_cols, configured_cols)

        # ── BQ tables + DAG names from config / registry ──────────────────────
        raw_bq = file_config.get("bq_table") or entry.get("bq_table") or []
        bq_tables = [raw_bq] if isinstance(raw_bq, str) else list(raw_bq)

        raw_dags = file_config.get("dag_names") or entry.get("dag_names") or []
        dag_names = [raw_dags] if isinstance(raw_dags, str) else list(raw_dags)

        return ExcelIngestOutput(
            table_name=table_name,
            file_stem=file_stem,
            col_config=col_config,
            bq_tables=bq_tables,
            dag_names=dag_names,
            actual_columns=actual_cols,
            row_count=len(df),
        )
