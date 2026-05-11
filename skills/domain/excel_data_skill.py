"""ExcelDataSkill — query and manage Excel/DuckDB data, trace Excel lineage."""
from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import Field

from base import BaseSkill, ToolOutput, BaseInput


class ExcelDataInput(BaseInput):
    """Query Excel data loaded into DuckDB, list or inspect loaded tables, reload Excel files,
    look up which BigQuery table or DAG a mapping file is linked to, or trace data lineage
    from an Excel mapping file through to its Airflow DAG and BigQuery destination."""
    action: Literal[
        "query_excel_data",
        "list_loaded_tables",
        "get_table_schema",
        "reingest_excel_files",
        "get_bq_table_for_mapping_file",
        "get_dags_for_mapping_file",
        "trace_from_excel",
    ] = Field(
        ...,
        description=(
            "query_excel_data: run a SQL query against DuckDB-loaded Excel tables; "
            "list_loaded_tables: list all Excel files currently loaded into DuckDB; "
            "get_table_schema: show column names and types for a loaded DuckDB table; "
            "reingest_excel_files: reload Excel files from disk into DuckDB; "
            "get_bq_table_for_mapping_file: look up the BigQuery table linked to a mapping file; "
            "get_dags_for_mapping_file: look up DAG names linked to a mapping file; "
            "trace_from_excel: trace full lineage from a mapping file to DAG and BigQuery destination."
        ),
    )
    sql: str | None = Field(None, description="SQL to run against DuckDB (action=query_excel_data).")
    table_name: str | None = Field(None, description="DuckDB table name (action=get_table_schema).")
    mapping_file_name: str | None = Field(None, description="Excel mapping file name (.xlsx or stem).")
    folder_filter: str | None = Field(None, description="Folder filter for list_loaded_tables or reingest_excel_files.")
    composer_env: str | None = Field(None, description="Composer environment alias (action=trace_from_excel).")


class ExcelDataSkill(BaseSkill):
    name = "ExcelDataSkill"
    description = ExcelDataInput.__doc__.strip()
    InputModel = ExcelDataInput
    OutputModel = ToolOutput

    async def execute(self, input: ExcelDataInput) -> ToolOutput:
        return await asyncio.to_thread(self._run, input)

    def _run(self, input: ExcelDataInput) -> ToolOutput:
        from tools.excel_tools import (
            query_excel_data, list_loaded_tables, get_table_schema,
            reingest_excel_files, get_bq_table_for_mapping_file,
            get_dags_for_mapping_file, trace_from_excel,
        )
        query_excel_data = query_excel_data.func
        list_loaded_tables = list_loaded_tables.func
        get_table_schema = get_table_schema.func
        reingest_excel_files = reingest_excel_files.func
        get_bq_table_for_mapping_file = get_bq_table_for_mapping_file.func
        get_dags_for_mapping_file = get_dags_for_mapping_file.func
        trace_from_excel = trace_from_excel.func
        if input.action == "query_excel_data":
            result = query_excel_data(sql=input.sql or "")
        elif input.action == "list_loaded_tables":
            result = list_loaded_tables(folder_filter=input.folder_filter)
        elif input.action == "get_table_schema":
            result = get_table_schema(table_name=input.table_name or "")
        elif input.action == "reingest_excel_files":
            result = reingest_excel_files(folder_filter=input.folder_filter)
        elif input.action == "get_bq_table_for_mapping_file":
            result = get_bq_table_for_mapping_file(mapping_file_name=input.mapping_file_name or "")
        elif input.action == "get_dags_for_mapping_file":
            result = get_dags_for_mapping_file(mapping_file_name=input.mapping_file_name or "")
        else:
            result = trace_from_excel(
                mapping_file_name=input.mapping_file_name or "",
                composer_env=input.composer_env,
            )
        return ToolOutput(result=result)
