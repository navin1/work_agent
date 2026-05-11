"""SchemaSkill — BigQuery schema introspection and MySQL-to-BQ audit."""
from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import Field

from base import BaseSkill, ToolOutput, BaseInput


class SchemaInput(BaseInput):
    """Introspect BigQuery table schemas to any nesting depth, or run a full MySQL-to-BigQuery schema reconciliation audit."""
    action: Literal["introspect_bq_schema", "run_schema_audit"] = Field(
        ...,
        description=(
            "introspect_bq_schema: fetch the full nested schema for a BigQuery table; "
            "run_schema_audit: run the MySQL → BigQuery reconciliation audit and produce an Excel report."
        ),
    )
    project_id: str | None = Field(None, description="GCP project ID (action=introspect_bq_schema).")
    dataset_id: str | None = Field(None, description="BigQuery dataset ID.")
    table_id: str | None = Field(None, description="BigQuery table ID.")


class SchemaSkill(BaseSkill):
    name = "SchemaSkill"
    description = SchemaInput.__doc__.strip()
    InputModel = SchemaInput
    OutputModel = ToolOutput

    async def execute(self, input: SchemaInput) -> ToolOutput:
        return await asyncio.to_thread(self._run, input)

    def _run(self, input: SchemaInput) -> ToolOutput:
        from tools.schema_tools import introspect_bq_schema, run_schema_audit
        introspect_bq_schema = introspect_bq_schema.func
        run_schema_audit = run_schema_audit.func
        if input.action == "introspect_bq_schema":
            result = introspect_bq_schema(
                project_id=input.project_id or "",
                dataset_id=input.dataset_id or "",
                table_id=input.table_id or "",
            )
        else:
            result = run_schema_audit()
        return ToolOutput(result=result)
