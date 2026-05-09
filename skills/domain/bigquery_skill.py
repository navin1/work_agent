"""BigQuerySkill — query BigQuery and explore dataset/table metadata."""
from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import Field

from base import BaseSkill, ToolOutput, BaseInput


class BigQueryInput(BaseInput):
    """Query BigQuery tables, list datasets and tables, or check job statistics."""
    action: Literal["query_bigquery", "list_datasets", "list_tables", "get_job_stats"] = Field(
        ...,
        description=(
            "query_bigquery: run a SQL query; "
            "list_datasets: list all datasets in a project; "
            "list_tables: list tables in a dataset; "
            "get_job_stats: fetch stats for a completed BQ job."
        ),
    )
    sql: str | None = Field(None, description="SQL to execute (action=query).")
    project_id: str | None = Field(None, description="GCP project ID.")
    dataset_id: str | None = Field(None, description="Dataset ID (action=list_tables).")
    job_id: str | None = Field(None, description="Job ID (action=get_job_stats).")


class BigQuerySkill(BaseSkill):
    name = "BigQuerySkill"
    description = BigQueryInput.__doc__.strip()
    InputModel = BigQueryInput
    OutputModel = ToolOutput

    async def execute(self, input: BigQueryInput) -> ToolOutput:
        return await asyncio.to_thread(self._run, input)

    def _run(self, input: BigQueryInput) -> ToolOutput:
        from tools.bigquery_tools import (
            query_bigquery, list_bq_datasets, list_bq_tables, get_bq_job_stats,
        )
        if input.action == "query_bigquery":
            result = query_bigquery(sql=input.sql or "", project_id=input.project_id)
        elif input.action == "list_datasets":
            result = list_bq_datasets(project_id=input.project_id or "")
        elif input.action == "list_tables":
            result = list_bq_tables(project_id=input.project_id or "", dataset_id=input.dataset_id or "")
        else:
            result = get_bq_job_stats(job_id=input.job_id or "", project_id=input.project_id)
        return ToolOutput(result=result)
