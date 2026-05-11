"""OptimizerSkill — analyse and AI-optimise SQL queries and Airflow DAG files."""
from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import Field

from base import BaseSkill, ToolOutput, BaseInput


class OptimizerInput(BaseInput):
    """Analyse SQL for performance issues, or generate AI-optimised versions of SQL queries and Airflow DAG files."""
    action: Literal[
        "get_sql_flags",
        "optimise_sql",
        "optimise_dag",
        "optimise_all_dag_sqls",
        "optimise_sql_file",
    ] = Field(
        ...,
        description=(
            "get_sql_flags: static analysis of SQL for performance issues (full scans, missing partition filters, etc.); "
            "optimise_sql: AI-rewrite of an inline SQL string; "
            "optimise_dag: optimisation suggestions for an Airflow DAG Python file; "
            "optimise_all_dag_sqls: optimise every SQL task in a DAG at once; "
            "optimise_sql_file: fetch a .sql file from GCS or Git and optimise it."
        ),
    )
    sql: str | None = Field(None, description="Inline SQL to analyse or optimise.")
    file_path: str | None = Field(None, description="GCS or Git path to a .sql file.")
    composer_env: str | None = Field(None, description="Composer environment alias.")
    dag_id: str | None = Field(None, description="DAG ID (for DAG-level actions).")


class OptimizerSkill(BaseSkill):
    name = "OptimizerSkill"
    description = OptimizerInput.__doc__.strip()
    InputModel = OptimizerInput
    OutputModel = ToolOutput

    async def execute(self, input: OptimizerInput) -> ToolOutput:
        return await asyncio.to_thread(self._run, input)

    def _run(self, input: OptimizerInput) -> ToolOutput:
        from tools.optimizer_tools import (
            get_sql_flags, optimise_sql, optimise_dag,
            optimise_all_dag_sqls, optimise_sql_file,
        )
        get_sql_flags = get_sql_flags.func
        optimise_sql = optimise_sql.func
        optimise_dag = optimise_dag.func
        optimise_all_dag_sqls = optimise_all_dag_sqls.func
        optimise_sql_file = optimise_sql_file.func
        env = self._resolve_env(input.composer_env)

        if input.action == "get_sql_flags":
            result = get_sql_flags(sql=input.sql or "")
        elif input.action == "optimise_sql":
            result = optimise_sql(sql=input.sql or "", composer_env=env)
        elif input.action == "optimise_dag":
            result = optimise_dag(composer_env=env or "", dag_id=input.dag_id or "", file_path=input.file_path)
        elif input.action == "optimise_all_dag_sqls":
            result = optimise_all_dag_sqls(composer_env=env or "", dag_id=input.dag_id or "")
        else:
            result = optimise_sql_file(file_path=input.file_path or "", composer_env=env)
        return ToolOutput(result=result)

    @staticmethod
    def _resolve_env(env: str | None) -> str | None:
        if env:
            return env
        from core.workspace import get_pinned_workspace
        from core import config
        pinned = get_pinned_workspace().get("composer_env")
        if pinned:
            return pinned
        return next(iter(config.COMPOSER_ENVS), None)
