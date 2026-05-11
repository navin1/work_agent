"""ComposerSkill — interact with Cloud Composer (Airflow) environments."""
from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import Field

from base import BaseSkill, ToolOutput, BaseInput


class ComposerInput(BaseInput):
    """Interact with Cloud Composer (Airflow) — list DAGs, get source code and rendered SQL,
    check run history, view task performance metrics, read execution logs, and compare DAG snapshots."""
    action: Literal[
        "list_composers",
        "list_dags",
        "get_dag_details",
        "get_dag_rendered_files",
        "get_dag_run_history",
        "get_task_sql",
        "get_task_performance",
        "get_error_logs",
        "get_execution_log",
        "list_airflow_jobs",
        "get_dag_task_graph",
        "get_dag_snapshot_diff",
    ] = Field(
        ...,
        description=(
            "list_composers: show all configured Composer environments; "
            "list_dags: list all DAGs in an environment; "
            "get_dag_details: get DAG Python source + task list; "
            "get_dag_rendered_files: get DAG source + all rendered SQL files per task; "
            "get_dag_run_history: last N DAG run records; "
            "get_task_sql: extract SQL from a specific task; "
            "get_task_performance: duration and success metrics per task; "
            "get_error_logs: error logs for a failed run or task; "
            "get_execution_log: execution log at DAG/run/task level; "
            "list_airflow_jobs: list recent Airflow job runs; "
            "get_dag_task_graph: task dependency graph with execution state; "
            "get_dag_snapshot_diff: compare current DAG source against stored snapshot."
        ),
    )
    composer_env: str | None = Field(None, description="Composer environment alias (e.g. 'prod').")
    dag_id: str | None = Field(None, description="DAG ID.")
    task_id: str | None = Field(None, description="Task ID.")
    run_id: str | None = Field(None, description="DAG run ID.")
    limit: int = Field(10, description="Maximum number of records to return.")
    rendered: bool = Field(True, description="Return rendered SQL (action=get_task_sql).")
    tag_filter: str | None = Field(None, description="Filter DAGs by tag (action=list_dags).")
    subfolder_filter: str | None = Field(None, description="Filter DAGs by subfolder (action=list_dags).")


class ComposerSkill(BaseSkill):
    name = "ComposerSkill"
    description = ComposerInput.__doc__.strip()
    InputModel = ComposerInput
    OutputModel = ToolOutput

    async def execute(self, input: ComposerInput) -> ToolOutput:
        return await asyncio.to_thread(self._run, input)

    def _run(self, input: ComposerInput) -> ToolOutput:
        from tools.composer_tools import (
            list_composers, list_dags, get_dag_details, get_dag_rendered_files,
            get_dag_run_history, get_task_sql, get_task_performance,
            get_error_logs, get_execution_log, list_airflow_jobs,
            get_dag_task_graph, get_dag_snapshot_diff,
        )
        list_composers = list_composers.func
        list_dags = list_dags.func
        get_dag_details = get_dag_details.func
        get_dag_rendered_files = get_dag_rendered_files.func
        get_dag_run_history = get_dag_run_history.func
        get_task_sql = get_task_sql.func
        get_task_performance = get_task_performance.func
        get_error_logs = get_error_logs.func
        get_execution_log = get_execution_log.func
        list_airflow_jobs = list_airflow_jobs.func
        get_dag_task_graph = get_dag_task_graph.func
        get_dag_snapshot_diff = get_dag_snapshot_diff.func
        env = self._resolve_env(input.composer_env)

        if input.action == "list_composers":
            result = list_composers()
        elif input.action == "list_dags":
            result = list_dags(
                composer_env=env or "",
                tag_filter=input.tag_filter,
                subfolder_filter=input.subfolder_filter,
            )
        elif input.action == "get_dag_details":
            result = get_dag_details(composer_env=env or "", dag_id=input.dag_id or "")
        elif input.action == "get_dag_rendered_files":
            result = get_dag_rendered_files(composer_env=env or "", dag_id=input.dag_id or "")
        elif input.action == "get_dag_run_history":
            result = get_dag_run_history(composer_env=env or "", dag_id=input.dag_id or "", limit=input.limit)
        elif input.action == "get_task_sql":
            result = get_task_sql(
                composer_env=env or "",
                dag_id=input.dag_id or "",
                task_id=input.task_id or "",
                rendered=input.rendered,
            )
        elif input.action == "get_task_performance":
            result = get_task_performance(
                composer_env=env or "",
                dag_id=input.dag_id or "",
                task_id=input.task_id,
                limit=input.limit,
            )
        elif input.action == "get_error_logs":
            result = get_error_logs(
                composer_env=env or "",
                dag_id=input.dag_id or "",
                run_id=input.run_id or "",
                task_id=input.task_id,
            )
        elif input.action == "get_execution_log":
            result = get_execution_log(
                composer_env=env or "",
                dag_id=input.dag_id or "",
                run_id=input.run_id,
                task_id=input.task_id,
            )
        elif input.action == "list_airflow_jobs":
            result = list_airflow_jobs(composer_env=env or "", dag_id=input.dag_id, limit=input.limit)
        elif input.action == "get_dag_task_graph":
            result = get_dag_task_graph(composer_env=env or "", dag_id=input.dag_id or "", run_id=input.run_id)
        else:
            result = get_dag_snapshot_diff(composer_env=env or "", dag_id=input.dag_id or "")
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
