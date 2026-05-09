"""SQLSkill — primitive skill for fetching, deconstructing, and annotating DAG SQL.

Called by domain skills (e.g. MappingSkill). Never dispatched directly by the LLM.
Output.annotated_sql has /* SOURCE_FILE: ... */ headers injected so the LLM
can attribute evidence snippets back to their origin file.
"""
from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import Field

from base import BaseInput, BaseOutput, BaseSkill


class SQLFetchInput(BaseInput):
    dag_id: str = Field(..., description="Airflow DAG ID")
    source_mode: Literal["local", "git", "composer"] = Field("git")
    task_id: str | None = Field(None, description="Restrict to one task (all tasks if omitted)")
    composer_env: str | None = Field(None)
    local_dag_path: str | None = Field(None)
    git_repo_path: str | None = Field(None)
    git_ref: str | None = Field(None, description="Branch or commit ref for git mode")


class SQLFetchOutput(BaseOutput):
    annotated_sql: str                   # full SQL with SOURCE_FILE headers
    task_files: dict[str, str]           # task_id → file path string
    tasks_evaluated: list[str]
    structures: dict[str, dict]          # task_id → deconstructed structure
    merged_structure: dict               # union of all task structures
    jinja_note: str                      # prompt note about Jinja resolution
    fetch_error: str | None = None


class SQLSkill(BaseSkill):
    name = "SQLSkill"
    description = "Fetch and parse DAG SQL — primitive, not a dispatch target."
    InputModel = SQLFetchInput
    OutputModel = SQLFetchOutput

    async def execute(self, input: SQLFetchInput) -> SQLFetchOutput:
        return await asyncio.to_thread(self._run, input)

    def _run(self, input: SQLFetchInput) -> SQLFetchOutput:
        from tools.mapping_validation_tools import (
            _fetch_sql_local,
            _fetch_sql_git,
            _fetch_all_task_sqls,
            _load_jinja_vars,
            _load_jinja_vars_for_git,
            _deconstruct_sql,
            _merge_structures,
            _build_annotated_sql,
        )
        from core import config

        task_sqls: dict[str, str] = {}
        task_files: dict[str, str] = {}
        fetch_error: str | None = None
        jinja_note = ""

        if input.source_mode == "local":
            root = input.local_dag_path or config.LOCAL_DAG_ROOT
            if not root:
                fetch_error = "LOCAL_DAG_ROOT not set and local_dag_path not provided."
            else:
                try:
                    jinja_vars = _load_jinja_vars()
                    task_sqls, task_files = _fetch_sql_local(
                        input.dag_id, root, input.task_id, jinja_vars
                    )
                    if jinja_vars:
                        jinja_note = (
                            "Jinja expressions were pre-resolved using configured variable values. "
                            "Unknown expressions are replaced with '__JINJA__' — do not flag these."
                        )
                except Exception as exc:
                    fetch_error = str(exc)

        elif input.source_mode == "git":
            repo = input.git_repo_path or config.LOCAL_GIT_REPO_PATH
            ref = input.git_ref or config.LOCAL_GIT_DEFAULT_BRANCH
            if not repo:
                fetch_error = "LOCAL_GIT_REPO_PATH not set and git_repo_path not provided."
            else:
                try:
                    jinja_vars = _load_jinja_vars_for_git(repo, ref)
                    task_sqls, task_files = _fetch_sql_git(
                        input.dag_id, repo, ref, input.task_id
                    )
                    if jinja_vars:
                        jinja_note = (
                            "Jinja expressions were pre-resolved using configured variable values. "
                            "Unknown expressions are replaced with '__JINJA__' — do not flag these."
                        )
                except Exception as exc:
                    fetch_error = str(exc)

        else:  # composer
            env = input.composer_env
            if not env:
                from core.workspace import get_pinned_workspace
                env = get_pinned_workspace().get("composer_env")
            if not env and config.COMPOSER_ENVS:
                env = next(iter(config.COMPOSER_ENVS))
            if env:
                try:
                    task_sqls = _fetch_all_task_sqls(env, input.dag_id)
                    # composer mode doesn't populate task_files (no local paths)
                except Exception as exc:
                    fetch_error = str(exc)
            else:
                fetch_error = "No Composer environment configured."

        structures: dict[str, dict] = {
            tid: _deconstruct_sql(sql) for tid, sql in task_sqls.items()
        }
        merged = _merge_structures(list(structures.values())) if structures else {
            "ctes": {}, "joins": [], "where_clauses": [], "group_by": [],
            "select_expressions": {}, "aggregations": [], "destination_table": None,
        }

        return SQLFetchOutput(
            annotated_sql=_build_annotated_sql(task_sqls, task_files),
            task_files=task_files,
            tasks_evaluated=list(task_sqls.keys()),
            structures=structures,
            merged_structure=merged,
            jinja_note=jinja_note,
            fetch_error=fetch_error,
        )
