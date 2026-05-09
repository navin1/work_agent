"""UserSkill — manage saved queries, favorites, glossary, and workspace settings."""
from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import Field

from base import BaseSkill, ToolOutput, BaseInput


class UserInput(BaseInput):
    """Save and retrieve SQL queries and favorites, manage the domain glossary,
    and pin workspace defaults (Composer environment, DAG, BQ project)."""
    action: Literal[
        "save_query",
        "get_saved_queries",
        "update_glossary",
        "get_glossary",
        "pin_workspace",
        "save_favorite",
        "get_favorites",
    ] = Field(
        ...,
        description=(
            "save_query: save a SQL query with a name and description; "
            "get_saved_queries: retrieve saved queries (optionally filtered); "
            "update_glossary: add or update a domain glossary term; "
            "get_glossary: get all glossary terms and definitions; "
            "pin_workspace: set default composer_env / dag_id / bq_project for the session; "
            "save_favorite: save a query to the persistent favorites list; "
            "get_favorites: retrieve favorites (optionally filtered)."
        ),
    )
    # save_query / save_favorite
    name: str | None = Field(None, description="Name for the query or favorite.")
    sql: str | None = Field(None, description="SQL string to save.")
    source: str | None = Field(None, description="Query source: bigquery | mysql | duckdb.")
    description: str | None = Field(None, description="Optional description for the query.")
    tags: str | None = Field(None, description="Comma-separated tags.")
    # get_saved_queries / get_favorites
    search: str | None = Field(None, description="Search term to filter saved queries or favorites.")
    # update_glossary
    term: str | None = Field(None, description="Glossary term to add or update.")
    definition: str | None = Field(None, description="Definition of the glossary term.")
    # pin_workspace
    composer_env: str | None = Field(None, description="Composer environment alias to pin.")
    dag_id: str | None = Field(None, description="DAG ID to pin.")
    bq_project: str | None = Field(None, description="BigQuery project to pin.")


class UserSkill(BaseSkill):
    name = "UserSkill"
    description = UserInput.__doc__.strip()
    InputModel = UserInput
    OutputModel = ToolOutput

    async def execute(self, input: UserInput) -> ToolOutput:
        return await asyncio.to_thread(self._run, input)

    def _run(self, input: UserInput) -> ToolOutput:
        from tools.user_tools import (
            save_query, get_saved_queries, update_glossary, get_glossary,
            pin_workspace, save_favorite, get_favorites,
        )
        if input.action == "save_query":
            result = save_query(
                name=input.name or "",
                sql=input.sql or "",
                source=input.source or "bigquery",
                description=input.description,
                tags=input.tags,
            )
        elif input.action == "get_saved_queries":
            result = get_saved_queries(search=input.search, source=input.source)
        elif input.action == "update_glossary":
            result = update_glossary(term=input.term or "", definition=input.definition or "")
        elif input.action == "get_glossary":
            result = get_glossary()
        elif input.action == "pin_workspace":
            result = pin_workspace(
                composer_env=input.composer_env,
                dag_id=input.dag_id,
                bq_project=input.bq_project,
            )
        elif input.action == "save_favorite":
            result = save_favorite(
                name=input.name or "",
                sql=input.sql or "",
                source=input.source or "bigquery",
                tags=input.tags,
            )
        else:
            result = get_favorites(search=input.search)
        return ToolOutput(result=result)
