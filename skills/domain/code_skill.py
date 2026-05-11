"""CodeSkill — compare Git vs GCS code versions and AI-optimise files/folders."""
from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import Field

from base import BaseSkill, ToolOutput, BaseInput


class CodeInput(BaseInput):
    """Compare code between Git and GCS, or AI-optimise individual SQL/Python files and entire folders."""
    action: Literal["compare_git_gcs", "optimise_file", "optimise_folder"] = Field(
        ...,
        description=(
            "compare_git_gcs: diff files between Git repo and deployed GCS bucket for a path; "
            "optimise_file: fetch one .sql or .py file and generate an AI-optimised version; "
            "optimise_folder: scan a folder and AI-optimise every .sql and .py file in it."
        ),
    )
    file_path: str | None = Field(None, description="File path (for compare_git_gcs or optimise_file).")
    folder_path: str | None = Field(None, description="Folder path (for compare_git_gcs or optimise_folder).")
    composer_env: str | None = Field(None, description="Composer environment alias (used for SDK version context).")


class CodeSkill(BaseSkill):
    name = "CodeSkill"
    description = CodeInput.__doc__.strip()
    InputModel = CodeInput
    OutputModel = ToolOutput

    async def execute(self, input: CodeInput) -> ToolOutput:
        return await asyncio.to_thread(self._run, input)

    def _run(self, input: CodeInput) -> ToolOutput:
        from tools.code_tools import compare_git_gcs, optimise_file, optimise_folder
        compare_git_gcs = compare_git_gcs.func
        optimise_file = optimise_file.func
        optimise_folder = optimise_folder.func
        env = self._resolve_env(input.composer_env)

        if input.action == "compare_git_gcs":
            result = compare_git_gcs(folder_path=input.folder_path, file_path=input.file_path)
        elif input.action == "optimise_file":
            result = optimise_file(file_path=input.file_path or "", composer_env=env)
        else:
            result = optimise_folder(folder_path=input.folder_path or "", composer_env=env)
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
