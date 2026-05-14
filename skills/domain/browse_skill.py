"""BrowseSkill — browse and read files from GCS, Git, or local paths."""
from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import Field

from base import BaseSkill, ToolOutput, BaseInput


class BrowseInput(BaseInput):
    """Browse folders or read individual files from GCS buckets, Git repositories, or local paths."""
    action: Literal["browse_gcs", "browse_git", "browse_local", "read_file"] = Field(
        ...,
        description=(
            "browse_gcs: list files at a GCS path (gs://bucket/prefix); "
            "browse_git: list files in a Git repo path; "
            "browse_local: list files at a local filesystem path (absolute or relative); "
            "read_file: read the full content of a file (local, gs://, or Git path)."
        ),
    )
    path: str = Field(..., description="File or folder path to browse or read.")


class BrowseSkill(BaseSkill):
    name = "BrowseSkill"
    description = BrowseInput.__doc__.strip()
    InputModel = BrowseInput
    OutputModel = ToolOutput

    async def execute(self, input: BrowseInput) -> ToolOutput:
        return await asyncio.to_thread(self._run, input)

    def _run(self, input: BrowseInput) -> ToolOutput:
        from tools.browse_tools import browse_gcs, browse_git, browse_local
        from tools.code_tools import read_file
        browse_gcs = browse_gcs.func
        browse_git = browse_git.func
        browse_local = browse_local.func
        read_file = read_file.func

        if input.action == "browse_gcs":
            result = browse_gcs(path=input.path)
        elif input.action == "browse_git":
            result = browse_git(path=input.path)
        elif input.action == "browse_local":
            result = browse_local(path=input.path)
        else:
            result = read_file(file_path=input.path)
        return ToolOutput(result=result)
