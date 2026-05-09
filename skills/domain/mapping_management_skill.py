"""MappingManagementSkill — discover mapping files, validate folders, export results."""
from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import Field

from base import BaseSkill, ToolOutput, BaseInput


class MappingManagementInput(BaseInput):
    """Discover available Excel mapping files, validate all mappings in a folder at once,
    or export validation results to an Excel report file."""
    action: Literal["discover_mapping_files", "validate_mapping_folder", "export_mapping_results"] = Field(
        ...,
        description=(
            "discover_mapping_files: list all Excel mapping files available for validation; "
            "validate_mapping_folder: bulk-validate every mapping file in a folder; "
            "export_mapping_results: export previously run validation results to an Excel file."
        ),
    )
    # discover_mapping_files
    folder_path: str | None = Field(None, description="Local folder to scan for .xlsx mapping files.")
    gcs_path: str | None = Field(None, description="GCS path to scan (gs://bucket/prefix).")
    git_folder: str | None = Field(None, description="Git repo folder path to scan.")
    git_repo_path: str | None = Field(None, description="Local git repo root path.")
    git_ref: str | None = Field(None, description="Git branch or commit ref.")
    # validate_mapping_folder
    dag_id: str | None = Field(None, description="DAG ID to validate against.")
    source_mode: Literal["local", "git", "composer"] = Field("git")
    composer_env: str | None = Field(None, description="Composer environment alias.")
    force_refresh: bool = Field(False, description="Bypass validation cache.")
    # export_mapping_results
    file_names: list[str] = Field(
        default_factory=list,
        description="List of mapping file names whose results should be exported.",
    )
    env_label: str = Field("git", description="Label used in the exported Excel filename.")


class MappingManagementSkill(BaseSkill):
    name = "MappingManagementSkill"
    description = MappingManagementInput.__doc__.strip()
    InputModel = MappingManagementInput
    OutputModel = ToolOutput

    async def execute(self, input: MappingManagementInput) -> ToolOutput:
        return await asyncio.to_thread(self._run, input)

    def _run(self, input: MappingManagementInput) -> ToolOutput:
        from tools.mapping_validation_tools import (
            discover_mapping_files,
            export_mapping_results,
            validate_mapping_folder,
        )
        if input.action == "discover_mapping_files":
            result = discover_mapping_files(
                folder_path=input.folder_path,
                gcs_path=input.gcs_path,
                git_folder=input.git_folder,
                git_repo_path=input.git_repo_path,
                git_ref=input.git_ref,
            )
        elif input.action == "validate_mapping_folder":
            result = validate_mapping_folder(
                dag_id=input.dag_id or "",
                source_mode=input.source_mode,
                composer_env=input.composer_env,
                force_refresh=input.force_refresh,
            )
        else:
            result = export_mapping_results(
                file_names=input.file_names,
                env_label=input.env_label,
            )
        return ToolOutput(result=result)
