"""ReconciliationSkill — three-way Git/GCS/Excel mapping reconciliation."""
from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import Field

from base import BaseSkill, ToolOutput, BaseInput


class ReconciliationInput(BaseInput):
    """Run three-way reconciliation comparing Git, GCS, and Excel mapping files to find
    discrepancies, get detail on a specific entity, or acknowledge a finding."""
    action: Literal["run_reconciliation", "get_detail", "acknowledge"] = Field(
        ...,
        description=(
            "run_reconciliation: compare Git vs GCS vs Excel mappings for all or filtered entities; "
            "get_detail: full reconciliation detail for one logical entity (file/DAG); "
            "acknowledge: mark a reconciliation finding as acknowledged with a reason."
        ),
    )
    scope: str = Field("all", description="Scope filter for run_reconciliation (e.g. 'all', 'sql', 'dag').")
    folder_filter: str | None = Field(None, description="Limit reconciliation to a specific folder.")
    logical_name: str | None = Field(None, description="Entity name for get_detail or acknowledge.")
    reason: str | None = Field(None, description="Reason for acknowledging a finding.")


class ReconciliationSkill(BaseSkill):
    name = "ReconciliationSkill"
    description = ReconciliationInput.__doc__.strip()
    InputModel = ReconciliationInput
    OutputModel = ToolOutput

    async def execute(self, input: ReconciliationInput) -> ToolOutput:
        return await asyncio.to_thread(self._run, input)

    def _run(self, input: ReconciliationInput) -> ToolOutput:
        from tools.reconciliation_tools import (
            run_reconciliation, get_reconciliation_detail, acknowledge_reconciliation_finding,
        )
        if input.action == "run_reconciliation":
            result = run_reconciliation(scope=input.scope, folder_filter=input.folder_filter)
        elif input.action == "get_detail":
            result = get_reconciliation_detail(logical_name=input.logical_name or "")
        else:
            result = acknowledge_reconciliation_finding(
                logical_name=input.logical_name or "",
                reason=input.reason or "",
            )
        return ToolOutput(result=result)
