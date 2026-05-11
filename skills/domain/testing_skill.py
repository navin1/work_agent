"""TestingSkill — compare query outputs and validate SQL optimisations."""
from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import Field

from base import BaseSkill, ToolOutput, BaseInput


class TestingInput(BaseInput):
    """Compare BigQuery query outputs between original and optimised SQL, or run a full
    safety validation to confirm an optimisation doesn't change results."""
    action: Literal["compare_query_outputs", "validate_optimisation"] = Field(
        ...,
        description=(
            "compare_query_outputs: run both queries against BigQuery and diff row-level results; "
            "validate_optimisation: full validation combining query comparison + AI structural checklist."
        ),
    )
    original_sql: str = Field(..., description="The original SQL query.")
    optimised_sql: str = Field(..., description="The optimised SQL query to test against the original.")


class TestingSkill(BaseSkill):
    name = "TestingSkill"
    description = TestingInput.__doc__.strip()
    InputModel = TestingInput
    OutputModel = ToolOutput

    async def execute(self, input: TestingInput) -> ToolOutput:
        return await asyncio.to_thread(self._run, input)

    def _run(self, input: TestingInput) -> ToolOutput:
        from tools.testing_tools import compare_query_outputs, validate_optimisation
        compare_query_outputs = compare_query_outputs.func
        validate_optimisation = validate_optimisation.func
        if input.action == "compare_query_outputs":
            result = compare_query_outputs(
                original_sql=input.original_sql,
                optimised_sql=input.optimised_sql,
            )
        else:
            result = validate_optimisation(
                original_sql=input.original_sql,
                optimised_sql=input.optimised_sql,
            )
        return ToolOutput(result=result)
