"""Kernel bootstrap — create and return a fully wired Kernel instance.

Import create_kernel() wherever you need the skill layer:
    from kernel_bootstrap import create_kernel
    kernel = create_kernel()
    result = await kernel.dispatch(user_message, history)
"""
from __future__ import annotations

from kernel import Kernel, KernelContext

# ── Primitives ────────────────────────────────────────────────────────────────
from skills.primitives.sql_skill import SQLSkill
from skills.primitives.llm_skill import LLMSkill
from skills.primitives.excel_skill import ExcelSkill

# ── Domain skills ─────────────────────────────────────────────────────────────
from skills.domain.mapping_skill import MappingSkill
from skills.domain.bigquery_skill import BigQuerySkill
from skills.domain.browse_skill import BrowseSkill
from skills.domain.composer_skill import ComposerSkill
from skills.domain.optimizer_skill import OptimizerSkill
from skills.domain.reconciliation_skill import ReconciliationSkill
from skills.domain.schema_skill import SchemaSkill
from skills.domain.code_skill import CodeSkill
from skills.domain.testing_skill import TestingSkill
from skills.domain.user_skill import UserSkill
from skills.domain.mapping_management_skill import MappingManagementSkill
from skills.domain.excel_data_skill import ExcelDataSkill


def create_kernel(context: KernelContext | None = None) -> Kernel:
    """Instantiate and register all skills. Returns a ready-to-use Kernel."""
    kernel = Kernel(context)

    # Primitives first — domain skills call these via kernel.invoke()
    kernel.register(SQLSkill(kernel),   domain=False)
    kernel.register(LLMSkill(kernel),   domain=False)
    kernel.register(ExcelSkill(kernel), domain=False)

    # Domain skills — LLM dispatch targets
    kernel.register(MappingSkill(kernel))
    kernel.register(BigQuerySkill(kernel))
    kernel.register(BrowseSkill(kernel))
    kernel.register(ComposerSkill(kernel))
    kernel.register(OptimizerSkill(kernel))
    kernel.register(ReconciliationSkill(kernel))
    kernel.register(SchemaSkill(kernel))
    kernel.register(CodeSkill(kernel))
    kernel.register(TestingSkill(kernel))
    kernel.register(UserSkill(kernel))
    kernel.register(MappingManagementSkill(kernel))
    kernel.register(ExcelDataSkill(kernel))

    return kernel
