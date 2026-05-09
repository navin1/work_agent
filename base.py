"""Base contracts for all skills.

Every skill is a class with:
  - A typed Input (Pydantic BaseModel) — becomes the LLM tool schema for domain skills
  - A typed Output (Pydantic BaseModel) — returned from execute()
  - An async execute(input) method
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar, Type

from pydantic import BaseModel

if TYPE_CHECKING:
    from kernel import Kernel


class BaseInput(BaseModel):
    """All skill inputs inherit from this. Fields + docstring become the LLM tool schema."""


class BaseOutput(BaseModel):
    """All skill outputs inherit from this."""


class TextOutput(BaseOutput):
    """Returned by the kernel when the LLM responds with text instead of a tool call."""
    text: str


class ToolOutput(BaseOutput):
    """Generic output for domain skills that wrap existing @tool functions.
    result is a JSON string — the same format the existing renderers expect.
    """
    result: str


class BaseSkill(ABC):
    name: ClassVar[str]
    description: ClassVar[str]       # used as the LLM tool description for domain skills
    InputModel: ClassVar[Type[BaseInput]]
    OutputModel: ClassVar[Type[BaseOutput]]
    dispatch_key: ClassVar[str | None] = None  # override for skills without an action field

    def __init__(self, kernel: "Kernel") -> None:
        self.kernel = kernel

    @abstractmethod
    async def execute(self, input: BaseInput) -> BaseOutput:
        ...
