"""Kernel: skill registry, typed invoke, and LLM-tool-calling dispatcher.

The dispatch loop works like this:
  1. Every registered domain skill exposes its InputModel as a Pydantic schema.
  2. The kernel converts those schemas into LangChain StructuredTools.
  3. On dispatch(), the LLM receives all tools + the user message and returns a tool_call.
  4. The kernel reads the tool_call, validates args against the skill's InputModel,
     and invokes the chosen skill — no string-matching, no if/elif chains.
  5. A follow-up LLM call generates the natural-language summary shown in the chat bubble.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import StructuredTool
from langchain_google_genai import ChatGoogleGenerativeAI

from base import BaseInput, BaseOutput, BaseSkill, TextOutput
from core import config

logger = logging.getLogger(__name__)


@dataclass
class KernelContext:
    project_id: str = field(default_factory=lambda: config.GOOGLE_CLOUD_PROJECT)
    location: str = field(default_factory=lambda: config.GOOGLE_CLOUD_LOCATION)
    model: str = field(default_factory=lambda: config.AGENT_MODEL)


class DispatchOutput(BaseOutput):
    """Returned by kernel.dispatch() — carries the LLM text response and all tool results."""
    output: str
    tool_calls: list[tuple[str, str]] = []  # [(action_key, result_json), ...]


class Kernel:
    def __init__(self, context: KernelContext | None = None) -> None:
        self._context = context or KernelContext()
        self._skills: dict[str, BaseSkill] = {}
        self._domain_skills: dict[str, BaseSkill] = {}
        self._llm: ChatGoogleGenerativeAI | None = None

    # ── Registration ──────────────────────────────────────────────────────────

    def register(self, skill: BaseSkill, *, domain: bool = True) -> "Kernel":
        """Register a skill. domain=True makes it a dispatch target for the LLM."""
        self._skills[skill.name] = skill
        if domain:
            self._domain_skills[skill.name] = skill
        logger.debug("registered skill=%s domain=%s", skill.name, domain)
        return self

    # ── Invoke ────────────────────────────────────────────────────────────────

    async def invoke(self, skill_name: str, input: BaseInput) -> BaseOutput:
        """Run a named skill with a typed input. Logs name, duration, and errors."""
        skill = self._skills.get(skill_name)
        if not skill:
            raise KeyError(f"Skill '{skill_name}' not registered. Available: {list(self._skills)}")

        t0 = time.monotonic()
        try:
            result = await skill.execute(input)
            logger.info("skill=%s elapsed=%.2fs", skill_name, time.monotonic() - t0)
            return result
        except Exception:
            logger.exception("skill=%s elapsed=%.2fs FAILED", skill_name, time.monotonic() - t0)
            raise

    # ── Dispatch (LLM tool calling) ───────────────────────────────────────────

    def _get_llm(self) -> ChatGoogleGenerativeAI:
        if self._llm is None:
            self._llm = ChatGoogleGenerativeAI(
                model=self._context.model,
                temperature=0,
            )
        return self._llm

    def _build_tools(self) -> list[StructuredTool]:
        """Convert every domain skill's InputModel into a LangChain StructuredTool."""
        tools = []
        for skill in self._domain_skills.values():
            tools.append(
                StructuredTool(
                    name=skill.name,
                    description=skill.description,
                    args_schema=skill.InputModel,
                    func=lambda **kwargs: kwargs,
                    coroutine=None,
                )
            )
        return tools

    async def dispatch(
        self,
        message: str,
        history: list[dict[str, str]] | None = None,
    ) -> DispatchOutput:
        """Route a user message to the right skill via LLM tool calling.

        Preprocessing (glossary expansion, workspace injection) is applied first.
        After the skill runs, a follow-up LLM call generates the natural-language
        summary shown in the chat bubble.
        """
        from core.system_prompt import build_system_prompt
        from core.preprocessor import preprocess_prompt

        processed = preprocess_prompt(message)

        llm = self._get_llm()
        tools = self._build_tools()
        llm_with_tools = llm.bind_tools(tools)

        messages: list[Any] = [SystemMessage(content=build_system_prompt())]
        for turn in (history or []):
            role = turn.get("role", "user")
            content = turn.get("content", "")
            messages.append(
                HumanMessage(content=content) if role == "user" else AIMessage(content=content)
            )
        messages.append(HumanMessage(content=processed))

        response = await asyncio.to_thread(llm_with_tools.invoke, messages)

        if not getattr(response, "tool_calls", None):
            content = response.content
            if isinstance(content, list):
                content = " ".join(b.get("text", "") for b in content if isinstance(b, dict))
            return DispatchOutput(output=content or "", tool_calls=[])

        tool_call = response.tool_calls[0]
        skill_name: str = tool_call["name"]
        skill_args: dict = tool_call["args"]

        skill = self._domain_skills.get(skill_name)
        if not skill:
            raise KeyError(f"LLM selected unregistered skill '{skill_name}'.")

        skill_input = skill.InputModel(**skill_args)
        action_key: str = (
            getattr(skill_input, "action", None)
            or getattr(skill, "dispatch_key", None)
            or skill_name
        )

        skill_output = await self.invoke(skill_name, skill_input)
        result_json: str = getattr(skill_output, "result", None)
        if result_json is None:
            result_json = skill_output.model_dump_json()

        # Follow-up LLM call: generate natural-language summary for the chat bubble
        follow_msgs = messages + [
            response,
            ToolMessage(content=str(result_json), tool_call_id=tool_call["id"]),
        ]
        summary_resp = await asyncio.to_thread(llm.invoke, follow_msgs)
        output_text = summary_resp.content
        if isinstance(output_text, list):
            output_text = " ".join(
                b.get("text", "") for b in output_text if isinstance(b, dict)
            )

        return DispatchOutput(output=output_text or "", tool_calls=[(action_key, str(result_json))])

    @property
    def context(self) -> KernelContext:
        return self._context
