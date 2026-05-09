"""LLMSkill — primitive skill for raw LLM calls with caching.

Handles the full LLM round-trip: check cache → call model → extract JSON → save cache.
Domain skills build the prompt; this skill executes it and returns the parsed result.
"""
from __future__ import annotations

import asyncio
import hashlib

from pydantic import Field

from base import BaseInput, BaseOutput, BaseSkill


class LLMInput(BaseInput):
    prompt: str
    cache_key: str | None = Field(
        None,
        description="Explicit cache key. Auto-derived from prompt hash if omitted.",
    )
    force_refresh: bool = Field(False, description="Bypass the verdict cache.")
    max_prompt_chars: int = Field(
        15_000,
        description="Hard cap on prompt length sent to the model (truncates SQL block).",
    )


class LLMOutput(BaseOutput):
    text: str
    parsed_json: list | dict | None = None   # best-effort JSON extraction
    cache_hit: bool = False


class LLMSkill(BaseSkill):
    name = "LLMSkill"
    description = "Raw LLM call with JSON extraction and caching — primitive, not a dispatch target."
    InputModel = LLMInput
    OutputModel = LLMOutput

    async def execute(self, input: LLMInput) -> LLMOutput:
        return await asyncio.to_thread(self._run, input)

    def _run(self, input: LLMInput) -> LLMOutput:
        from tools.mapping_validation_tools import (
            _call_llm,
            _extract_json_array,
            _extract_json,
            _get_cached_verdict,
            _save_verdict,
        )

        cache_key = input.cache_key or hashlib.sha256(input.prompt.encode()).hexdigest()

        if not input.force_refresh:
            cached = _get_cached_verdict(cache_key)
            if cached:
                raw = cached.get("text", "")
                return LLMOutput(
                    text=raw,
                    parsed_json=cached.get("parsed_json"),
                    cache_hit=True,
                )

        prompt = input.prompt
        if len(prompt) > input.max_prompt_chars:
            # Truncate but preserve the JSON rules block at the end
            prompt = prompt[: input.max_prompt_chars] + "\n...[SQL TRUNCATED]..."

        try:
            text = _call_llm(prompt)
        except Exception as exc:
            return LLMOutput(text="", parsed_json=None, cache_hit=False)

        parsed: list | dict | None = _extract_json_array(text) or _extract_json(text)
        _save_verdict(cache_key, {"text": text, "parsed_json": parsed})

        return LLMOutput(text=text, parsed_json=parsed, cache_hit=False)
