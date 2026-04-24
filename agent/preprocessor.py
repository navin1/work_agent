"""Glossary expansion and workspace injection before prompts reach the agent."""
import re
from datetime import datetime, timezone

from core import persistence
from core.workspace import get_pinned_workspace


def preprocess_prompt(raw: str) -> str:
    glossary = persistence.get_glossary()
    workspace = get_pinned_workspace()

    prompt = raw
    for term, definition in glossary.items():
        pattern = re.compile(rf"\b{re.escape(term)}\b", re.IGNORECASE)
        prompt = pattern.sub(f"{term} ({definition})", prompt, count=1)

    pins = [f"{k}={v}" for k, v in workspace.items() if v]
    if pins:
        prompt = f"[workspace: {', '.join(pins)}] {prompt}"

    persistence.append_session_history({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "raw_prompt": raw,
        "processed_prompt": prompt,
    })

    return prompt
