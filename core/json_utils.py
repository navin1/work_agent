"""JSON serialization utilities."""
import json
import math
import re


def safe_json(obj) -> str:
    """Serialize to JSON, converting NaN/Infinity to null and unknown types to strings.

    Replaces json.dumps(obj, default=str) everywhere in tools — prevents the Gemini
    API rejecting payloads that contain literal NaN (invalid JSON per RFC 8259).
    """
    def _fix(o):
        if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
            return None
        if isinstance(o, dict):
            return {k: _fix(v) for k, v in o.items()}
        if isinstance(o, (list, tuple)):
            return [_fix(v) for v in o]
        return o
    return json.dumps(_fix(obj), default=str)


def extract_json(text: str) -> dict | list:
    """Robustly extract and parse the first JSON object or array from LLM output.

    Handles: plain JSON, ```json fences, extra text before/after the JSON block.
    Raises json.JSONDecodeError if no valid JSON is found.
    """
    text = text.strip()

    # Strip ```json ... ``` or ``` ... ``` fences
    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if fenced:
        return json.loads(fenced.group(1).strip())

    # Find the first { or [ and match to its closing bracket
    for start_char, end_char in [('{', '}'), ('[', ']')]:
        start = text.find(start_char)
        if start == -1:
            continue
        depth = 0
        in_string = False
        escape = False
        for i, ch in enumerate(text[start:], start):
            if escape:
                escape = False
                continue
            if ch == '\\' and in_string:
                escape = True
                continue
            if ch == '"':
                in_string = not in_string
                continue
            if not in_string:
                if ch == start_char:
                    depth += 1
                elif ch == end_char:
                    depth -= 1
                    if depth == 0:
                        return json.loads(text[start:i + 1])

    return json.loads(text)
