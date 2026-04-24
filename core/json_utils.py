"""JSON serialization utilities."""
import json
import math


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
