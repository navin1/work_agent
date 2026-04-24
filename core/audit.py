"""Append-only audit log writer."""
import json
import time
from datetime import datetime, timezone
from core import config


def log_audit(
    module: str,
    source: str,
    query: str,
    row_count: int = 0,
    duration_ms: int = 0,
    user_action: str = None,
) -> None:
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "module": module,
        "source": source,
        "query": query,
        "row_count": row_count,
        "duration_ms": duration_ms,
        "user_action": user_action,
    }
    try:
        with open(config.AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass
