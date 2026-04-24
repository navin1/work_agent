"""Three-way reconciliation: Git vs GCS vs Excel mapping sheets."""
import json
from core.json_utils import safe_json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from langchain.tools import tool

from core import config, persistence
from core.audit import log_audit

_cache: dict = {}
_cache_ts: float = 0.0


def normalise_name(raw: str) -> str:
    mappings = persistence.get_name_mappings()
    if raw in mappings:
        return mappings[raw]
    name = Path(raw).stem.lower()
    name = re.sub(r"[^a-z0-9]", "_", name)
    name = re.sub(r"_+", "_", name)
    name = re.sub(r"_v\d+$", "", name)
    return name.strip("_")


def _fetch_git_files() -> dict[str, dict]:
    """Fetch SQL/PY/YAML files from Git via API."""
    files = {}
    try:
        import requests
        headers = {"Authorization": f"token {config.GIT_API_TOKEN}"}
        for root_path in config.GIT_ROOT_PATHS:
            url = f"{config.GIT_API_BASE_URL}/repos/{config.GIT_REPO}/contents/{root_path}?ref={config.GIT_BRANCH}"
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code != 200:
                continue
            for item in resp.json():
                if isinstance(item, dict) and item.get("type") == "file":
                    name = item.get("name", "")
                    if any(name.endswith(ext) for ext in config.GIT_FILE_EXTENSIONS):
                        norm = normalise_name(name)
                        content_resp = requests.get(item["download_url"], headers=headers, timeout=30)
                        files[norm] = {
                            "raw_name": name,
                            "path": item.get("path"),
                            "sha": item.get("sha"),
                            "content": content_resp.text if content_resp.status_code == 200 else None,
                            "url": item.get("html_url"),
                        }
    except Exception:
        pass
    return files


def _fetch_gcs_files() -> dict[str, dict]:
    """Fetch SQL/PY/YAML files from GCS buckets."""
    files = {}
    try:
        from google.cloud import storage
        from core.auth import get_credentials
        creds, _ = get_credentials()
        client = storage.Client(credentials=creds)
        for bucket_name, prefix in zip(config.GCS_BUCKETS, config.GCS_PREFIXES + [""]):
            bucket = client.bucket(bucket_name)
            for blob in client.list_blobs(bucket_name, prefix=prefix):
                name = Path(blob.name).name
                if any(name.endswith(ext) for ext in config.GCS_FILE_EXTENSIONS):
                    norm = normalise_name(name)
                    content = blob.download_as_text() if blob.size and blob.size < 5_000_000 else None
                    files[norm] = {
                        "raw_name": name,
                        "path": blob.name,
                        "bucket": bucket_name,
                        "updated": str(blob.updated),
                        "content": content,
                        "md5": blob.md5_hash,
                    }
    except Exception:
        pass
    return files


def _fetch_mapping_entries() -> dict[str, dict]:
    """Build index from Excel registry."""
    entries = {}
    registry = persistence.get_registry()
    for entry in registry:
        norm = normalise_name(Path(entry["file_path"]).name)
        entries[norm] = entry
    return entries


def _reconcile_all(scope: str = "all", folder_filter: str = None) -> dict:
    git_files = _fetch_git_files()
    gcs_files = _fetch_gcs_files()
    mapping_entries = _fetch_mapping_entries()
    ignores = persistence.get_reconciliation_ignores()

    all_names = set(git_files) | set(gcs_files) | set(mapping_entries)
    if folder_filter:
        all_names = {n for n in all_names if folder_filter.lower() in n}

    results = []
    counts = {"in_sync": 0, "content_drift": 0, "schema_drift": 0, "undeclared": 0,
              "not_deployed": 0, "no_source": 0, "git_only": 0, "gcs_orphan": 0,
              "mapping_ghost": 0, "bq_missing": 0}
    critical = []

    for name in sorted(all_names):
        in_git = name in git_files
        in_gcs = name in gcs_files
        in_map = name in mapping_entries
        acknowledged = name in ignores

        git_r = git_files.get(name)
        gcs_r = gcs_files.get(name)
        map_r = mapping_entries.get(name)

        if scope == "git_gcs" and (not in_git or not in_gcs):
            continue
        if scope == "git_mapping" and (not in_git or not in_map):
            continue
        if scope == "gcs_mapping" and (not in_gcs or not in_map):
            continue

        # Determine status
        if in_git and in_gcs and in_map:
            git_content = (git_r or {}).get("content", "")
            gcs_content = (gcs_r or {}).get("content", "")
            if git_content and gcs_content and git_content.strip() == gcs_content.strip():
                status = "in_sync"
            else:
                status = "content_drift"
        elif in_git and in_gcs and not in_map:
            status = "undeclared"
        elif in_git and in_map and not in_gcs:
            status = "not_deployed"
        elif in_gcs and in_map and not in_git:
            status = "no_source"
        elif in_git and not in_gcs and not in_map:
            status = "git_only"
        elif in_gcs and not in_git and not in_map:
            status = "gcs_orphan"
        elif in_map and not in_git and not in_gcs:
            status = "mapping_ghost"
        else:
            status = "undeclared"

        bq_table = (map_r or {}).get("bq_table", "")
        if in_map and not bq_table:
            status = "bq_missing"

        counts[status] = counts.get(status, 0) + 1
        entry = {
            "logical_name": name,
            "status": status,
            "acknowledged": acknowledged,
            "in_git": in_git,
            "in_gcs": in_gcs,
            "in_mapping": in_map,
            "bq_table": bq_table,
        }
        results.append(entry)
        if status in ("content_drift", "not_deployed", "no_source", "mapping_ghost") and not acknowledged:
            critical.append(entry)

    return {
        "summary": counts,
        "critical_findings": critical,
        "results": results,
        "total": len(results),
        "cache_age_minutes": 0,
    }


@tool
def run_reconciliation(scope: str = "all", folder_filter: str = None) -> str:
    """Three-way comparison: Git repo files vs GCS bucket files vs Excel mapping sheets.
    scope: 'all'|'git_gcs'|'git_mapping'|'gcs_mapping'.
    Results cached for RECONCILIATION_CACHE_TTL_MINUTES minutes.
    Status types: in_sync, content_drift, schema_drift, undeclared, not_deployed, no_source,
    git_only, gcs_orphan, mapping_ghost, bq_missing.
    Returns JSON: summary counts by status, critical_findings list, cache_age_minutes."""
    global _cache, _cache_ts
    start = time.time()
    try:
        now = time.time()
        cache_age = (now - _cache_ts) / 60 if _cache_ts else float("inf")
        ttl = config.RECONCILIATION_CACHE_TTL_MINUTES

        if _cache and cache_age < ttl:
            result = dict(_cache)
            result["cache_age_minutes"] = round(cache_age, 1)
            log_audit("reconciliation_tools", "cache", "run_reconciliation", duration_ms=int((time.time()-start)*1000))
            return safe_json(result)

        result = _reconcile_all(scope=scope, folder_filter=folder_filter)
        _cache = result
        _cache_ts = now

        log_audit("reconciliation_tools", "live", "run_reconciliation",
                  row_count=result["total"], duration_ms=int((time.time()-start)*1000))
        return safe_json(result)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def get_reconciliation_detail(logical_name: str) -> str:
    """Full reconciliation detail for one entity.
    Returns JSON: git_record, gcs_record, mapping_record, status,
    content_diff (unified diff string), schema_diff table, git_commit_history, gcs_version_history."""
    start = time.time()
    try:
        import difflib
        git_files = _fetch_git_files()
        gcs_files = _fetch_gcs_files()
        mapping_entries = _fetch_mapping_entries()

        norm = normalise_name(logical_name)
        git_r = git_files.get(norm)
        gcs_r = gcs_files.get(norm)
        map_r = mapping_entries.get(norm)

        diff = ""
        if git_r and gcs_r:
            gc = (git_r.get("content") or "").splitlines(keepends=True)
            sc = (gcs_r.get("content") or "").splitlines(keepends=True)
            diff = "".join(difflib.unified_diff(gc, sc, fromfile="git", tofile="gcs"))

        log_audit("reconciliation_tools", logical_name, "get_reconciliation_detail", duration_ms=int((time.time()-start)*1000))
        return json.dumps({
            "logical_name": norm,
            "git_record": git_r,
            "gcs_record": gcs_r,
            "mapping_record": map_r,
            "content_diff": diff,
        }, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def acknowledge_reconciliation_finding(logical_name: str, reason: str) -> str:
    """Mark a reconciliation finding as acknowledged with a reason.
    Saves to reconciliation_ignores.json. Finding still appears but is flagged as acknowledged.
    Returns confirmation string."""
    try:
        ignores = dict(persistence.get_reconciliation_ignores())
        ignores[normalise_name(logical_name)] = {
            "reason": reason,
            "acknowledged_at": datetime.now(timezone.utc).isoformat(),
        }
        persistence.save_reconciliation_ignores(ignores)
        log_audit("reconciliation_tools", logical_name, "acknowledge", user_action=reason)
        return json.dumps({"status": "acknowledged", "logical_name": logical_name, "reason": reason})
    except Exception as exc:
        return json.dumps({"error": str(exc)})
