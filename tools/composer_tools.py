"""Cloud Composer / Airflow REST API tools."""
import json
from core.json_utils import safe_json
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import re
import requests
import urllib3
from langchain.tools import tool

from core import config, persistence
from core.audit import log_audit
from core.sql_formatter import format_sql, extract_sql

# Suppress InsecureRequestWarning when SSL verification is intentionally disabled.
# This prevents the warning from appearing in tool output and confusing the agent.
if config.HTTP_SSL_VERIFY is False:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# ── Credential cache (avoids re-fetching a token on every HTTP request) ───────

_cred_cache: dict = {}   # keyed by scope string


def _get_token() -> str:
    """Return a valid GCP Bearer token, refreshing only when expired or missing."""
    import google.auth
    import google.auth.transport.requests as google_requests

    scope = "https://www.googleapis.com/auth/cloud-platform"
    creds = _cred_cache.get("creds")
    # Refresh if missing or expired (or within 60 s of expiry)
    if creds is None or not creds.token or (
        creds.expiry and (creds.expiry - datetime.now(timezone.utc).replace(tzinfo=None)).total_seconds() < 60
    ):
        creds, _ = google.auth.default(scopes=[scope])
        creds.refresh(google_requests.Request())
        _cred_cache["creds"] = creds
    return creds.token


# ── Auth / HTTP helpers ───────────────────────────────────────────────────────

def _make_session(_env_name: str = "") -> requests.Session:
    """Return a requests.Session with a cached GCP Bearer token and SSL config."""
    session = requests.Session()
    session.verify = config.HTTP_SSL_VERIFY
    session.headers.update({
        "Authorization": f"Bearer {_get_token()}",
        "Content-Type": "application/json",
    })
    return session


def _base_url(env_name: str) -> str:
    info = config.get_composer_info(env_name)
    url = info.get("airflow_url")
    if not url:
        error = info.get("_error", "unknown error")
        raise ValueError(
            f"Could not resolve Airflow URL for '{env_name}'. "
            f"Cause: {error}. "
            f"Fix: set COMPOSER_ENVS=alias:https://your-airflow-url to use a direct URL, "
            f"or check that the GCP project/location/env-name are correct and ADC is configured."
        )
    return url + "/api/v1"


def _enc(s: str) -> str:
    """URL-encode a single path segment (handles + and : in dag_run_ids)."""
    return quote(str(s), safe="")


def _get(env_name: str, path: str, params: dict = None) -> dict:
    url = _base_url(env_name) + path
    resp = _make_session(env_name).get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ── DAG source fetchers ───────────────────────────────────────────────────────

def _fetch_dag_source_gcs(dag_id: str) -> str | None:
    """Fetch DAG Python source from GCS. Searches subfolders."""
    try:
        from google.cloud import storage
        from core.auth import get_credentials
        creds, _ = get_credentials()
        client = storage.Client(credentials=creds)

        # Determine search prefixes: DAG_FOLDER first, then GCS_PREFIXES
        dag_folder = config.DAG_FOLDER.strip() if config.DAG_FOLDER else ""
        search_prefixes = []
        if dag_folder:
            for folder in dag_folder.split(","):
                folder = folder.strip().rstrip("/")
                if folder:
                    search_prefixes.append(folder + "/")
        if not search_prefixes:
            search_prefixes = config.GCS_PREFIXES or [""]

        for bucket_name in config.GCS_BUCKETS:
            bucket = client.bucket(bucket_name)
            for prefix in search_prefixes:
                # Try exact match first (fast path)
                for ext in [".py"]:
                    blob_path = f"{prefix}{dag_id}{ext}"
                    try:
                        blob = bucket.blob(blob_path)
                        if blob.exists():
                            return blob.download_as_text()
                    except Exception:
                        pass

                # Search subfolders: list all blobs under prefix and match by filename
                try:
                    for blob in client.list_blobs(bucket_name, prefix=prefix):
                        name = blob.name
                        # Match if blob filename (last segment) equals dag_id.py
                        filename = name.rsplit("/", 1)[-1]
                        if filename == f"{dag_id}.py":
                            return blob.download_as_text()
                except Exception:
                    continue
        return None
    except Exception:
        return None


def _make_git_session() -> requests.Session:
    """Return a requests.Session configured for GitHub API calls."""
    session = requests.Session()
    session.verify = config.HTTP_SSL_VERIFY
    session.headers.update({
        "Authorization": f"token {config.GIT_API_TOKEN}",
        "Accept": "application/vnd.github.v3.raw",
    })
    return session


def _fetch_file_from_git(file_path: str) -> str | None:
    """Fetch a file from the configured Git repository by its path."""
    try:
        if not config.GIT_API_TOKEN or not config.GIT_REPO:
            return None
        url = f"{config.GIT_API_BASE_URL}/repos/{config.GIT_REPO}/contents/{file_path}"
        resp = _make_git_session().get(url, params={"ref": config.GIT_BRANCH}, timeout=20)
        if resp.status_code == 200:
            return resp.text
        return None
    except Exception:
        return None


def _fetch_sql_file(file_path: str) -> str | None:
    """Fetch a .sql (or any) file from GCS buckets then Git, given a relative path like 'sql/foo/bar.sql'."""
    # Try GCS buckets
    try:
        from google.cloud import storage
        from core.auth import get_credentials
        creds, _ = get_credentials()
        client = storage.Client(credentials=creds)
        dag_folder = config.DAG_FOLDER.strip() if config.DAG_FOLDER else ""
        search_prefixes = []
        if dag_folder:
            for folder in dag_folder.split(","):
                folder = folder.strip().rstrip("/")
                if folder:
                    # SQL files often live alongside DAGs or one level up
                    search_prefixes.append(folder + "/")
                    parent = folder.rsplit("/", 1)[0] if "/" in folder else ""
                    if parent:
                        search_prefixes.append(parent + "/")
        if not search_prefixes:
            search_prefixes = config.GCS_PREFIXES or [""]
        for bucket_name in config.GCS_BUCKETS:
            bucket = client.bucket(bucket_name)
            # Try the path as-is (relative to each search root)
            for prefix in search_prefixes + [""]:
                candidate = (prefix + file_path).lstrip("/")
                try:
                    blob = bucket.blob(candidate)
                    if blob.exists():
                        return blob.download_as_text()
                except Exception:
                    continue
    except Exception:
        pass
    # Try Git
    for root in config.GIT_ROOT_PATHS:
        content = _fetch_file_from_git(f"{root}{file_path}")
        if content:
            return content
    content = _fetch_file_from_git(file_path)
    return content


def _unwrap_rendered_fields(inst: dict) -> dict:
    """Normalise renderedFields API response across Airflow versions.
    Airflow 2.5+ nests rendered values under a 'rendered_fields' key."""
    if isinstance(inst.get("rendered_fields"), dict):
        return inst["rendered_fields"]
    return inst


def _extract_sql_from_truncated_config(s: str) -> str | None:
    """Extract SQL from Airflow's truncated configuration repr string.

    When [core]max_templated_field_length is exceeded, Airflow returns the
    configuration field as a repr-escaped string starting with 'Truncated.'.
    The SQL lives at the innermost 'query': 'SQL...' level.
    Newlines are encoded as the two-char sequence backslash-n.
    """
    if not isinstance(s, str) or not s.startswith("Truncated"):
        return None
    # Use rfind to get the deepest (innermost) 'query': 'SQL level
    for q in ("'query': '", '"query": "'):
        idx = s.rfind(q)
        if idx == -1:
            continue
        sql_raw = s[idx + len(q):]
        # Unescape repr-encoded newlines and tabs
        sql_raw = sql_raw.replace("\\n", "\n").replace("\\t", "\t").replace("\\'", "'")
        # Strip trailing truncation artifact e.g. "...'" or "... 's'"
        sql_raw = re.sub(r"\s*'?\s*\.\.\.\s*$", "", sql_raw).strip()
        from core.sql_formatter import _SQL_RE  # noqa: PLC0415
        if len(sql_raw) > 20 and _SQL_RE.search(sql_raw):
            return sql_raw
    return None


def _extract_rendered_sql(inst: dict) -> str | None:
    """Extract SQL from a task instance API response.
    Handles nested 'rendered_fields', Airflow truncated config strings,
    and .sql file-path references."""
    fields = _unwrap_rendered_fields(inst)
    sql = extract_sql(fields)
    if sql:
        return sql
    # Airflow truncates large configuration fields into a repr string
    config_val = fields.get("configuration")
    if isinstance(config_val, str):
        sql = _extract_sql_from_truncated_config(config_val)
        if sql:
            return sql
    # If a field value is a .sql file path, fetch the file
    for key in ("sql", "query", "bql"):
        val = fields.get(key)
        if isinstance(val, str) and val.strip().lower().endswith(".sql"):
            content = _fetch_sql_file(val.strip())
            if content:
                return content
    return None


def _fetch_dag_source(dag_id: str) -> str | None:
    """Fetch DAG source from GCS (with subfolder search) then Git fallback."""
    source = _fetch_dag_source_gcs(dag_id)
    if source:
        return source
    # Try git paths
    for root in config.GIT_ROOT_PATHS:
        content = _fetch_file_from_git(f"{root}{dag_id}.py")
        if content:
            return content
        # Search one level of subfolders in git via tree API (best-effort)
        try:
            if config.GIT_API_TOKEN and config.GIT_REPO:
                url = f"{config.GIT_API_BASE_URL}/repos/{config.GIT_REPO}/git/trees/HEAD"
                tree_resp = _make_git_session().get(url, params={"recursive": "1"}, timeout=20)
                if tree_resp.status_code == 200:
                    for item in tree_resp.json().get("tree", []):
                        item_path = item.get("path", "")
                        if item_path.endswith(f"/{dag_id}.py") or item_path.endswith(f"/{dag_id}.py"):
                            content = _fetch_file_from_git(item_path)
                            if content:
                                return content
        except Exception:
            pass
    return None


# ── Composer list tool ────────────────────────────────────────────────────────

@tool
def list_composers() -> str:
    """List all configured Cloud Composer environments with their connection details.
    Returns JSON list with: env_name, url, airflow_version, bq_sdk, python_version."""
    try:
        result = []
        for name in config.COMPOSER_ENVS:
            info = config.get_composer_info(name)
            result.append({
                "env_name": name,
                "url": info["airflow_url"],
                "airflow_version": info["airflow_version"],
                "bq_sdk": info["bq_sdk"],
                "python_version": info["python_version"],
            })
        if not result:
            return safe_json({
                "composers": [],
                "count": 0,
                "note": "No Composer environments configured. Set COMPOSER_ENVS in .env file.",
            })
        log_audit("composer_tools", "config", "list_composers", row_count=len(result))
        return safe_json({"composers": result, "count": len(result)})
    except Exception as exc:
        return safe_json({"error": str(exc)})


# ── DAG listing ───────────────────────────────────────────────────────────────

@tool
def list_dags(composer_env: str, tag_filter: str = None, subfolder_filter: str = None) -> str:
    """List all DAGs in a Composer environment.
    Returns JSON list with: dag_id, schedule, is_paused, last_run_time, last_run_status, tags, file_location, subfolder.
    tag_filter: only include DAGs with this tag.
    subfolder_filter: only include DAGs whose file path contains this string.
    composer_env must match a key in COMPOSER_ENVS."""
    start = time.time()
    try:
        data = _get(composer_env, "/dags", {"limit": 1000})
        dags = []
        for d in data.get("dags", []):
            tags = [t.get("name", "") for t in d.get("tags", [])]
            if tag_filter and tag_filter not in tags:
                continue
            file_loc = d.get("file_loc") or d.get("fileloc", "")
            if subfolder_filter and subfolder_filter.lower() not in file_loc.lower():
                continue
            # Extract subfolder from file_loc path
            subfolder = ""
            if file_loc:
                parts = file_loc.replace("\\", "/").split("/")
                if len(parts) > 1:
                    subfolder = "/".join(parts[:-1])
            dags.append({
                "dag_id": d.get("dag_id"),
                "schedule": d.get("schedule_interval"),
                "is_paused": d.get("is_paused"),
                "last_run_time": d.get("last_run") or d.get("last_parsed_time"),
                "last_run_status": None,
                "tags": tags,
                "file_location": file_loc,
                "subfolder": subfolder,
            })
        airflow_url = config.get_composer_info(composer_env).get("airflow_url", "")
        log_audit("composer_tools", composer_env, "list_dags", row_count=len(dags),
                  duration_ms=int((time.time()-start)*1000))
        return safe_json({
            "composer_env": composer_env,
            "airflow_url": airflow_url,
            "dags": dags,
            "count": len(dags),
        })
    except Exception as exc:
        return safe_json({"error": str(exc)})


# ── DAG details + rendered files ──────────────────────────────────────────────

@tool
def get_dag_details(composer_env: str, dag_id: str) -> str:
    """Get DAG source code (raw Python), and task list with operator types.
    Returns JSON with dag_source, tasks."""
    start = time.time()
    try:
        dag_data = _get(composer_env, f"/dags/{dag_id}")
        tasks_data = _get(composer_env, f"/dags/{dag_id}/tasks")

        tasks = []
        for t in tasks_data.get("tasks", []):
            tasks.append({
                "task_id": t.get("task_id"),
                "operator": t.get("class_ref", {}).get("class_name", ""),
                "depends_on": t.get("downstream_task_ids", []),
            })

        dag_source = _fetch_dag_source(dag_id) or "(source not available)"

        log_audit("composer_tools", composer_env, f"dag_details:{dag_id}",
                  duration_ms=int((time.time()-start)*1000))
        return safe_json({
            "dag_id": dag_id,
            "dag_source": dag_source,
            "tasks": tasks,
            "file_loc": dag_data.get("file_loc", ""),
        })
    except Exception as exc:
        return safe_json({"error": str(exc)})


@tool
def get_dag_rendered_files(composer_env: str, dag_id: str) -> str:
    """Get the DAG Python source and ALL rendered SQL files for every task in the DAG.
    Rendered SQL uses Jinja context from the last successful run.
    Use this to see exactly what SQL ran for any given DAG.
    Returns JSON with: dag_id, dag_source, last_run_id, tasks_sql [{task_id, operator, raw_sql, rendered_sql}]."""
    start = time.time()
    try:
        tasks_data = _get(composer_env, f"/dags/{dag_id}/tasks")
        tasks = tasks_data.get("tasks", [])

        # Fetch recent successful runs — kept as a list so each task can walk them
        dag_runs_cache = []
        try:
            runs_data = _get(composer_env, f"/dags/{dag_id}/dagRuns", {
                "limit": 10, "order_by": "-execution_date", "state": "success"
            })
            dag_runs_cache = runs_data.get("dag_runs", [])
        except Exception:
            pass

        dag_source = _fetch_dag_source(dag_id) or "(source not available)"

        tasks_sql = []
        for t in tasks:
            task_id = t.get("task_id", "")
            operator = t.get("class_ref", {}).get("class_name", "")
            raw_sql = None
            rendered_sql = None

            # Get raw SQL from task definition
            try:
                task_data = _get(composer_env, f"/dags/{dag_id}/tasks/{task_id}")
                raw_sql = extract_sql(task_data)
                if not raw_sql:
                    for field in ("sql", "query", "bql"):
                        val = task_data.get(field)
                        if isinstance(val, str) and val.strip().lower().endswith(".sql"):
                            raw_sql = _fetch_sql_file(val.strip())
                            break
            except Exception:
                pass

            # Get rendered SQL — walk recent runs until this task is found
            for dag_run in (dag_runs_cache if dag_runs_cache else []):
                try:
                    ti_detail = _get(composer_env,
                                     f"/dags/{_enc(dag_id)}/dagRuns/{_enc(dag_run['dag_run_id'])}/taskInstances/{_enc(task_id)}")
                    rendered_sql = _extract_rendered_sql(ti_detail)
                    if rendered_sql:
                        break
                except Exception:
                    continue

            if raw_sql or rendered_sql:
                tasks_sql.append({
                    "task_id": task_id,
                    "operator": operator,
                    "raw_sql": format_sql(raw_sql) if raw_sql else None,
                    "rendered_sql": format_sql(rendered_sql) if rendered_sql else (format_sql(raw_sql) if raw_sql else None),
                })

        log_audit("composer_tools", composer_env, f"dag_rendered_files:{dag_id}",
                  row_count=len(tasks_sql), duration_ms=int((time.time()-start)*1000))
        return safe_json({
            "dag_id": dag_id,
            "dag_source": dag_source,
            "last_run_id": dag_runs_cache[0]["dag_run_id"] if dag_runs_cache else None,
            "tasks_with_sql": len(tasks_sql),
            "tasks_sql": tasks_sql,
        })
    except Exception as exc:
        return safe_json({"error": str(exc)})


# ── Run history ───────────────────────────────────────────────────────────────

@tool
def get_dag_run_history(composer_env: str, dag_id: str, limit: int = 10) -> str:
    """Get last N run records for a DAG.
    Each record: run_id, logical_date, start_time, end_time, duration_seconds, status, triggered_by.
    Returns JSON list."""
    start = time.time()
    try:
        data = _get(composer_env, f"/dags/{dag_id}/dagRuns", {
            "limit": limit, "order_by": "-execution_date"
        })
        runs = []
        for r in data.get("dag_runs", []):
            start_t = r.get("start_date")
            end_t = r.get("end_date")
            duration = None
            if start_t and end_t:
                try:
                    s = datetime.fromisoformat(start_t.replace("Z", "+00:00"))
                    e = datetime.fromisoformat(end_t.replace("Z", "+00:00"))
                    duration = (e - s).total_seconds()
                except Exception:
                    pass
            runs.append({
                "run_id": r.get("dag_run_id"),
                "logical_date": r.get("logical_date") or r.get("execution_date"),
                "start_time": start_t,
                "end_time": end_t,
                "duration_seconds": duration,
                "status": r.get("state"),
                "triggered_by": r.get("run_type"),
            })
        log_audit("composer_tools", composer_env, f"run_history:{dag_id}",
                  row_count=len(runs), duration_ms=int((time.time()-start)*1000))
        return safe_json({"dag_id": dag_id, "runs": runs})
    except Exception as exc:
        return safe_json({"error": str(exc)})


# ── Task SQL ──────────────────────────────────────────────────────────────────

@tool
def get_task_sql(composer_env: str, dag_id: str, task_id: str, rendered: bool = True) -> str:
    """Extract SQL from a task. Targets BigQueryInsertJobOperator, BigQueryOperator, MySqlOperator,
    SQLExecuteQueryOperator, PythonOperator with SQL in kwargs.
    If rendered=True, resolves Jinja using last successful run logical_date.
    Auto-formats with sqlglot. Returns JSON with raw_sql, rendered_sql."""
    start = time.time()
    debug: dict = {}
    try:
        _task_path = f"/dags/{dag_id}/tasks/{task_id}"
        debug["url_task_definition"] = _base_url(composer_env) + _task_path
        task_data = _get(composer_env, _task_path)
        debug["task_keys"] = list(task_data.keys()) if isinstance(task_data, dict) else str(type(task_data))
        # Surface the raw template field values for diagnosis
        for key in ("sql", "query", "bql", "configuration"):
            val = task_data.get(key)
            if val is not None:
                debug[f"task_{key}"] = str(val)[:300]

        raw_sql = extract_sql(task_data)
        debug["raw_sql_found"] = raw_sql is not None

        # If raw value is a .sql file path, fetch the file
        if not raw_sql:
            for key in ("sql", "query", "bql"):
                val = task_data.get(key)
                if isinstance(val, str) and val.strip().lower().endswith(".sql"):
                    raw_sql = _fetch_sql_file(val.strip())
                    debug["sql_file_fetched"] = val.strip()
                    debug["sql_file_found"] = raw_sql is not None
                    break

        rendered_sql = None
        rendered_error = None
        if rendered:
            try:
                # Step 1: get recent dag runs (confirmed working endpoint)
                _runs_path = f"/dags/{dag_id}/dagRuns"
                debug["url_dag_runs"] = _base_url(composer_env) + _runs_path + "?limit=10&order_by=-execution_date&state=success"
                runs_data = _get(composer_env, _runs_path, {
                    "limit": 10, "order_by": "-execution_date", "state": "success"
                })
                dag_runs = runs_data.get("dag_runs", [])
                debug["dag_runs_found"] = len(dag_runs)

                # Step 2: walk runs until we find one where this task ran
                for dag_run in dag_runs:
                    run_id = dag_run["dag_run_id"]
                    _ti_detail_path = f"/dags/{_enc(dag_id)}/dagRuns/{_enc(run_id)}/taskInstances/{_enc(task_id)}"
                    debug["url_task_instance_detail"] = _base_url(composer_env) + _ti_detail_path
                    try:
                        ti_detail = _get(composer_env, _ti_detail_path)
                    except Exception:
                        continue  # task didn't run in this run, try next
                    debug["run_id_used"] = run_id
                    debug["task_instance_state"] = ti_detail.get("state")
                    rf = ti_detail.get("rendered_fields")
                    if isinstance(rf, dict):
                        debug["rendered_fields_inner_keys"] = list(rf.keys())
                        for key in ("sql", "query", "bql", "configuration"):
                            val = rf.get(key)
                            if val is not None:
                                debug[f"rendered_{key}"] = str(val)[:300]
                    rendered_sql = _extract_rendered_sql(ti_detail)
                    debug["rendered_sql_found"] = rendered_sql is not None
                    break  # found a run with this task — stop
            except Exception as e:
                rendered_error = str(e)
                debug["rendered_error"] = rendered_error

        result = {
            "dag_id": dag_id,
            "task_id": task_id,
            "raw_sql": format_sql(raw_sql) if raw_sql else None,
            "rendered_sql": format_sql(rendered_sql) if rendered_sql else (format_sql(raw_sql) if raw_sql else None),
            "_debug": debug,
        }
        if rendered_error and not result["rendered_sql"]:
            result["rendered_warning"] = rendered_error
        log_audit("composer_tools", composer_env, f"task_sql:{dag_id}/{task_id}",
                  duration_ms=int((time.time()-start)*1000))
        return safe_json(result)
    except Exception as exc:
        return safe_json({"error": str(exc), "_debug": debug})


# ── Task performance ──────────────────────────────────────────────────────────

@tool
def get_task_performance(composer_env: str, dag_id: str, task_id: str = None, limit: int = 10) -> str:
    """Performance metrics for one task or all tasks in a DAG.
    Per task: avg_duration_s, max_duration_s, p95_duration_s, success_rate, run_count, health_status.
    Returns JSON performance matrix."""
    start = time.time()
    try:
        thresholds = persistence.get_thresholds()
        warn_s = thresholds.get("task_warning_seconds", 300)
        crit_s = thresholds.get("task_critical_seconds", 600)

        runs_data = _get(composer_env, f"/dags/{dag_id}/dagRuns",
                         {"limit": limit, "order_by": "-execution_date"})
        run_ids = [r["dag_run_id"] for r in runs_data.get("dag_runs", [])]

        task_stats: dict[str, list[float]] = {}
        task_success: dict[str, list[bool]] = {}

        for run_id in run_ids:
            try:
                instances = _get(composer_env, f"/dags/{_enc(dag_id)}/dagRuns/{_enc(run_id)}/taskInstances")
                for inst in instances.get("task_instances", []):
                    tid = inst["task_id"]
                    if task_id and tid != task_id:
                        continue
                    dur = inst.get("duration")
                    state = inst.get("state")
                    if dur is not None:
                        task_stats.setdefault(tid, []).append(float(dur))
                    task_success.setdefault(tid, []).append(state == "success")
            except Exception:
                continue

        matrix = []
        for tid, durations in task_stats.items():
            arr = sorted(durations)
            avg = sum(arr) / len(arr)
            mx = max(arr)
            p95 = arr[int(len(arr) * 0.95)] if len(arr) > 1 else arr[-1]
            successes = task_success.get(tid, [])
            rate = sum(successes) / len(successes) if successes else 1.0
            if avg >= crit_s or rate < thresholds.get("success_rate_critical", 0.7):
                health = "critical"
            elif avg >= warn_s or rate < thresholds.get("success_rate_warning", 0.9):
                health = "warning"
            else:
                health = "healthy"
            matrix.append({
                "task_id": tid,
                "avg_duration_s": round(avg, 2),
                "max_duration_s": round(mx, 2),
                "p95_duration_s": round(p95, 2),
                "success_rate": round(rate, 3),
                "run_count": len(durations),
                "health_status": health,
            })

        log_audit("composer_tools", composer_env, f"task_perf:{dag_id}",
                  duration_ms=int((time.time()-start)*1000))
        return safe_json({"dag_id": dag_id, "performance": matrix})
    except Exception as exc:
        return safe_json({"error": str(exc)})


# ── Execution logs ────────────────────────────────────────────────────────────

@tool
def get_error_logs(composer_env: str, dag_id: str, run_id: str, task_id: str = None) -> str:
    """Error logs for a failed run or specific failed task.
    Returns JSON with log_lines, error_type, stack_trace."""
    start = time.time()
    try:
        instances = _get(composer_env, f"/dags/{_enc(dag_id)}/dagRuns/{_enc(run_id)}/taskInstances")
        failed = [
            i for i in instances.get("task_instances", [])
            if i.get("state") in ("failed", "upstream_failed")
            and (task_id is None or i["task_id"] == task_id)
        ]
        logs_out = []
        for inst in failed[:5]:
            tid = inst["task_id"]
            try_number = inst.get("try_number", 1)
            try:
                log_resp = _make_session(composer_env).get(
                    _base_url(composer_env) + f"/dags/{_enc(dag_id)}/dagRuns/{_enc(run_id)}/taskInstances/{_enc(tid)}/logs/{try_number}",
                    timeout=30
                )
                lines = log_resp.text.splitlines()
                error_lines = [l for l in lines if "ERROR" in l or "Traceback" in l or "Exception" in l]
                logs_out.append({
                    "task_id": tid,
                    "state": inst.get("state"),
                    "log_lines": lines[-50:],
                    "error_lines": error_lines,
                })
            except Exception as le:
                logs_out.append({"task_id": tid, "error": str(le)})

        log_audit("composer_tools", composer_env, f"error_logs:{dag_id}/{run_id}",
                  duration_ms=int((time.time()-start)*1000))
        return safe_json({"dag_id": dag_id, "run_id": run_id, "failed_tasks": logs_out})
    except Exception as exc:
        return safe_json({"error": str(exc)})


@tool
def get_execution_log(composer_env: str, dag_id: str, run_id: str = None, task_id: str = None) -> str:
    """Get execution log at any level — DAG, job (run), or task.
    - dag_id only → lists recent runs with state and duration
    - dag_id + run_id → all task instances with state, duration, start/end time
    - dag_id + run_id + task_id → full log output for that task (last try)
    Returns JSON with appropriate detail level."""
    start = time.time()
    try:
        # Level 1: DAG only — list recent runs
        if not run_id:
            data = _get(composer_env, f"/dags/{dag_id}/dagRuns",
                        {"limit": 20, "order_by": "-execution_date"})
            runs = []
            for r in data.get("dag_runs", []):
                s = r.get("start_date")
                e = r.get("end_date")
                duration = None
                if s and e:
                    try:
                        duration = (
                            datetime.fromisoformat(e.replace("Z", "+00:00")) -
                            datetime.fromisoformat(s.replace("Z", "+00:00"))
                        ).total_seconds()
                    except Exception:
                        pass
                runs.append({
                    "run_id": r.get("dag_run_id"),
                    "logical_date": r.get("logical_date") or r.get("execution_date"),
                    "state": r.get("state"),
                    "start_time": s,
                    "end_time": e,
                    "duration_seconds": duration,
                    "triggered_by": r.get("run_type"),
                })
            log_audit("composer_tools", composer_env, f"exec_log_dag:{dag_id}",
                      duration_ms=int((time.time()-start)*1000))
            return safe_json({"level": "dag", "dag_id": dag_id, "runs": runs})

        # Level 2: run_id provided — list all task instances
        if not task_id:
            instances = _get(composer_env, f"/dags/{_enc(dag_id)}/dagRuns/{_enc(run_id)}/taskInstances")
            tasks_info = []
            for inst in instances.get("task_instances", []):
                s = inst.get("start_date")
                e = inst.get("end_date")
                duration = None
                if s and e:
                    try:
                        duration = (
                            datetime.fromisoformat(e.replace("Z", "+00:00")) -
                            datetime.fromisoformat(s.replace("Z", "+00:00"))
                        ).total_seconds()
                    except Exception:
                        pass
                tasks_info.append({
                    "task_id": inst.get("task_id"),
                    "state": inst.get("state"),
                    "start_time": s,
                    "end_time": e,
                    "duration_seconds": duration,
                    "try_number": inst.get("try_number"),
                    "operator": inst.get("operator", ""),
                })
            log_audit("composer_tools", composer_env, f"exec_log_run:{dag_id}/{run_id}",
                      duration_ms=int((time.time()-start)*1000))
            return safe_json({
                "level": "run",
                "dag_id": dag_id,
                "run_id": run_id,
                "task_instances": tasks_info,
            })

        # Level 3: task_id provided — fetch full log
        inst = _get(composer_env, f"/dags/{_enc(dag_id)}/dagRuns/{_enc(run_id)}/taskInstances/{_enc(task_id)}")
        try_number = inst.get("try_number", 1)
        log_resp = _make_session(composer_env).get(
            _base_url(composer_env) + f"/dags/{_enc(dag_id)}/dagRuns/{_enc(run_id)}/taskInstances/{_enc(task_id)}/logs/{try_number}",
            timeout=30,
        )
        lines = log_resp.text.splitlines()
        error_lines = [l for l in lines if any(k in l for k in ("ERROR", "Traceback", "Exception", "CRITICAL"))]
        log_audit("composer_tools", composer_env, f"exec_log_task:{dag_id}/{run_id}/{task_id}",
                  duration_ms=int((time.time()-start)*1000))
        return safe_json({
            "level": "task",
            "dag_id": dag_id,
            "run_id": run_id,
            "task_id": task_id,
            "state": inst.get("state"),
            "try_number": try_number,
            "start_time": inst.get("start_date"),
            "end_time": inst.get("end_date"),
            "duration_seconds": inst.get("duration"),
            "log_lines": lines,
            "error_lines": error_lines,
            "log_line_count": len(lines),
        })
    except Exception as exc:
        return safe_json({"error": str(exc)})


# ── Airflow jobs (DAG runs) ───────────────────────────────────────────────────

@tool
def list_airflow_jobs(composer_env: str, dag_id: str = None, limit: int = 20) -> str:
    """List recent Airflow job runs (DAG runs) for all DAGs or a specific DAG.
    Each job: dag_id, run_id, state, start_time, end_time, duration_seconds, triggered_by.
    If dag_id omitted, returns jobs across all DAGs (up to limit per DAG).
    Returns JSON list sorted by start_time descending."""
    start = time.time()
    try:
        all_jobs = []

        if dag_id:
            dag_ids = [dag_id]
        else:
            dags_data = _get(composer_env, "/dags", {"limit": 1000})
            dag_ids = [d.get("dag_id") for d in dags_data.get("dags", []) if d.get("dag_id")]

        for did in dag_ids:
            try:
                runs_data = _get(composer_env, f"/dags/{did}/dagRuns",
                                 {"limit": limit if dag_id else 5, "order_by": "-execution_date"})
                for r in runs_data.get("dag_runs", []):
                    s = r.get("start_date")
                    e = r.get("end_date")
                    duration = None
                    if s and e:
                        try:
                            duration = (
                                datetime.fromisoformat(e.replace("Z", "+00:00")) -
                                datetime.fromisoformat(s.replace("Z", "+00:00"))
                            ).total_seconds()
                        except Exception:
                            pass
                    all_jobs.append({
                        "dag_id": did,
                        "run_id": r.get("dag_run_id"),
                        "state": r.get("state"),
                        "start_time": s,
                        "end_time": e,
                        "duration_seconds": duration,
                        "triggered_by": r.get("run_type"),
                        "logical_date": r.get("logical_date") or r.get("execution_date"),
                    })
            except Exception:
                continue

        # Sort by start_time descending
        all_jobs.sort(key=lambda x: x.get("start_time") or "", reverse=True)
        if not dag_id:
            all_jobs = all_jobs[:limit]

        log_audit("composer_tools", composer_env, f"list_airflow_jobs:{dag_id or 'all'}",
                  row_count=len(all_jobs), duration_ms=int((time.time()-start)*1000))
        return safe_json({
            "composer_env": composer_env,
            "dag_id_filter": dag_id,
            "jobs": all_jobs,
            "count": len(all_jobs),
        })
    except Exception as exc:
        return safe_json({"error": str(exc)})


# ── Task graph ────────────────────────────────────────────────────────────────

@tool
def get_dag_task_graph(composer_env: str, dag_id: str, run_id: str = None) -> str:
    """List all tasks in a DAG with their dependencies, execution state, and duration.
    If run_id is omitted, uses the latest run.
    Returns JSON with tasks list and an ASCII dependency diagram.
    Use this to see the full execution picture for a DAG run."""
    start = time.time()
    try:
        # Get task definitions (dependencies)
        tasks_data = _get(composer_env, f"/dags/{dag_id}/tasks")
        task_defs = {t["task_id"]: t for t in tasks_data.get("tasks", [])}

        # Resolve run_id
        if not run_id:
            runs_data = _get(composer_env, f"/dags/{dag_id}/dagRuns",
                             {"limit": 1, "order_by": "-execution_date"})
            runs = runs_data.get("dag_runs", [])
            if runs:
                run_id = runs[0]["dag_run_id"]
                run_state = runs[0].get("state")
                run_start = runs[0].get("start_date")
                run_end = runs[0].get("end_date")
            else:
                run_state = run_start = run_end = None
        else:
            run_info = _get(composer_env, f"/dags/{_enc(dag_id)}/dagRuns/{_enc(run_id)}")
            run_state = run_info.get("state")
            run_start = run_info.get("start_date")
            run_end = run_info.get("end_date")

        # Get task instance states
        task_states = {}
        if run_id:
            instances = _get(composer_env, f"/dags/{_enc(dag_id)}/dagRuns/{_enc(run_id)}/taskInstances")
            for inst in instances.get("task_instances", []):
                tid = inst["task_id"]
                s = inst.get("start_date")
                e = inst.get("end_date")
                dur = None
                if s and e:
                    try:
                        dur = (
                            datetime.fromisoformat(e.replace("Z", "+00:00")) -
                            datetime.fromisoformat(s.replace("Z", "+00:00"))
                        ).total_seconds()
                    except Exception:
                        dur = inst.get("duration")
                task_states[tid] = {
                    "state": inst.get("state"),
                    "start_time": s,
                    "end_time": e,
                    "duration_seconds": dur or inst.get("duration"),
                    "try_number": inst.get("try_number"),
                    "operator": inst.get("operator", ""),
                }

        # Build task list
        tasks = []
        for tid, tdef in task_defs.items():
            state_info = task_states.get(tid, {})
            tasks.append({
                "task_id": tid,
                "operator": tdef.get("class_ref", {}).get("class_name", ""),
                "depends_on": tdef.get("downstream_task_ids", []),
                "state": state_info.get("state"),
                "start_time": state_info.get("start_time"),
                "end_time": state_info.get("end_time"),
                "duration_seconds": state_info.get("duration_seconds"),
                "try_number": state_info.get("try_number"),
            })

        # Build ASCII dependency diagram
        diagram = _build_task_diagram(task_defs)

        log_audit("composer_tools", composer_env, f"task_graph:{dag_id}",
                  row_count=len(tasks), duration_ms=int((time.time()-start)*1000))
        return safe_json({
            "dag_id": dag_id,
            "run_id": run_id,
            "run_state": run_state,
            "run_start": run_start,
            "run_end": run_end,
            "tasks": tasks,
            "diagram": diagram,
        })
    except Exception as exc:
        return safe_json({"error": str(exc)})


def _build_task_diagram(task_defs: dict) -> str:
    """Build a simple ASCII tree of task dependencies."""
    # Find roots (tasks with no upstream dependencies)
    all_downstream = set()
    for td in task_defs.values():
        for d in td.get("downstream_task_ids", []):
            all_downstream.add(d)

    roots = [tid for tid in task_defs if tid not in all_downstream]

    lines = []
    visited = set()

    def render(tid: str, prefix: str, is_last: bool):
        if tid in visited:
            lines.append(f"{prefix}{'└── ' if is_last else '├── '}{tid} (↑ see above)")
            return
        visited.add(tid)
        connector = "└── " if is_last else "├── "
        lines.append(f"{prefix}{connector}{tid}")
        children = task_defs.get(tid, {}).get("downstream_task_ids", [])
        child_prefix = prefix + ("    " if is_last else "│   ")
        for i, child in enumerate(children):
            render(child, child_prefix, i == len(children) - 1)

    for i, root in enumerate(roots):
        lines.append(root)
        children = task_defs.get(root, {}).get("downstream_task_ids", [])
        for j, child in enumerate(children):
            render(child, "", j == len(children) - 1)

    return "\n".join(lines) if lines else "(no tasks)"


# ── Snapshot diff ─────────────────────────────────────────────────────────────

@tool
def get_dag_snapshot_diff(composer_env: str, dag_id: str) -> str:
    """Compare current DAG source against stored weekly snapshot.
    Returns JSON with has_changes bool, unified_diff string, snapshot_date.
    Saves new snapshot if none exists."""
    start = time.time()
    try:
        import difflib
        snapshot_dir = Path(config.USER_DATA_ROOT) / "dag_snapshots"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = snapshot_dir / f"{composer_env}_{dag_id}.py"

        current_source = _fetch_dag_source(dag_id) or ""
        snapshot_date = None
        unified_diff = ""
        has_changes = False

        if snapshot_path.exists():
            snapshot_source = snapshot_path.read_text(encoding="utf-8")
            snapshot_date = str(datetime.fromtimestamp(snapshot_path.stat().st_mtime))
            diff = list(difflib.unified_diff(
                snapshot_source.splitlines(keepends=True),
                current_source.splitlines(keepends=True),
                fromfile=f"snapshot ({snapshot_date})",
                tofile="current",
            ))
            unified_diff = "".join(diff)
            has_changes = bool(unified_diff)
        else:
            has_changes = False
            unified_diff = "(no snapshot — current source saved as baseline)"

        snapshot_path.write_text(current_source, encoding="utf-8")

        log_audit("composer_tools", composer_env, f"snapshot_diff:{dag_id}",
                  duration_ms=int((time.time()-start)*1000))
        return safe_json({
            "dag_id": dag_id,
            "has_changes": has_changes,
            "unified_diff": unified_diff,
            "snapshot_date": snapshot_date,
        })
    except Exception as exc:
        return safe_json({"error": str(exc)})
