"""Git vs GCS code comparison and file optimisation tools."""
import difflib
import json
from core.json_utils import safe_json, extract_json
import time
import zipfile
from datetime import datetime
from pathlib import Path

import requests
from langchain.tools import tool

from core import config
from core.audit import log_audit
from core.llm import get_llm
from core.sql_formatter import format_sql


# ── Git helpers ───────────────────────────────────────────────────────────────

def _git_session(raw: bool = False) -> requests.Session:
    """Return a requests.Session configured for GitHub API calls."""
    session = requests.Session()
    session.verify = config.HTTP_SSL_VERIFY
    session.headers.update({
        "Authorization": f"token {config.GIT_API_TOKEN}",
        "Accept": "application/vnd.github.v3.raw" if raw else "application/vnd.github.v3+json",
    })
    return session


def _list_git_files(folder_path: str) -> dict[str, dict]:
    """Return {relative_path: {sha, size, full_path}} for all files under folder_path in Git."""
    if not config.GIT_API_TOKEN or not config.GIT_REPO:
        return {}
    url = f"{config.GIT_API_BASE_URL}/repos/{config.GIT_REPO}/git/trees/{config.GIT_BRANCH}"
    resp = _git_session().get(url, params={"recursive": "1"}, timeout=30)
    resp.raise_for_status()
    prefix = folder_path.rstrip("/") + "/" if folder_path else ""
    result = {}
    for item in resp.json().get("tree", []):
        if item.get("type") != "blob":
            continue
        path = item["path"]
        if prefix and not path.startswith(prefix):
            continue
        rel = path[len(prefix):]
        result[rel] = {"sha": item.get("sha"), "size": item.get("size", 0), "full_path": path}
    return result


def _fetch_git_content(full_path: str) -> str | None:
    if not config.GIT_API_TOKEN or not config.GIT_REPO:
        return None
    url = f"{config.GIT_API_BASE_URL}/repos/{config.GIT_REPO}/contents/{full_path}"
    resp = _git_session(raw=True).get(url, params={"ref": config.GIT_BRANCH}, timeout=20)
    return resp.text if resp.status_code == 200 else None


# ── GCS helpers ───────────────────────────────────────────────────────────────

def _list_gcs_files(folder_path: str) -> dict[str, dict]:
    """Return {relative_path: {md5, size, gcs_path, bucket, blob_name}} for all blobs."""
    try:
        from google.cloud import storage
        from core.auth import get_credentials
        creds, _ = get_credentials()
        client = storage.Client(credentials=creds)
        prefix = folder_path.rstrip("/") + "/" if folder_path else ""
        result = {}
        for bucket_name in config.GCS_BUCKETS:
            for blob in client.list_blobs(bucket_name, prefix=prefix):
                rel = blob.name[len(prefix):]
                if not rel:
                    continue
                result[rel] = {
                    "md5": blob.md5_hash,
                    "size": blob.size,
                    "gcs_path": f"gs://{bucket_name}/{blob.name}",
                    "bucket": bucket_name,
                    "blob_name": blob.name,
                }
        return result
    except Exception:
        return {}


def _fetch_gcs_content(bucket_name: str, blob_name: str) -> str | None:
    try:
        from google.cloud import storage
        from core.auth import get_credentials
        creds, _ = get_credentials()
        client = storage.Client(credentials=creds)
        return client.bucket(bucket_name).blob(blob_name).download_as_text()
    except Exception:
        return None


# ── File resolution ───────────────────────────────────────────────────────────

def _is_text_file(path: str) -> bool:
    return Path(path).suffix.lower() in {".sql", ".py", ".yaml", ".yml", ".json", ".txt", ".sh"}


def _export_path(filename: str) -> Path:
    exports = Path(config.EXPORTS_ROOT)
    exports.mkdir(parents=True, exist_ok=True)
    stem = Path(filename).stem
    ext = Path(filename).suffix
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return exports / f"{stem}_optimised_{ts}{ext}"


def _fetch_file(file_path: str) -> tuple[str | None, str]:
    """Fetch file content from local path, GCS (gs://...), or Git path.
    Returns (content, resolved_path)."""
    # GCS path
    if file_path.startswith("gs://"):
        try:
            from google.cloud import storage
            from core.auth import get_credentials
            creds, _ = get_credentials()
            client = storage.Client(credentials=creds)
            parts = file_path[5:].split("/", 1)
            bucket_name, blob_name = parts[0], parts[1] if len(parts) > 1 else ""
            content = client.bucket(bucket_name).blob(blob_name).download_as_text()
            return content, file_path
        except Exception as e:
            return None, f"GCS fetch failed: {e}"

    # Local path: absolute, ./-relative, or existing file
    local = Path(file_path)
    if local.is_absolute() or file_path.startswith("./") or file_path.startswith("../"):
        if local.exists():
            return local.read_text(encoding="utf-8"), str(local.resolve())
        return None, f"Local file not found: {file_path}"
    if local.exists():
        return local.read_text(encoding="utf-8"), str(local.resolve())

    # Git path — try direct lookup then tree search
    content = _fetch_git_content(file_path)
    if content:
        return content, file_path

    filename = Path(file_path).name
    try:
        url = f"{config.GIT_API_BASE_URL}/repos/{config.GIT_REPO}/git/trees/{config.GIT_BRANCH}"
        resp = _git_session().get(url, params={"recursive": "1"}, timeout=30)
        if resp.ok:
            for item in resp.json().get("tree", []):
                if item.get("type") == "blob" and item["path"].endswith(f"/{filename}"):
                    c = _fetch_git_content(item["path"])
                    if c:
                        return c, item["path"]
    except Exception:
        pass

    return None, file_path


def _scan_folder(folder_path: str) -> list[dict]:
    """Scan a folder (local, GCS, or Git) and return [{file_path, file_name, content}]
    for all .sql and .py files."""
    ext_set = {".sql", ".py"}
    results = []

    # Local folder
    local = Path(folder_path)
    if local.is_dir() or (
        (folder_path.startswith("./") or folder_path.startswith("../") or local.is_absolute())
        and local.is_dir()
    ):
        for f in sorted(local.rglob("*")):
            if f.is_file() and f.suffix.lower() in ext_set:
                try:
                    results.append({
                        "file_path": str(f),
                        "file_name": f.name,
                        "content": f.read_text(encoding="utf-8"),
                    })
                except Exception:
                    pass
        return results

    # GCS folder
    if folder_path.startswith("gs://"):
        try:
            from google.cloud import storage
            from core.auth import get_credentials
            creds, _ = get_credentials()
            client = storage.Client(credentials=creds)
            parts = folder_path[5:].split("/", 1)
            bucket_name = parts[0]
            prefix = (parts[1].rstrip("/") + "/") if len(parts) > 1 and parts[1] else ""
            for blob in client.list_blobs(bucket_name, prefix=prefix):
                if Path(blob.name).suffix.lower() in ext_set:
                    try:
                        results.append({
                            "file_path": f"gs://{bucket_name}/{blob.name}",
                            "file_name": Path(blob.name).name,
                            "content": blob.download_as_text(),
                        })
                    except Exception:
                        pass
        except Exception:
            pass
        return results

    # Git folder
    for rel, info in _list_git_files(folder_path).items():
        if Path(rel).suffix.lower() in ext_set:
            content = _fetch_git_content(info["full_path"])
            if content:
                results.append({
                    "file_path": info["full_path"],
                    "file_name": Path(rel).name,
                    "content": content,
                })
    return results


# ── Optimisation prompts ──────────────────────────────────────────────────────

_SQL_OPT_PROMPT = """You are a BigQuery SQL performance expert and technical documentation writer.
ABSOLUTE CONSTRAINTS (never violate):
- Do NOT change functional output, result set, business logic, column names, data types, or row semantics.
- Do NOT add, remove, or rename columns. Do NOT change WHERE, HAVING, or JOIN conditions in a way that alters which rows are returned.
- Do NOT change GROUP BY keys, ORDER BY expressions, or aggregation functions.
- Optimise ONLY for: partition filtering, clustering keys, JOIN order, CTE extraction, subquery elimination, scan reduction, slot efficiency.

The optimised_content field MUST begin with this block comment header — fill in the OVERVIEW
with 3 to 4 plain-English sentences describing what this SQL does, what it filters or transforms,
and what it produces. Be crisp. No bullet points. No sub-sections. Max 4 lines of text.

/*
 * ============================================================================
 * OVERVIEW
 * ============================================================================
 * <sentence 1: what this SQL does overall>
 * <sentence 2: what data it reads / what filters it applies>
 * <sentence 3: what it produces / where the output goes>
 * <sentence 4 (optional): any key business rule or transformation worth noting>
 * ============================================================================
 */

Return JSON only — no markdown, no preamble:
{
  "optimised_content": "<header comment followed by the full optimised SQL>",
  "changes": [{"change_type":"...","original_snippet":"...","optimised_snippet":"...","reason":"...","estimated_impact":"High|Medium|Low","confidence":"High|Medium|Low"}],
  "overall_confidence_score": <0-100>,
  "overall_summary": "..."
}"""

_PY_OPT_PROMPT = """You are a Python and Apache Airflow code optimisation expert.

ABSOLUTE CONSTRAINTS (never violate):
- Do NOT change functional behaviour, data outputs, return values, or business logic.
- Do NOT change function signatures, argument names, or public API surfaces.
- Do NOT alter exception handling in ways that change which errors propagate.
- NEVER change an operator type (e.g. BashOperator must stay BashOperator).
  Replacing a functional operator with DummyOperator/EmptyOperator removes behaviour — forbidden.
- IMPORTS — never rewrite, rename, or reorganise import statements.
  Only REMOVE complete import lines that are genuinely unused after rewriting.
  Keep every import that is used exactly as-is. E.g. `from datetime import timedelta`
  must stay `from datetime import timedelta` — do NOT move timedelta to a different module.
- Optimise ONLY for: memory efficiency, idiomatic Python, import hygiene, redundant variables.

AIRFLOW DAG REWRITE RULES (apply when the file defines an Airflow DAG):

1. UNUSED IMPORTS — remove complete import lines that are not referenced after rewriting.
   Never change the form of an import. Only delete the whole line if nothing in the
   rewritten code references that name.

2. CONTEXT MANAGER — convert the bare assignment pattern to the context-manager pattern
   and remove dag=dag from every operator:
   BEFORE:  dag = DAG('my_dag', ...)
            task = SomeOperator(..., dag=dag)
   AFTER:   with DAG('my_dag', ...) as dag:
                task = SomeOperator(...)   # dag=dag removed — inferred from context manager

3. catchup=False — add to the DAG constructor if not already present.

4. EMPTY queryParameters — remove `"queryParameters": []` from operator configurations.
   It is the default value and adds no information.

5. LOOP COLLAPSE — when multiple operator blocks share the same type and config shape,
   differing only by an entity name, replace them with a for-loop.
   RULES for the loop:
   a. The entities list uses the FULL task name exactly as it appears in task_id
      (e.g. "eda_osr_rps_s_fee_item_snap", not a short suffix like "fee_item_snap").
   b. Use direct local variables (bq_start, bq_main, bq_end) — NOT a dict.
   c. Jinja {% include %} paths MUST use Python f-strings with escaped braces:
        f"{{% include 'bq_sql/{entity}_start.sql' %}}"
      This produces: {% include 'bq_sql/eda_osr_rps_s_fee_item_snap_start.sql' %}
      DO NOT use string concatenation like "{% include 'bq_sql/" + var + "...".
   d. Set the dependency chain INSIDE the loop on a single line:
        start_task >> bq_start >> bq_main >> bq_end >> end_task

6. SECTION DELIMITER COMMENTS — remove comments that only restate the task name
   (e.g. ##-----osr_rps_s_fee_item_snap_dag_start). Keep substantive comments.

7. DAG DOC_MD VARIABLE — wire dag_doc_md into Airflow (server injects the content):
   a. Add the line  `dag_doc_md = ""`  immediately before the DAG constructor.
      Do NOT write any multiline string — the server replaces this with the full Markdown.
   b. Pass it to the DAG constructor: `doc_md=dag_doc_md,`
   c. Set it on the dag object as the FIRST line inside `with DAG(...) as dag:`:
        dag.doc_md = dag_doc_md
   The variable name MUST be exactly `dag_doc_md`.

doc_md field — MANDATORY when the file is an Airflow DAG (contains `from airflow` or `import airflow`).
For non-DAG Python files set doc_md to null.

Return JSON only — no markdown, no preamble:
{
  "optimised_content": "<full optimised Python source>",
  "changes": [{"change_type":"...","original_snippet":"...","optimised_snippet":"...","reason":"...","estimated_impact":"High|Medium|Low","confidence":"High|Medium|Low"}],
  "overall_confidence_score": <0-100>,
  "overall_summary": "...",
  "doc_md": {
    "overview": "<3-4 crisp sentences describing what this pipeline does, what data it processes, what it loads, and its business purpose — written for on-call engineers who need fast context>",
    "control_m_job": "<DAG id converted to UPPER_SNAKE_CASE, e.g. eda_osr_rps_285 → EDA_OSR_RPS_285>",
    "impacted_objects": [
      {"name": "<schema.table exactly as in SQL>", "description": "<one line>", "operation": "<read|write|read/write>", "type": "<table|view>"}
    ]
  }
}"""


# ── Single-file optimisation (shared by both tools) ───────────────────────────

def _optimise_single(
    file_name: str, content: str, ext: str, composer_env: str | None
) -> dict:
    """AI-optimise a single file. Returns result dict or raises on failure."""
    from langchain_core.messages import SystemMessage, HumanMessage

    opt_prompt = _SQL_OPT_PROMPT if ext == ".sql" else _PY_OPT_PROMPT
    sdk_info = config.get_composer_sdk_info(composer_env) if composer_env else {}
    if sdk_info:
        opt_prompt = (
            opt_prompt
            + f"\nAirflow: {sdk_info.get('airflow_version','')}, "
            f"Python: {sdk_info.get('python_version','')}"
        )

    llm = get_llm()
    response = llm.invoke([
        SystemMessage(content=opt_prompt),
        HumanMessage(content=f"Optimise this file ({file_name}):\n\n{content}"),
    ])
    raw = response.content
    parsed = extract_json(raw)

    import re as _re
    optimised = parsed.get("optimised_content", content)
    if ext == ".sql":
        content_display = format_sql(content)
        # sqlglot strips /* */ block comments — preserve the header then reattach
        _hdr_match = _re.match(r"^(\s*/\*.*?\*/\s*)", optimised, _re.DOTALL)
        if _hdr_match:
            _header = _hdr_match.group(1).rstrip()
            _body   = optimised[_hdr_match.end():]
            optimised = _header + "\n\n" + format_sql(_body)
        else:
            optimised = format_sql(optimised)
    else:
        content_display = content
        is_airflow_dag = "from airflow" in content or "import airflow" in content
        doc_md = parsed.get("doc_md") or {}
        if is_airflow_dag:
            # Guarantee minimal doc_md fields
            if not doc_md.get("control_m_job"):
                doc_md["control_m_job"] = Path(file_name).stem.upper().replace("-", "_")
            if not isinstance(doc_md.get("impacted_objects"), list):
                doc_md["impacted_objects"] = []
            from tools.optimizer_tools import _inject_dag_docmd
            optimised = _inject_dag_docmd(optimised, Path(file_name).stem, doc_md)

    return {
        "file_name": file_name,
        "file_type": "sql" if ext == ".sql" else "python",
        "original_content": content_display,
        "optimised_content": optimised,
        "changes": parsed.get("changes", []),
        "overall_confidence_score": parsed.get("overall_confidence_score"),
        "overall_summary": parsed.get("overall_summary", ""),
        "doc_md": doc_md if (ext == ".py") else {},
    }


# ── Tool 1: compare Git vs GCS ────────────────────────────────────────────────

@tool
def read_file(file_path: str) -> str:
    """Read and return the raw content of any file.

    Accepts local paths (absolute or ./relative), GCS paths (gs://bucket/path),
    or Git paths (relative path within the configured repo, e.g. sql/rps800/load.sql).

    Use this when the user wants to VIEW, SHOW, DISPLAY, or READ a file without
    modifying it. Never use optimise_file or optimise_sql_file just to read content.

    Returns JSON with: file_path, content, size_bytes, extension."""
    try:
        content, resolved_path = _fetch_file(file_path)
        if content is None:
            return safe_json({"error": f"File not found: {file_path}"})
        ext = Path(resolved_path).suffix.lstrip(".")
        return safe_json({
            "file_path": resolved_path,
            "content": content,
            "size_bytes": len(content.encode()),
            "extension": ext,
        })
    except Exception as exc:
        return safe_json({"error": str(exc)})


@tool
def compare_git_gcs(folder_path: str = None, file_path: str = None) -> str:
    """Compare code between the Git repository and the deployed GCS bucket.

    Use folder_path to compare an entire folder (e.g. 'dags/', 'sql/rps800/').
    Use file_path to compare one specific file (e.g. 'dags/dag_rps800_load.py').

    Returns JSON with:
      - summary: counts of only_in_git, only_in_gcs, identical, different
      - only_in_git: files present in Git but not deployed to GCS
      - only_in_gcs: files deployed to GCS but not in Git
      - identical: files whose content matches exactly
      - different: files that exist in both but have content drift
      - diffs: {filename: {unified_diff, git_path, gcs_path, git_size, gcs_size}}"""
    start = time.time()
    try:
        if not config.GCS_BUCKETS:
            return json.dumps({"error": "GCS_BUCKETS not configured in .env"})
        if not config.GIT_REPO:
            return json.dumps({"error": "GIT_REPO not configured in .env"})

        if file_path:
            folder = str(Path(file_path).parent)
            target_file = Path(file_path).name
        else:
            folder = (folder_path or "").rstrip("/")
            target_file = None

        git_files = _list_git_files(folder)
        gcs_files = _list_gcs_files(folder)

        if target_file:
            git_files = {k: v for k, v in git_files.items() if k == target_file}
            gcs_files = {k: v for k, v in gcs_files.items() if k == target_file}

        only_in_git = [f for f in git_files if f not in gcs_files]
        only_in_gcs = [f for f in gcs_files if f not in git_files]
        common = [f for f in git_files if f in gcs_files]

        identical = []
        different = []
        diffs = {}

        for rel in common:
            if not _is_text_file(rel):
                identical.append(rel)
                continue
            git_content = _fetch_git_content(git_files[rel]["full_path"])
            gcs_info = gcs_files[rel]
            gcs_content = _fetch_gcs_content(gcs_info["bucket"], gcs_info["blob_name"])

            if git_content is None or gcs_content is None:
                different.append(rel)
                diffs[rel] = {"error": "Could not fetch content for comparison"}
                continue

            if git_content == gcs_content:
                identical.append(rel)
            else:
                different.append(rel)
                diff_lines = list(difflib.unified_diff(
                    git_content.splitlines(keepends=True),
                    gcs_content.splitlines(keepends=True),
                    fromfile=f"git/{rel}",
                    tofile=f"gcs/{rel}",
                ))
                diffs[rel] = {
                    "unified_diff": "".join(diff_lines),
                    "git_path": f"{config.GIT_REPO}/{config.GIT_BRANCH}/{git_files[rel]['full_path']}",
                    "gcs_path": gcs_info["gcs_path"],
                    "git_size_bytes": git_files[rel].get("size", 0),
                    "gcs_size_bytes": gcs_info.get("size", 0),
                }

        log_audit("code_tools", folder or file_path, "compare_git_gcs",
                  row_count=len(different), duration_ms=int((time.time()-start)*1000))
        return json.dumps({
            "folder": folder or file_path,
            "summary": {
                "only_in_git": len(only_in_git),
                "only_in_gcs": len(only_in_gcs),
                "identical": len(identical),
                "different": len(different),
                "total_git": len(git_files),
                "total_gcs": len(gcs_files),
            },
            "only_in_git": only_in_git,
            "only_in_gcs": only_in_gcs,
            "identical": identical,
            "different": different,
            "diffs": diffs,
        }, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


# ── Tool 2: optimise a single file ────────────────────────────────────────────

@tool
def optimise_file(file_path: str, composer_env: str = None) -> str:
    """Fetch a file and generate an AI-optimised version.

    file_path can be:
      - A local path (absolute: /home/user/sql/load.sql  or relative: ./dags/dag.py)
      - A GCS path  (gs://bucket/path/file.sql)
      - A Git path  (dags/dag_rps800_load.py, sql/rps800/load.sql)

    Handles:
      - .sql files → BigQuery SQL performance optimisation
      - .py files  → Python / DAG structural + code quality optimisation

    HARD CONSTRAINT: no functional/output changes — performance and best practices only.

    Returns JSON with: file_name, file_type, original_content, optimised_content,
    changes [{change_type, original_snippet, optimised_snippet, reason, estimated_impact, confidence}],
    overall_confidence_score (0-100), overall_summary, export_path (saved to exports/ for download)."""
    start = time.time()
    try:
        content, resolved_path = _fetch_file(file_path)
        if content is None:
            return json.dumps({"error": f"Could not fetch file: {resolved_path}"})

        file_name = Path(file_path).name
        ext = Path(file_path).suffix.lower()

        if ext not in {".sql", ".py"}:
            return json.dumps({"error": f"Unsupported file type '{ext}'. Supported: .sql, .py"})

        result = _optimise_single(file_name, content, ext, composer_env)

        out_path = _export_path(file_name)
        out_path.write_text(result["optimised_content"], encoding="utf-8")

        log_audit("code_tools", file_path, "optimise_file",
                  duration_ms=int((time.time()-start)*1000))
        return json.dumps({
            "file_path": resolved_path,
            **result,
            "export_path": str(out_path),
        }, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


# ── Tool 3: optimise an entire folder ─────────────────────────────────────────

@tool
def optimise_folder(folder_path: str, composer_env: str = None) -> str:
    """Scan a folder and AI-optimise every .sql and .py file it contains.

    folder_path can be:
      - A local path  (absolute: /path/to/sql/  or relative: ./sql/rps800/)
      - A GCS prefix  (gs://bucket/sql/rps800/)
      - A Git folder  (sql/rps800/, dags/)

    HARD CONSTRAINT: no functional/output changes — performance and best practices only.
    No column names, return values, business logic, or data outputs will be changed.

    Returns JSON with per-file results and a zip archive at export_path for bulk download.
    Each result: file_name, file_type, changes, overall_confidence_score, overall_summary, status."""
    start = time.time()
    try:
        files = _scan_folder(folder_path)
        if not files:
            return json.dumps({"error": f"No .sql or .py files found in: {folder_path}"})

        results = []
        for f in files:
            ext = Path(f["file_name"]).suffix.lower()
            try:
                r = _optimise_single(f["file_name"], f["content"], ext, composer_env)
                r["source_path"] = f["file_path"]
                r["status"] = "ok"
            except Exception as e:
                r = {
                    "file_name": f["file_name"],
                    "source_path": f["file_path"],
                    "status": "error",
                    "error": str(e),
                }
            results.append(r)

        exports = Path(config.EXPORTS_ROOT)
        exports.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        folder_label = Path(folder_path.rstrip("/")).name or "folder"
        zip_path = exports / f"{folder_label}_optimised_{ts}.zip"

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for r in results:
                if r.get("status") == "ok" and r.get("optimised_content"):
                    zf.writestr(r["file_name"], r["optimised_content"])

        ok_count = sum(1 for r in results if r.get("status") == "ok")
        log_audit("code_tools", folder_path, "optimise_folder",
                  row_count=ok_count, duration_ms=int((time.time()-start)*1000))
        return json.dumps({
            "folder_path": folder_path,
            "total_files": len(files),
            "optimised": ok_count,
            "errors": len(results) - ok_count,
            "export_path": str(zip_path),
            "results": results,
        }, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})
