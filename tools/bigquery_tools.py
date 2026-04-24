"""BigQuery read-only tools."""
import json
import time

from langchain.tools import tool

from core import config
from core.audit import log_audit
from core.sql_formatter import is_ddl_dml


def _billing_project() -> str | None:
    """Return the project charged for BQ slot usage.
    BQ_BILLING_PROJECT takes precedence; falls back to first allowed project."""
    return config.BQ_BILLING_PROJECT or (config.BQ_ALLOWED_PROJECTS[0] if config.BQ_ALLOWED_PROJECTS else None)


def _get_client():
    """Create a BigQuery client billed to BQ_BILLING_PROJECT (not the data project)."""
    from google.cloud import bigquery
    from core.auth import get_credentials
    creds, _ = get_credentials()
    return bigquery.Client(project=_billing_project(), credentials=creds)


def _validate_project(project_id: str) -> str | None:
    allowed = config.BQ_ALLOWED_PROJECTS
    if not allowed:
        return None
    if project_id not in allowed:
        return f"Project '{project_id}' is not in BQ_ALLOWED_PROJECTS: {allowed}"
    return None


@tool
def query_bigquery(sql: str, project_id: str = None) -> str:
    """Execute read-only SQL against BigQuery.
    Only projects in BQ_ALLOWED_PROJECTS are accessible.
    Never adds LIMIT automatically. Rejects DDL/DML before execution.
    Returns JSON with columns, rows, and stats (bytes_processed, slot_ms, cache_hit, execution_time_ms)."""
    start = time.time()
    try:
        if is_ddl_dml(sql):
            return json.dumps({"error": "DDL/DML not permitted. Only SELECT queries are allowed."})

        if project_id:
            err = _validate_project(project_id)
            if err:
                return json.dumps({"error": err})

        client = _get_client()
        job_config = __import__("google.cloud.bigquery", fromlist=["QueryJobConfig"]).QueryJobConfig()
        job_config.use_query_cache = True

        job = client.query(sql, job_config=job_config)
        result = job.result()
        df = result.to_dataframe()

        duration_ms = int((time.time() - start) * 1000)
        stats = {
            "bytes_processed": job.total_bytes_processed,
            "slot_ms": job.slot_millis,
            "cache_hit": job.cache_hit,
            "execution_time_ms": duration_ms,
            "job_id": job.job_id,
        }
        out = {
            "columns": list(df.columns),
            "rows": df.values.tolist(),
            "row_count": len(df),
            "stats": stats,
        }
        log_audit("bigquery_tools", project_id or "bq", sql, row_count=len(df), duration_ms=duration_ms)
        return json.dumps(out, default=str)
    except Exception as exc:
        log_audit("bigquery_tools", project_id or "bq", sql, duration_ms=int((time.time() - start) * 1000))
        return json.dumps({"error": str(exc)})


@tool
def list_bq_datasets(project_id: str) -> str:
    """List all datasets in a BigQuery project (must be in allowed projects).
    Returns JSON list of dataset IDs."""
    try:
        err = _validate_project(project_id)
        if err:
            return json.dumps({"error": err})
        client = _get_client()
        datasets = [d.dataset_id for d in client.list_datasets(project=project_id)]
        log_audit("bigquery_tools", project_id, "list_datasets", row_count=len(datasets))
        return json.dumps({"project_id": project_id, "datasets": datasets})
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def list_bq_tables(project_id: str, dataset_id: str) -> str:
    """List all tables in a BigQuery dataset with row counts and sizes.
    Returns JSON list."""
    try:
        err = _validate_project(project_id)
        if err:
            return json.dumps({"error": err})
        client = _get_client()
        dataset_ref = client.dataset(dataset_id, project=project_id)
        tables = []
        for tbl in client.list_tables(dataset_ref):
            t = client.get_table(tbl)
            tables.append({
                "table_id": t.table_id,
                "row_count": t.num_rows,
                "size_bytes": t.num_bytes,
                "created": str(t.created),
                "modified": str(t.modified),
                "table_type": t.table_type,
            })
        log_audit("bigquery_tools", f"{project_id}.{dataset_id}", "list_tables", row_count=len(tables))
        return json.dumps({"project_id": project_id, "dataset_id": dataset_id, "tables": tables}, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def get_bq_job_stats(job_id: str, project_id: str = None) -> str:
    """Fetch execution stats for a completed BigQuery job:
    bytes_processed, slot_ms, cache_hit, scan_type, execution_plan.
    project_id: the project where the job ran (defaults to BQ_BILLING_PROJECT).
    Returns JSON stats object."""
    try:
        resolved = project_id or _billing_project()
        if not resolved:
            return json.dumps({"error": "project_id required — BQ_BILLING_PROJECT not configured."})
        client = _get_client()
        job = client.get_job(job_id, project=resolved)
        stats = {
            "job_id": job_id,
            "state": job.state,
            "bytes_processed": job.total_bytes_processed,
            "slot_ms": job.slot_millis,
            "cache_hit": job.cache_hit,
            "created": str(job.created),
            "started": str(job.started),
            "ended": str(job.ended),
        }
        log_audit("bigquery_tools", resolved, f"job_stats:{job_id}")
        return json.dumps(stats, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})
