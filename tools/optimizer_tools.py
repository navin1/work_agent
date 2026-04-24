"""SQL and DAG optimisation tools."""
import json
import time

from langchain.tools import tool

from core import config
from core.audit import log_audit
from core.llm import get_llm
from core.sql_formatter import format_sql


_OPTIMISE_SYSTEM_PROMPT = """You are a BigQuery/Airflow SQL performance expert.
Environment: Airflow {airflow_version}, BQ SDK {bq_sdk}, Python {python_version}.
ABSOLUTE CONSTRAINT: Do NOT change functional output, business logic,
column names, data types, or row-level semantics. Optimize only for:
partition filtering, clustering keys, JOIN order, CTE extraction,
subquery elimination, scan reduction, slot efficiency.
For each change return structured JSON: change_type, original_snippet,
optimised_snippet, reason, estimated_impact (High/Medium/Low),
confidence (High/Medium/Low).
Also return: overall_confidence_score (0-100), overall_summary.
Return JSON only. No markdown, no preamble."""


def _flag_sql(sql: str) -> list[dict]:
    """Analyse SQL for performance issues using sqlglot AST."""
    flags = []
    try:
        import sqlglot
        import sqlglot.expressions as exp

        tree = sqlglot.parse_one(sql, read="bigquery")

        # SELECT *
        for select in tree.find_all(exp.Star):
            flags.append({
                "flag_type": "select_star",
                "severity": "medium",
                "description": "SELECT * scans all columns — specify only needed columns.",
                "line_hint": None,
            })
            break

        # No WHERE clause on FROM tables
        has_where = tree.find(exp.Where)
        if not has_where:
            flags.append({
                "flag_type": "full_table_scan",
                "severity": "high",
                "description": "No WHERE clause found — this may result in a full table scan.",
                "line_hint": None,
            })

        # Repeated subqueries (same subquery appears multiple times)
        subqueries = list(tree.find_all(exp.Subquery))
        sub_texts = [s.sql() for s in subqueries]
        if len(sub_texts) != len(set(sub_texts)):
            flags.append({
                "flag_type": "repeated_subquery",
                "severity": "medium",
                "description": "Repeated subquery detected — extract to a CTE for efficiency.",
                "line_hint": None,
            })

        # Non-SARGable: functions on columns in WHERE
        if has_where:
            for func in tree.find(exp.Where).find_all(exp.Anonymous) if tree.find(exp.Where) else []:
                flags.append({
                    "flag_type": "non_sargable",
                    "severity": "medium",
                    "description": f"Function '{func.name}' applied to column in WHERE — may prevent partition/index use.",
                    "line_hint": None,
                })
                break

    except Exception:
        flags.append({
            "flag_type": "parse_error",
            "severity": "low",
            "description": "Could not fully parse SQL for AST analysis — manual review recommended.",
            "line_hint": None,
        })
    return flags


@tool
def get_sql_flags(sql: str) -> str:
    """Analyse SQL using sqlglot AST for performance issues.
    Detects: full table scan, missing partition filter, SELECT *, non-SARGable predicates,
    repeated subqueries (CTE candidates), inefficient JOIN order, missing clustering key filter.
    Returns JSON list of {flag_type, severity, description, line_hint}."""
    try:
        flags = _flag_sql(sql)
        log_audit("optimizer_tools", "sqlglot", "get_sql_flags", row_count=len(flags))
        return json.dumps({"flags": flags, "count": len(flags)})
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def optimise_sql(sql: str, composer_env: str = None) -> str:
    """Generate AI-optimised version of SQL using LLM.
    Uses Composer SDK versions from env vars when composer_env provided.
    HARD CONSTRAINT enforced in prompt: must not change functional output, business logic,
    column names, data types, or row-level semantics.
    Returns JSON with: original_sql, optimised_sql,
    changes [{change_type, original_snippet, optimised_snippet, reason, estimated_impact, confidence}],
    overall_confidence_score (0-100), overall_summary."""
    start = time.time()
    try:
        sdk_info = config.get_composer_sdk_info(composer_env) if composer_env else {
            "airflow_version": "2.6.3",
            "bq_sdk": "google-cloud-bigquery==3.11.0",
            "python_version": "3.10",
        }
        system = _OPTIMISE_SYSTEM_PROMPT.format(**sdk_info)
        llm = get_llm()
        from langchain_core.messages import SystemMessage, HumanMessage
        response = llm.invoke([
            SystemMessage(content=system),
            HumanMessage(content=f"Optimise this SQL:\n\n{sql}"),
        ])
        raw = response.content.strip()
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:])
            if raw.endswith("```"):
                raw = raw[:-3]
        parsed = json.loads(raw)
        parsed["original_sql"] = format_sql(sql)
        if "optimised_sql" in parsed:
            parsed["optimised_sql"] = format_sql(parsed["optimised_sql"])
        log_audit("optimizer_tools", "llm", "optimise_sql", duration_ms=int((time.time()-start)*1000))
        return json.dumps(parsed)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def optimise_dag(composer_env: str, dag_id: str) -> str:
    """Structural optimisation suggestions for a DAG.
    Covers: task parallelism, redundant dependencies, trigger rules, sensor timeouts.
    Tailored to Airflow version from env vars.
    HARD CONSTRAINT: no functional changes.
    Returns JSON with suggestions [{description, current_code, suggested_code, reason, confidence}]."""
    start = time.time()
    try:
        from tools.composer_tools import _fetch_dag_source
        source = _fetch_dag_source(dag_id) or ""
        if not source:
            return json.dumps({"error": "Could not retrieve DAG source code from GCS or Git."})

        sdk_info = config.get_composer_sdk_info(composer_env)
        llm = get_llm()
        from langchain_core.messages import SystemMessage, HumanMessage
        system = f"""You are an Apache Airflow DAG optimisation expert.
Airflow version: {sdk_info['airflow_version']}, Python: {sdk_info['python_version']}.
ABSOLUTE CONSTRAINT: Do NOT suggest changes that alter functional behaviour, data outputs, or business logic.
Only suggest structural improvements: task parallelism, dependency graph, trigger rules, sensor timeouts, pool usage.
Return JSON only: list of suggestions, each with: description, current_code, suggested_code, reason, confidence (High/Medium/Low).
No markdown, no preamble."""
        response = llm.invoke([
            SystemMessage(content=system),
            HumanMessage(content=f"Optimise this DAG:\n\n{source}"),
        ])
        raw = response.content.strip()
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:])
            if raw.endswith("```"):
                raw = raw[:-3]
        suggestions = json.loads(raw)
        log_audit("optimizer_tools", composer_env, f"optimise_dag:{dag_id}", duration_ms=int((time.time()-start)*1000))
        return json.dumps({"dag_id": dag_id, "suggestions": suggestions})
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def optimise_all_dag_sqls(composer_env: str, dag_id: str) -> str:
    """Optimise ALL SQL queries across every task in a DAG.
    For each task containing SQL: runs get_sql_flags then AI optimisation.
    HARD CONSTRAINT: no functional/output changes — performance only.
    Returns JSON with per-task results: task_id, flags, original_sql, optimised_sql, changes, confidence_score."""
    start = time.time()
    try:
        from tools.composer_tools import _get, _get_headers, _base_url
        import requests

        tasks_data = _get(composer_env, f"/dags/{dag_id}/tasks")
        tasks = tasks_data.get("tasks", [])

        # Get last successful run for rendered context
        run_id = None
        try:
            runs_data = _get(composer_env, f"/dags/{dag_id}/dagRuns", {
                "limit": 5, "order_by": "-start_date", "state": "success"
            })
            if runs_data.get("dag_runs"):
                run_id = runs_data["dag_runs"][0]["dag_run_id"]
        except Exception:
            pass

        sdk_info = config.get_composer_sdk_info(composer_env)
        system = _OPTIMISE_SYSTEM_PROMPT.format(**sdk_info)
        llm = get_llm()
        from langchain_core.messages import SystemMessage, HumanMessage

        results = []
        for task in tasks:
            task_id = task.get("task_id", "")
            sql = None

            # Try rendered fields first
            if run_id:
                try:
                    inst = _get(composer_env, f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/renderedFields")
                    for field in ["sql", "query", "bql"]:
                        if field in inst and inst[field]:
                            sql = inst[field]
                            break
                except Exception:
                    pass

            # Fall back to task definition
            if not sql:
                try:
                    task_data = _get(composer_env, f"/dags/{dag_id}/tasks/{task_id}")
                    for field in ["sql", "query", "bql"]:
                        val = task_data.get(field) or task_data.get("template_fields", {}).get(field)
                        if val:
                            sql = val
                            break
                except Exception:
                    pass

            if not sql or not sql.strip():
                results.append({"task_id": task_id, "has_sql": False})
                continue

            flags = _flag_sql(sql)
            task_result = {
                "task_id": task_id,
                "has_sql": True,
                "original_sql": format_sql(sql),
                "flags": flags,
                "optimised_sql": None,
                "changes": [],
                "confidence_score": None,
                "error": None,
            }

            try:
                response = llm.invoke([
                    SystemMessage(content=system),
                    HumanMessage(content=f"Optimise this SQL:\n\n{sql}"),
                ])
                raw = response.content.strip()
                if raw.startswith("```"):
                    raw = "\n".join(raw.split("\n")[1:])
                    if raw.endswith("```"):
                        raw = raw[:-3]
                parsed = json.loads(raw)
                task_result["optimised_sql"] = format_sql(parsed.get("optimised_sql", sql))
                task_result["changes"] = parsed.get("changes", [])
                task_result["confidence_score"] = parsed.get("overall_confidence_score")
                task_result["summary"] = parsed.get("overall_summary", "")
            except Exception as e:
                task_result["error"] = str(e)

            results.append(task_result)

        sql_tasks = [r for r in results if r.get("has_sql")]
        log_audit("optimizer_tools", composer_env, f"optimise_all_dag_sqls:{dag_id}",
                  row_count=len(sql_tasks), duration_ms=int((time.time()-start)*1000))
        return json.dumps({
            "dag_id": dag_id,
            "total_tasks": len(tasks),
            "sql_tasks": len(sql_tasks),
            "results": results,
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def optimise_sql_file(file_path: str, composer_env: str = None) -> str:
    """Optimise a SQL file fetched from GCS or Git by its path.
    file_path: GCS path (gs://bucket/path/file.sql) or Git path (e.g. sql/rps800/load.sql).
    HARD CONSTRAINT: no functional/output changes — performance only.
    Returns JSON with original_sql, optimised_sql, flags, changes, confidence_score."""
    start = time.time()
    try:
        sql = None

        # GCS path
        if file_path.startswith("gs://"):
            try:
                from google.cloud import storage
                from core.auth import get_credentials
                creds, _ = get_credentials()
                client = storage.Client(credentials=creds)
                # gs://bucket/path/to/file.sql
                parts = file_path[5:].split("/", 1)
                bucket_name = parts[0]
                blob_path = parts[1] if len(parts) > 1 else ""
                blob = client.bucket(bucket_name).blob(blob_path)
                sql = blob.download_as_text()
            except Exception as e:
                return json.dumps({"error": f"Failed to read GCS file: {e}"})

        # Git/local path
        if sql is None:
            try:
                from tools.composer_tools import _fetch_file_from_git
                sql = _fetch_file_from_git(file_path)
            except Exception:
                pass

        # Local filesystem fallback
        if sql is None:
            from pathlib import Path
            local = Path(file_path)
            if local.exists():
                sql = local.read_text(encoding="utf-8")

        if not sql:
            return json.dumps({"error": f"Could not read SQL file: {file_path}"})

        flags = _flag_sql(sql)

        sdk_info = config.get_composer_sdk_info(composer_env) if composer_env else {
            "airflow_version": "2.6.3",
            "bq_sdk": "google-cloud-bigquery==3.11.0",
            "python_version": "3.10",
        }
        system = _OPTIMISE_SYSTEM_PROMPT.format(**sdk_info)
        llm = get_llm()
        from langchain_core.messages import SystemMessage, HumanMessage
        response = llm.invoke([
            SystemMessage(content=system),
            HumanMessage(content=f"Optimise this SQL:\n\n{sql}"),
        ])
        raw = response.content.strip()
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:])
            if raw.endswith("```"):
                raw = raw[:-3]
        parsed = json.loads(raw)

        log_audit("optimizer_tools", "llm", f"optimise_sql_file:{file_path}",
                  duration_ms=int((time.time()-start)*1000))
        return json.dumps({
            "file_path": file_path,
            "original_sql": format_sql(sql),
            "optimised_sql": format_sql(parsed.get("optimised_sql", sql)),
            "flags": flags,
            "changes": parsed.get("changes", []),
            "overall_confidence_score": parsed.get("overall_confidence_score"),
            "overall_summary": parsed.get("overall_summary", ""),
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})
