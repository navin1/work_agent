"""SQL and DAG optimisation tools."""
import json
from core.json_utils import safe_json, extract_json
import time

from langchain.tools import tool
from langchain_core.messages import SystemMessage, HumanMessage

from core import config
from core.audit import log_audit
from core.llm import get_llm
from core.sql_formatter import format_sql
from pathlib import Path

from tools.composer_tools import _fetch_dag_source, _dag_source_not_found_error, _get, _fetch_file_from_git


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


_DAG_OPTIMISE_SYSTEM_PROMPT = """You are an Apache Airflow DAG optimisation expert and technical documentation writer.
Airflow version: {airflow_version}, Python: {python_version}.
ABSOLUTE CONSTRAINT: Do NOT suggest changes that alter functional behaviour, data outputs, business logic, or scheduling semantics.

═══ DAG LOADING RULES — violations prevent Airflow from discovering the DAG ═══

RULE 1 — dag=dag and context manager are an ATOMIC pair:
  • dag=dag in task constructors is ONLY valid when the DAG is defined as a plain variable:
      dag = DAG('id', ...)
      task = BashOperator(..., dag=dag)   ← required here
  • When the DAG uses `with DAG(...) as dag:`, tasks inside the block must NOT have dag=dag.
  • To modernise: convert `dag = DAG(...)` → `with DAG(...) as dag:`, indent all tasks
    inside the block, THEN remove dag=dag. Do NOT remove dag=dag without the context
    manager already wrapping the tasks — doing so silently disconnects tasks from the DAG.

RULE 2 — catchup=False must be explicit:
  • Airflow defaults to catchup=True. Without catchup=False, turning on a DAG after any
    gap floods the scheduler with backfill runs for every missed interval from start_date.
  • Always add catchup=False to the DAG constructor unless the user intentionally needs backfill.

RULE 3 — DAG must remain at module level:
  • Airflow's DagBag discovers DAGs by importing the file and scanning for DAG objects at
    module scope. Never move the DAG into a function (unless using the @dag decorator),
    a conditional block, or a try/except.

RULE 4 — start_date must be a fixed datetime:
  • start_date=datetime.now() or datetime.today() creates a moving target — Airflow will
    never schedule the DAG correctly. It must be a hardcoded date, e.g. datetime(2024, 1, 1).

RULE 5 — no top-level side-effects:
  • Variable.get(), Connection.get(), or any DB/API call at module scope runs on every
    scheduler parse cycle (every 30 s by default). Flag these and move them inside tasks.

═══ MODERNISATION suggestions (tailor to the Airflow version above) ═══

  - Remove deprecated `provide_context=True` from PythonOperator (Airflow ≥ 2.0).
  - Update legacy import paths:
      airflow.operators.bash_operator      → airflow.operators.bash
      airflow.operators.python_operator    → airflow.operators.python
      airflow.sensors.base_sensor_operator → airflow.sensors.base
  - Replace `schedule_interval` with `schedule` (Airflow ≥ 2.4).
  - Replace `execution_date` Jinja macro / Python variable with `logical_date` (Airflow ≥ 2.2).
  - Replace `DummyOperator` with `EmptyOperator` (Airflow ≥ 2.4).
  - Replace `.set_upstream()` / `.set_downstream()` calls with `>>` / `<<` bitshift operators.
  - Replace `PythonOperator` with `@task` decorator (TaskFlow API) for pure-function callables (Airflow ≥ 2.0).
  - Add `deferrable=True` to long-running operators (BigQuery, Dataproc, Sensors) to free worker slots (Airflow ≥ 2.2).
  - Replace Python `for` loops generating tasks with Dynamic Task Mapping `.expand()` / `.partial()` (Airflow ≥ 2.3).
  - Replace complex trigger_rule init/cleanup patterns with `@setup` / `@teardown` decorators (Airflow ≥ 2.7).
  - Remove duplicate keys already in `default_args` (retries, retry_delay, owner set per-task).
  - Add `doc_md` to the DAG if missing.

═══ STRUCTURAL suggestions ═══

  - Identify sequential tasks that are independent and can run in parallel.
  - Suggest `task_groups` for logically related tasks.
  - Add or tighten `sensor_timeout` / `poke_interval` on sensors.
  - Add `retries` and `retry_delay` to `default_args` if missing.
  - Suggest `execution_timeout` for long-running tasks.
  - Add `trigger_rule` where ALL_SUCCESS is unnecessarily strict.
  - Suggest `pool` assignment for resource-heavy tasks.
  - Consolidate redundant branching operators.

Your response MUST be a single JSON object — NOT a list, NOT wrapped in any other structure.
No markdown. No preamble. No trailing text. Start the response with {{ and end with }}.

{{
  "suggestions": [
    {{
      "description": "<what to change>",
      "current_code": "<existing code snippet>",
      "suggested_code": "<improved code snippet>",
      "reason": "<why this improves the DAG>",
      "category": "<dag_loading | modernisation | structural>",
      "confidence": "<High | Medium | Low>"
    }}
  ],
  "doc_md": {{
    "overview": "<3-4 crisp sentences: what this DAG does, what data it processes, what it loads/computes, and its business purpose. Infer from task names, operator types, and any SQL provided.>",
    "control_m_job": "<DAG id converted to UPPER_SNAKE_CASE, e.g. dag_rps800_load → DAG_RPS800_LOAD>",
    "impacted_objects": [
      {{
        "name": "<schema.table_or_view as it literally appears in the SQL>",
        "description": "<one-line description of what this object holds>",
        "operation": "<read | write | read/write>",
        "type": "<table | view>"
      }}
    ]
  }}
}}

Rules for impacted_objects (read from the RENDERED SQL blocks if provided):
  • FROM clause / JOIN → operation "read"
  • INSERT INTO / CREATE OR REPLACE TABLE / MERGE INTO target → operation "write"
  • Appears in both → operation "read/write"
  • Up to 10 objects, schema-qualified exactly as written in the SQL.
  • Names ending in _view / _vw / _v, or used only as SELECT sources → type "view"; otherwise "table".
  • If no rendered SQL is provided, infer table names from operator params and file path hints in the DAG.
  • doc_md is MANDATORY — always populate overview and control_m_job even if SQL is unavailable."""


def _call_llm(system: str, user_content: str) -> str:
    """Invoke the LLM with a system + user message and return raw text."""
    return get_llm().invoke([
        SystemMessage(content=system),
        HumanMessage(content=user_content),
    ]).content


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
        sdk_info = config.get_composer_sdk_info(composer_env) if composer_env else config.get_default_sdk_info()
        system = _OPTIMISE_SYSTEM_PROMPT.format(**sdk_info)
        raw = _call_llm(system, f"Optimise this SQL:\n\n{sql}")
        parsed = extract_json(raw)
        parsed["original_sql"] = format_sql(sql)
        if "optimised_sql" in parsed:
            parsed["optimised_sql"] = format_sql(parsed["optimised_sql"])
        log_audit("optimizer_tools", "llm", "optimise_sql", duration_ms=int((time.time()-start)*1000))
        return json.dumps(parsed)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def optimise_dag(composer_env: str, dag_id: str) -> str:
    """Optimisation suggestions for a DAG in three categories: dag_loading, modernisation, structural.
    dag_loading: catchup=False, atomic dag=dag+context-manager conversion, module-level DAG,
      fixed start_date, no top-level side-effects — fixes that prevent Airflow from loading the DAG.
    Modernisation: provide_context, legacy import paths, schedule_interval→schedule,
      execution_date→logical_date, DummyOperator→EmptyOperator, set_upstream→>>, TaskFlow @task.
    Structural: task parallelism, dependency graph, trigger rules, sensor timeouts, pool usage.
    Tailored to the Airflow version from env vars. HARD CONSTRAINT: no functional changes.
    Returns JSON with suggestions [{description, current_code, suggested_code, reason, category, confidence}]
    and doc_md {overview, control_m_job, impacted_objects}."""
    start = time.time()
    try:
        source = _fetch_dag_source(dag_id, composer_env) or ""
        if not source:
            return json.dumps(_dag_source_not_found_error(dag_id, composer_env))

        # Fetch rendered SQL from every task via Airflow API (accurate table names)
        sql_context = ""
        try:
            tasks_data = _get(composer_env, f"/dags/{dag_id}/tasks")
            tasks = tasks_data.get("tasks", [])

            run_id = None
            try:
                runs_data = _get(composer_env, f"/dags/{dag_id}/dagRuns", {
                    "limit": 5, "order_by": "-start_date", "state": "success",
                })
                if runs_data.get("dag_runs"):
                    run_id = runs_data["dag_runs"][0]["dag_run_id"]
            except Exception:
                pass

            for task in tasks[:20]:
                task_id = task.get("task_id", "")
                sql = None

                # Rendered fields first (Jinja-resolved, real table names)
                if run_id:
                    try:
                        inst = _get(
                            composer_env,
                            f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/renderedFields",
                        )
                        for field in ["sql", "query", "bql"]:
                            if inst.get(field):
                                sql = inst[field]
                                break
                    except Exception:
                        pass

                # Fall back to task definition (may contain Jinja templates)
                if not sql:
                    try:
                        task_def = _get(composer_env, f"/dags/{dag_id}/tasks/{task_id}")
                        for field in ["sql", "query", "bql"]:
                            val = task_def.get(field, "")
                            if val and str(val).strip():
                                sql = str(val)
                                break
                    except Exception:
                        pass

                if sql and sql.strip():
                    sql_context += f"\n\n--- Rendered SQL · task: {task_id} ---\n{sql[:3000]}"
        except Exception:
            pass

        sdk_info = config.get_composer_sdk_info(composer_env)
        system = _DAG_OPTIMISE_SYSTEM_PROMPT.format(**sdk_info)
        user_content = f"Optimise this DAG (id: {dag_id}):\n\n{source}"
        if sql_context:
            user_content += (
                "\n\n=== RENDERED SQL FROM TASKS "
                "(extract all table/view names for impacted_objects from here) ==="
                + sql_context
            )

        raw = _call_llm(system, user_content)
        parsed = extract_json(raw)

        # Handle both new {suggestions, doc_md} format and legacy list format
        if isinstance(parsed, list):
            suggestions = parsed
            doc_md = {
                "overview": "",
                "control_m_job": dag_id.upper().replace("-", "_"),
                "impacted_objects": [],
            }
        else:
            suggestions = parsed.get("suggestions", [])
            doc_md = parsed.get("doc_md", {})
            if not doc_md:
                doc_md = {
                    "overview": "",
                    "control_m_job": dag_id.upper().replace("-", "_"),
                    "impacted_objects": [],
                }

        log_audit("optimizer_tools", composer_env, f"optimise_dag:{dag_id}",
                  duration_ms=int((time.time() - start) * 1000))
        return json.dumps({"dag_id": dag_id, "suggestions": suggestions, "doc_md": doc_md})
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
                raw = _call_llm(system, f"Optimise this SQL:\n\n{sql}")
                parsed = extract_json(raw)
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
                sql = _fetch_file_from_git(file_path)
            except Exception:
                pass

        # Local filesystem fallback
        if sql is None:
            local = Path(file_path)
            if local.exists():
                sql = local.read_text(encoding="utf-8")

        if not sql:
            return json.dumps({"error": f"Could not read SQL file: {file_path}"})

        flags = _flag_sql(sql)

        sdk_info = config.get_composer_sdk_info(composer_env) if composer_env else config.get_default_sdk_info()
        system = _OPTIMISE_SYSTEM_PROMPT.format(**sdk_info)
        raw = _call_llm(system, f"Optimise this SQL:\n\n{sql}")
        parsed = extract_json(raw)

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
