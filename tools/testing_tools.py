"""Query output comparison and optimisation validation tools."""
import hashlib
import json
from core.json_utils import safe_json
import time

from langchain.tools import tool

from core.audit import log_audit
from core.sql_formatter import is_ddl_dml


def _hash_df(df) -> str:
    import pandas as pd
    sorted_df = df.sort_values(by=list(df.columns)).reset_index(drop=True)
    return hashlib.sha256(sorted_df.to_csv(index=False).encode()).hexdigest()


def _run_bigquery(sql: str) -> tuple:
    from tools.bigquery_tools import _get_client
    from core import config
    project_id = config.BQ_ALLOWED_PROJECTS[0] if config.BQ_ALLOWED_PROJECTS else None
    client = _get_client(project_id)
    job = client.query(sql)
    df = job.result().to_dataframe()
    return df, None


@tool
def compare_query_outputs(original_sql: str, optimised_sql: str) -> str:
    """Run both queries against BigQuery and compare results.
    Compares: row_count, column names and types, SHA-256 row hash
    (both result sets sorted on all columns before hashing).
    Returns JSON: status (PASS/FAIL), row_count_match, column_match,
    data_hash_match, diff_rows (first 100 mismatches)."""
    start = time.time()
    try:
        if is_ddl_dml(original_sql) or is_ddl_dml(optimised_sql):
            return json.dumps({"error": "DDL/DML not permitted."})

        orig_df, _ = _run_bigquery(original_sql)
        opt_df, _ = _run_bigquery(optimised_sql)

        row_count_match = len(orig_df) == len(opt_df)
        column_match = list(orig_df.columns) == list(opt_df.columns)
        orig_hash = _hash_df(orig_df) if column_match else None
        opt_hash = _hash_df(opt_df) if column_match else None
        data_hash_match = orig_hash == opt_hash if column_match else False

        diff_rows = []
        if column_match and not data_hash_match:
            orig_sorted = orig_df.sort_values(by=list(orig_df.columns)).reset_index(drop=True)
            opt_sorted = opt_df.sort_values(by=list(opt_df.columns)).reset_index(drop=True)
            max_rows = min(len(orig_sorted), len(opt_sorted), 100)
            for i in range(max_rows):
                if not orig_sorted.iloc[i].equals(opt_sorted.iloc[i]):
                    diff_rows.append({
                        "row": i,
                        "original": orig_sorted.iloc[i].to_dict(),
                        "optimised": opt_sorted.iloc[i].to_dict(),
                    })

        status = "PASS" if row_count_match and column_match and data_hash_match else "FAIL"
        result = {
            "status": status,
            "row_count_match": row_count_match,
            "original_row_count": len(orig_df),
            "optimised_row_count": len(opt_df),
            "column_match": column_match,
            "original_columns": list(orig_df.columns),
            "optimised_columns": list(opt_df.columns),
            "data_hash_match": data_hash_match,
            "diff_rows": diff_rows[:100],
        }
        log_audit("testing_tools", "bigquery", "compare_query_outputs",
                  duration_ms=int((time.time()-start)*1000))
        return safe_json(result)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def validate_optimisation(original_sql: str, optimised_sql: str) -> str:
    """Full validation: compare_query_outputs + AI structural checklist.
    Checklist items: output_columns_unchanged, where_logic_preserved, join_conditions_identical,
    aggregation_unchanged, group_by_preserved, no_new_filters_added, business_logic_intact.
    Returns JSON: overall_verdict (SAFE/UNSAFE), comparison_result, checklist [{item, status}]."""
    start = time.time()
    try:
        comparison_raw = compare_query_outputs.run({
            "original_sql": original_sql,
            "optimised_sql": optimised_sql,
        })
        comparison = json.loads(comparison_raw) if isinstance(comparison_raw, str) else comparison_raw

        from langchain_google_genai import ChatGoogleGenerativeAI
        from langchain_core.messages import HumanMessage
        from core import config
        llm = ChatGoogleGenerativeAI(model=config.AGENT_MODEL, temperature=0)
        checklist_items = [
            "output_columns_unchanged",
            "where_logic_preserved",
            "join_conditions_identical",
            "aggregation_unchanged",
            "group_by_preserved",
            "no_new_filters_added",
            "business_logic_intact",
        ]
        prompt = f"""Compare these two SQL queries structurally. For each checklist item return pass/warn/fail.
Original SQL:
{original_sql}

Optimised SQL:
{optimised_sql}

Checklist items: {json.dumps(checklist_items)}
Return JSON only: list of {{item, status (pass/warn/fail), reason}}. No markdown."""
        response = llm.invoke([HumanMessage(content=prompt)])
        raw = response.content.strip()
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:])
            if raw.endswith("```"):
                raw = raw[:-3]
        checklist = json.loads(raw)

        all_pass = comparison.get("status") == "PASS"
        any_fail = any(c.get("status") == "fail" for c in checklist)
        verdict = "SAFE" if all_pass and not any_fail else "UNSAFE"

        result = {
            "overall_verdict": verdict,
            "comparison_result": comparison,
            "checklist": checklist,
        }
        log_audit("testing_tools", "bigquery", "validate_optimisation",
                  duration_ms=int((time.time()-start)*1000))
        return safe_json(result)
    except Exception as exc:
        return json.dumps({"error": str(exc)})
