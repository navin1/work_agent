"""Results table renderer with export buttons and explain action."""
import json
import io
import pandas as pd
import streamlit as st


def render_dag_list(raw_json: str) -> None:
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse DAG list.")
        return

    if "error" in data:
        st.error(f"DAG list error: {data['error']}")
        return

    dags = data.get("dags", [])
    env = data.get("composer_env", "")
    if not dags:
        st.info(f"No DAGs found in **{env}**.")
        return

    airflow_url = data.get("airflow_url", "").rstrip("/")
    rows = [
        {
            "DAG ID": f"{airflow_url}/dags/{d.get('dag_id', '')}/grid" if airflow_url else d.get("dag_id", ""),
            "Schedule": d.get("schedule") or "—",
            "Paused": "⏸ Yes" if d.get("is_paused") else "▶ No",
            "Last Run": d.get("last_run_time", "") or "—",
            "Tags": ", ".join(d.get("tags") or []) or "—",
            "Subfolder": d.get("subfolder", "") or "—",
        }
        for d in dags
    ]
    df = pd.DataFrame(rows)

    col_config = {}
    if airflow_url:
        col_config["DAG ID"] = st.column_config.LinkColumn(
            "DAG ID",
            display_text=r".*/dags/([^/]+)/grid",
        )

    st.caption(f"**{len(dags)}** DAGs in **{env}**")
    st.dataframe(df, hide_index=True, use_container_width=True, column_config=col_config)

    col1, _ = st.columns([1, 5])
    with col1:
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        st.download_button("⬇ CSV", buf.getvalue(), f"{env}_dags.csv",
                           mime="text/csv", key=f"dags_csv_{id(raw_json)}")


def render_task_sql(raw_json: str) -> None:
    import streamlit.components.v1 as components
    from core import monaco

    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse task SQL result.")
        return

    if "error" in data:
        st.error(f"Task SQL error: {data['error']}")
        return

    sql = data.get("rendered_sql") or data.get("raw_sql")
    if not sql:
        st.info("No SQL found for this task.")
        return

    dag_id  = data.get("dag_id", "")
    task_id = data.get("task_id", "")
    label   = "Rendered SQL" if data.get("rendered_sql") else "Raw SQL (no successful run found)"

    st.markdown(f"**{label}** — `{dag_id}` / `{task_id}`")
    lines  = sql.count("\n") + 1
    height = max(300, lines * 22 + 60)
    components.html(monaco.editor(sql, language="sql", height=height), height=height + 20)


def render(raw_json: str, agent=None) -> None:
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse query result.")
        return

    if "error" in data:
        st.error(f"Query error: {data['error']}")
        return

    columns = data.get("columns", [])
    rows = data.get("rows", [])
    row_count = data.get("row_count", len(rows))
    stats = data.get("stats", {})

    if not columns:
        st.info("No data returned.")
        return

    df = pd.DataFrame(rows, columns=columns)

    # Stats row
    stat_parts = [f"**{row_count:,}** rows"]
    if stats.get("bytes_processed"):
        mb = stats["bytes_processed"] / 1_000_000
        stat_parts.append(f"**{mb:.1f} MB** scanned")
    if stats.get("cache_hit"):
        stat_parts.append("cache hit ✓")
    if stats.get("execution_time_ms"):
        stat_parts.append(f"**{stats['execution_time_ms']}ms**")
    st.caption("  ·  ".join(stat_parts))

    st.dataframe(df, hide_index=True, width="stretch")

    # Export buttons
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        csv_buf = io.StringIO()
        df.to_csv(csv_buf, index=False)
        st.download_button("⬇ CSV", csv_buf.getvalue(), "results.csv", mime="text/csv", key=f"csv_{id(raw_json)}")
    with col2:
        st.download_button("⬇ JSON", json.dumps({"columns": columns, "rows": rows}, default=str),
                           "results.json", mime="application/json", key=f"json_{id(raw_json)}")
    with col3:
        if agent and st.button("🤖 Explain this result", key=f"explain_{id(raw_json)}"):
            sample = df.head(5).to_dict(orient="records")
            from agent.agent import run_agent
            result = run_agent(agent, f"Explain this query result: {row_count} rows returned. Sample data: {json.dumps(sample, default=str)}")
            st.markdown(result.get("output", ""))
