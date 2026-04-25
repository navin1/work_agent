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
        st.warning("No SQL found for this task.")
        if data.get("rendered_warning"):
            st.error(f"Rendered fields error: {data['rendered_warning']}")
        debug = data.get("_debug", {})
        if debug:
            with st.expander("🔍 Diagnosis"):
                st.json(debug)
        return

    dag_id  = data.get("dag_id", "")
    task_id = data.get("task_id", "")
    label   = "Rendered SQL" if data.get("rendered_sql") else "Raw SQL (no successful run found)"

    lines  = sql.count("\n") + 1
    # Cap at 900px so massive SQL doesn't inflate the page — Monaco scrolls internally.
    height = min(max(300, lines * 22 + 60), 900)
    st.caption(f"**{label}** — `{dag_id}` / `{task_id}`  ·  {lines:,} lines")
    components.html(monaco.editor(sql, language="sql", height=height), height=height + 20)
    st.download_button(
        "⬇ Download SQL",
        data=sql.encode("utf-8"),
        file_name=f"{dag_id}__{task_id}.sql",
        mime="text/plain",
        key=f"dl_tasksql_{id(raw_json)}",
    )


def render_dag_rendered_files(raw_json: str) -> None:
    import streamlit.components.v1 as components
    from core import monaco

    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse DAG rendered files result.")
        return

    if "error" in data:
        st.error(f"DAG rendered files error: {data['error']}")
        return

    dag_id = data.get("dag_id", "")
    tasks_sql = data.get("tasks_sql", [])
    if not tasks_sql:
        st.info(f"No SQL-bearing tasks found in **{dag_id}**.")
        return

    st.caption(f"**{len(tasks_sql)}** task(s) with SQL in **{dag_id}**")
    for t in tasks_sql:
        tid = t.get("task_id", "")
        op  = t.get("operator", "")
        sql = t.get("rendered_sql") or t.get("raw_sql") or ""
        label = "Rendered SQL" if t.get("rendered_sql") else "Raw SQL"
        with st.expander(f"`{tid}`  —  {op}  ({label})", expanded=False):
            if sql:
                lines  = sql.count("\n") + 1
                height = min(max(280, lines * 22 + 60), 900)
                st.caption(f"{lines:,} lines")
                components.html(monaco.editor(sql, language="sql", height=height), height=height + 20)
                st.download_button(
                    "⬇ Download SQL",
                    data=sql.encode("utf-8"),
                    file_name=f"{dag_id}__{tid}.sql",
                    mime="text/plain",
                    key=f"dl_rendered_{id(raw_json)}_{tid}",
                )
            else:
                st.info("No SQL found for this task.")


def render_dag_task_graph(raw_json: str) -> None:
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse task graph result.")
        return

    if "error" in data:
        st.error(f"Task graph error: {data['error']}")
        return

    dag_id    = data.get("dag_id", "")
    run_id    = data.get("run_id", "—")
    run_state = data.get("run_state", "")
    tasks     = data.get("tasks", [])

    STATE_ICON = {
        "success": "✅", "failed": "❌", "running": "🔄",
        "skipped": "⏭", "upstream_failed": "⚠️", "queued": "⏳",
    }
    rows = [
        {
            "Task": t.get("task_id", ""),
            "Operator": t.get("operator", "").replace("Operator", ""),
            "State": STATE_ICON.get(t.get("state", ""), "○") + " " + (t.get("state") or "—"),
            "Duration (s)": round(t["duration_seconds"], 1) if t.get("duration_seconds") else "—",
            "Try": t.get("try_number") or "—",
            "Depends On": ", ".join(t.get("depends_on") or []) or "—",
        }
        for t in tasks
    ]
    df = pd.DataFrame(rows)

    state_label = f"  ·  Run state: **{run_state}**" if run_state else ""
    st.caption(f"**{dag_id}**  ·  run: `{run_id}`{state_label}")
    st.dataframe(df, hide_index=True, use_container_width=True)

    diagram = data.get("diagram", "")
    if diagram:
        with st.expander("Dependency diagram", expanded=False):
            st.code(diagram, language=None)

    col1, _ = st.columns([1, 5])
    with col1:
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        st.download_button("⬇ CSV", buf.getvalue(), f"{dag_id}_tasks.csv",
                           mime="text/csv", key=f"tg_csv_{id(raw_json)}")


def render_dag_details(raw_json: str) -> None:
    import streamlit.components.v1 as components
    from core import monaco

    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse DAG details result.")
        return

    if "error" in data:
        st.error(f"DAG details error: {data['error']}")
        return

    dag_id = data.get("dag_id", "")
    tasks  = data.get("tasks", [])
    source = data.get("dag_source", "")

    if tasks:
        rows = [
            {
                "Task": t.get("task_id", ""),
                "Operator": t.get("operator", "").replace("Operator", ""),
                "Depends On": ", ".join(t.get("depends_on") or []) or "—",
            }
            for t in tasks
        ]
        st.caption(f"**{dag_id}**  ·  {len(tasks)} task(s)")
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

    if source and source != "(source not available)":
        with st.expander("DAG source", expanded=False):
            lines  = source.count("\n") + 1
            height = max(300, min(lines * 18 + 40, 800))
            components.html(monaco.editor(source, language="python", height=height), height=height + 20)
            st.download_button(
                "⬇ Download DAG source",
                data=source.encode("utf-8"),
                file_name=f"{dag_id}.py",
                mime="text/plain",
                key=f"dl_dagsrc_{id(raw_json)}",
            )


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
