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

    rows = [
        {
            "DAG ID": d.get("dag_id", ""),
            "Schedule": d.get("schedule") or "—",
            "Paused": "⏸ Yes" if d.get("is_paused") else "▶ No",
            "Last Run": d.get("last_run_time", "") or "—",
            "Tags": ", ".join(d.get("tags") or []) or "—",
            "Subfolder": d.get("subfolder", "") or "—",
        }
        for d in dags
    ]
    df = pd.DataFrame(rows)

    table_key = f"dag_table_{id(raw_json)}"
    st.caption(f"**{len(dags)}** DAGs in **{env}** — click a row to inspect")
    selection = st.dataframe(
        df,
        hide_index=True,
        use_container_width=True,
        on_select="rerun",
        selection_mode="single-row",
        key=table_key,
    )

    selected_rows = (selection.selection.rows
                     if selection and hasattr(selection, "selection")
                     else [])
    if selected_rows:
        dag_id = dags[selected_rows[0]].get("dag_id", "")
        col_a, col_b, col_c = st.columns([2, 2, 4])
        with col_a:
            if st.button("🔍 DAG Details", key=f"dag_det_{dag_id}_{id(raw_json)}"):
                st.session_state.chat_prefill = f"get dag details for {dag_id} in {env}"
                st.rerun()
        with col_b:
            if st.button("📊 Run History", key=f"dag_hist_{dag_id}_{id(raw_json)}"):
                st.session_state.chat_prefill = f"show run history for {dag_id} in {env}"
                st.rerun()
        with col_c:
            if st.button("🗺 Task Graph", key=f"dag_graph_{dag_id}_{id(raw_json)}"):
                st.session_state.chat_prefill = f"show task graph for {dag_id} in {env}"
                st.rerun()

    col1, _ = st.columns([1, 5])
    with col1:
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        st.download_button("⬇ CSV", buf.getvalue(), f"{env}_dags.csv",
                           mime="text/csv", key=f"dags_csv_{id(raw_json)}")


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
