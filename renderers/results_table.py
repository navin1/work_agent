"""Results table renderer with export buttons and explain action."""
import json
import io
from urllib.parse import quote
import pandas as pd
import streamlit as st

# ── Flow diagram constants (used by render_dag_task_graph) ────────────────────

_TG_STATE_COLOR = {
    "success":      "#10B981",
    "failed":       "#EF4444",
    "running":      "#3B82F6",
    "queued":       "#F59E0B",
    "up_for_retry": "#F97316",
    "skipped":      "#9CA3AF",
}
_TG_STATE_ICON = {
    "success":      "✅",
    "failed":       "❌",
    "running":      "🔄",
    "queued":       "⏳",
    "up_for_retry": "🔁",
    "skipped":      "⏭",
}
_TG_TASK_COLOR     = "#8B5CF6"
_TG_CHILD_DAG_COLOR = "#6366F1"


def _tg_node_style(bg: str, min_w: str = "155px") -> dict:
    return {
        "background":   bg,
        "color":        "white",
        "border":       "2px solid rgba(255,255,255,0.18)",
        "borderRadius": "10px",
        "padding":      "10px 14px",
        "fontSize":     "11px",
        "fontWeight":   "600",
        "minWidth":     min_w,
        "textAlign":    "center",
        "whiteSpace":   "pre-wrap",
        "lineHeight":   "1.55",
        "boxShadow":    "0 2px 8px rgba(0,0,0,0.25)",
    }


def _tg_fmt_dur(secs) -> str:
    if secs is None:
        return ""
    s = int(secs)
    return f"{s // 60}m {s % 60}s" if s >= 60 else f"{s}s"


def _build_task_flow_graph(data: dict):
    from streamlit_flow.elements import StreamlitFlowNode, StreamlitFlowEdge

    dag_id       = data.get("dag_id", "")
    tasks        = data.get("tasks", [])
    airflow_url  = data.get("airflow_url", "")
    run_id       = data.get("run_id", "")
    composer_env = data.get("composer_env", "")

    nodes: list = []
    edges: list = []
    content_map: dict = {}
    child_dag_nodes_added: set = set()

    for task in tasks:
        task_id     = task["task_id"]
        task_nid    = f"tg_task__{dag_id}__{task_id}"
        operator    = task.get("operator", "")
        task_state  = task.get("state")
        trigger_dag = task.get("trigger_dag_id")
        is_trigger  = "TriggerDagRun" in operator

        op_short = (
            operator
            .replace("Operator", "")
            .replace("BigQuery", "BQ")
            .strip()
        )
        dur = _tg_fmt_dur(task.get("duration_seconds"))

        label = f"{'🔗' if is_trigger else '📋'}  {task_id}"
        if op_short:
            label += f"\n{op_short}"
        if task_state:
            badge = f"{_TG_STATE_ICON.get(task_state, '⬜')} {task_state}"
            if dur:
                badge += f"  ·  {dur}"
            label += f"\n{badge}"

        task_bg      = _TG_STATE_COLOR.get(task_state, _TG_TASK_COLOR)
        has_children = bool(task.get("depends_on")) or bool(is_trigger and trigger_dag)

        nodes.append(StreamlitFlowNode(
            id=task_nid, pos=(0, 0),
            data={"label": label},
            node_type="default" if has_children else "output",
            source_position="right", target_position="left",
            selectable=True,
            style=_tg_node_style(task_bg),
        ))

        for ds_id in task.get("depends_on", []):
            ds_nid = f"tg_task__{dag_id}__{ds_id}"
            edges.append(StreamlitFlowEdge(
                id=f"e_{task_nid}__{ds_nid}",
                source=task_nid, target=ds_nid,
                edge_type="smoothstep", animated=True,
            ))

        content_map[task_nid] = {
            "type":             "task",
            "dag_id":           dag_id,
            "task_id":          task_id,
            "operator":         operator,
            "state":            task_state,
            "duration_seconds": task.get("duration_seconds"),
            "composer_env":     composer_env,
            "airflow_url":      airflow_url,
            "run_id":           run_id,
        }

        if is_trigger and trigger_dag and trigger_dag not in child_dag_nodes_added:
            child_nid = f"tg_child_dag__{trigger_dag}"
            nodes.append(StreamlitFlowNode(
                id=child_nid, pos=(0, 0),
                data={"label": f"⚙  {trigger_dag}\n(child DAG)"},
                node_type="output",
                source_position="right", target_position="left",
                selectable=True,
                style=_tg_node_style(_TG_CHILD_DAG_COLOR, min_w="180px"),
            ))
            edges.append(StreamlitFlowEdge(
                id=f"e_{task_nid}__child__{trigger_dag}",
                source=task_nid, target=child_nid,
                edge_type="smoothstep", animated=False,
            ))
            content_map[child_nid] = {
                "type":        "child_dag",
                "dag_id":      trigger_dag,
                "parent_dag":  dag_id,
                "airflow_url": airflow_url,
            }
            child_dag_nodes_added.add(trigger_dag)

    return nodes, edges, content_map


def _fetch_task_sql(composer_env: str, dag_id: str, task_id: str, run_id: str) -> str:
    """Fetch rendered SQL for a task. Tries the specific run first, then recent successful runs."""
    from tools.composer_tools import (
        _get, _enc, _extract_rendered_sql, _best_sql,
        _get_sql_file_path, _fetch_sql_file,
    )
    from core.sql_formatter import format_sql, extract_sql

    raw_sql = rendered_sql = None

    # Raw SQL from task definition
    try:
        task_data = _get(composer_env, f"/dags/{dag_id}/tasks/{task_id}")
        path = _get_sql_file_path(task_data)
        if path:
            raw_sql = _fetch_sql_file(path)
        if not raw_sql:
            raw_sql = extract_sql(task_data)
    except Exception:
        pass

    # Rendered SQL — try the specific run_id first
    if run_id and run_id not in ("", "—"):
        try:
            ti = _get(composer_env,
                      f"/dags/{_enc(dag_id)}/dagRuns/{_enc(run_id)}/taskInstances/{_enc(task_id)}")
            rendered_sql = _extract_rendered_sql(ti)
        except Exception:
            pass

    # Fall back to recent successful runs
    if not rendered_sql:
        try:
            runs = _get(composer_env, f"/dags/{dag_id}/dagRuns",
                        {"limit": 10, "order_by": "-execution_date", "state": "success"})
            for r in runs.get("dag_runs", []):
                try:
                    ti = _get(composer_env,
                              f"/dags/{_enc(dag_id)}/dagRuns/{_enc(r['dag_run_id'])}/taskInstances/{_enc(task_id)}")
                    rendered_sql = _extract_rendered_sql(ti)
                    if rendered_sql:
                        break
                except Exception:
                    continue
        except Exception:
            pass

    best = _best_sql(raw_sql, rendered_sql)
    return format_sql(best) if best else ""


def _render_task_content_panel(info: dict) -> None:
    import streamlit.components.v1 as components
    from core import monaco

    st.divider()
    node_type = info["type"]

    if node_type == "task":
        task_id      = info["task_id"]
        dag_id       = info["dag_id"]
        operator     = info.get("operator", "")
        state        = info.get("state")
        dur_s        = info.get("duration_seconds")
        airflow_url  = info.get("airflow_url", "")
        run_id       = info.get("run_id", "")
        composer_env = info.get("composer_env", "")

        st.markdown(f"#### 📋  {task_id}")
        c1, c2, c3 = st.columns(3)
        c1.markdown(f"**DAG:** `{dag_id}`")
        c2.markdown(f"**Operator:** `{operator or '—'}`")
        state_badge = f"{_TG_STATE_ICON.get(state, '⬜')} `{state}`" if state else "`—`"
        c3.markdown(f"**State:** {state_badge}")
        if dur_s:
            st.caption(f"Duration: {_tg_fmt_dur(dur_s)}")

        if airflow_url and run_id and run_id != "—":
            task_url = (
                f"{airflow_url}/dags/{dag_id}/grid"
                f"?dag_run_id={quote(str(run_id), safe='')}&task_id={task_id}"
            )
            st.markdown(f"[↗ Open in Airflow]({task_url})")

        if composer_env:
            sql_key = f"tg_sql_{dag_id}__{task_id}"
            if sql_key not in st.session_state:
                with st.spinner("Fetching SQL…"):
                    try:
                        st.session_state[sql_key] = _fetch_task_sql(
                            composer_env, dag_id, task_id, run_id
                        )
                    except Exception as exc:
                        st.session_state[sql_key] = ""
                        st.warning(f"Could not fetch SQL: {exc}")
            sql = st.session_state[sql_key]
            if sql:
                st.markdown("**Rendered SQL:**")
                lines = sql.count("\n") + 1
                h = min(max(300, lines * 22 + 60), 900)
                components.html(monaco.editor(sql, language="sql", height=h), height=h + 20)
                st.download_button(
                    "⬇ Download SQL",
                    data=sql.encode("utf-8"),
                    file_name=f"{dag_id}__{task_id}.sql",
                    mime="text/plain",
                    key=f"dl_tg_sql_{dag_id}__{task_id}",
                )
            else:
                st.info("No SQL found for this task.")

    elif node_type == "child_dag":
        child_dag_id = info["dag_id"]
        parent_dag   = info.get("parent_dag", "")
        airflow_url  = info.get("airflow_url", "")

        st.markdown(f"#### ⚙  {child_dag_id}")
        st.caption(f"Triggered by: **{parent_dag}**")
        if airflow_url:
            st.markdown(f"[↗ Open DAG in Airflow]({airflow_url}/dags/{child_dag_id}/grid)")


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


def render_dag_task_graph(raw_json: str, is_history: bool = False) -> None:
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse task graph result.")
        return

    if "error" in data:
        st.error(f"Task graph error: {data['error']}")
        return

    dag_id       = data.get("dag_id", "")
    run_id       = data.get("run_id", "—")
    run_state    = data.get("run_state", "")
    tasks        = data.get("tasks", [])
    airflow_url  = data.get("airflow_url", "")

    STATE_ICON = {
        "success": "✅", "failed": "❌", "running": "🔄",
        "skipped": "⏭", "upstream_failed": "⚠️", "queued": "⏳",
    }

    # ── Section 1: Task Table ─────────────────────────────────────────────────
    state_label = f"  ·  Run state: **{run_state}**" if run_state else ""
    st.caption(f"**{dag_id}**  ·  run: `{run_id}`{state_label}")

    rows = []
    for t in tasks:
        task_id = t.get("task_id", "")
        row = {
            "Task": task_id,
            "Operator": t.get("operator", "").replace("Operator", ""),
            "State": STATE_ICON.get(t.get("state", ""), "○") + " " + (t.get("state") or "—"),
            "Duration (s)": round(t["duration_seconds"], 1) if t.get("duration_seconds") else "—",
            "Try": t.get("try_number") or "—",
            "Depends On": ", ".join(t.get("depends_on") or []) or "—",
        }
        if airflow_url and run_id and run_id != "—":
            row["Airflow"] = (
                f"{airflow_url}/dags/{dag_id}/grid"
                f"?dag_run_id={quote(str(run_id), safe='')}&task_id={task_id}"
            )
        rows.append(row)

    df = pd.DataFrame(rows)

    col_config = {}
    if airflow_url and "Airflow" in df.columns:
        col_config["Airflow"] = st.column_config.LinkColumn("Airflow ↗", display_text="Open ↗")

    st.dataframe(df, hide_index=True, use_container_width=True,
                 column_config=col_config if col_config else None)

    col1, _ = st.columns([1, 5])
    with col1:
        buf = io.StringIO()
        df.drop(columns=["Airflow"], errors="ignore").to_csv(buf, index=False)
        st.download_button("⬇ CSV", buf.getvalue(), f"{dag_id}_tasks.csv",
                           mime="text/csv", key=f"tg_csv_{id(raw_json)}")

    # ── Section 2: Dependency Diagram ─────────────────────────────────────────
    st.markdown("#### Dependency Diagram")

    if is_history:
        diagram = data.get("diagram", "")
        if diagram:
            st.code(diagram, language=None)
        return

    st.caption("🖱 Click any node to view details and fetch SQL.")
    try:
        from streamlit_flow import streamlit_flow
        from streamlit_flow.state import StreamlitFlowState
        from streamlit_flow.layouts import TreeLayout
    except ImportError:
        st.warning("streamlit-flow-component not installed — cannot render visual diagram.")
        diagram = data.get("diagram", "")
        if diagram:
            st.code(diagram, language=None)
        return

    state_key     = f"tg_flow_state__{dag_id}"
    component_key = f"tg_flow__{dag_id}"

    nodes, edges, content_map = _build_task_flow_graph(data)

    if not nodes:
        st.info("No tasks to display.")
        return

    if state_key not in st.session_state:
        st.session_state[state_key] = StreamlitFlowState(nodes, edges)

    new_state = streamlit_flow(
        key=component_key,
        state=st.session_state[state_key],
        height=520,
        fit_view=True,
        get_node_on_click=True,
        layout=TreeLayout(direction="right", node_node_spacing=100),
        show_minimap=True,
        show_controls=True,
        hide_watermark=True,
        min_zoom=0.25,
    )
    st.session_state[state_key] = new_state

    selected_id = new_state.selected_id
    if selected_id and selected_id in content_map:
        _render_task_content_panel(content_map[selected_id])
    else:
        st.caption("No node selected — click a node to explore.")


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
