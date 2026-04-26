"""Interactive lineage graph renderer using streamlit-flow (ReactFlow wrapper).

Visualises: Excel file → DAG(s) → Tasks → SQL nodes.
Click any node to see its rendered content / execution details.
"""
import json
from datetime import datetime, timezone

import streamlit as st
import streamlit.components.v1 as components

from core import monaco

# ── Colour / icon maps ────────────────────────────────────────────────────────

_STATE_COLOR = {
    "success":      "#10B981",
    "failed":       "#EF4444",
    "running":      "#3B82F6",
    "queued":       "#F59E0B",
    "up_for_retry": "#F97316",
    "skipped":      "#9CA3AF",
}

_STATE_ICON = {
    "success":      "✅",
    "failed":       "❌",
    "running":      "🔄",
    "queued":       "⏳",
    "up_for_retry": "🔁",
    "skipped":      "⏭",
}

_NODE_COLORS = {
    "excel": "#0EA5E9",   # sky-blue
    "dag":   "#6366F1",   # indigo (overridden by run-state color)
    "task":  "#8B5CF6",   # violet (overridden by run-state color)
    "sql":   "#F59E0B",   # amber
}


# ── Formatting helpers ────────────────────────────────────────────────────────

def _sql_height(sql: str) -> int:
    lines = sql.count("\n") + 1
    return min(max(300, lines * 22 + 60), 900)


def _fmt_dur(secs) -> str:
    if secs is None:
        return ""
    s = int(secs)
    return f"{s // 60}m {s % 60}s" if s >= 60 else f"{s}s"


def _fmt_ago(iso_str: str) -> str:
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        mins = int((datetime.now(timezone.utc) - dt).total_seconds() / 60)
        if mins < 1:
            return "just now"
        if mins < 60:
            return f"{mins}m ago"
        h = mins // 60
        return f"{h}h ago" if h < 24 else f"{h // 24}d ago"
    except Exception:
        return iso_str[:10]


# ── Node style factory ────────────────────────────────────────────────────────

def _style(bg: str, min_w: str = "160px", font_size: str = "12px") -> dict:
    return {
        "background":    bg,
        "color":         "white",
        "border":        "2px solid rgba(255,255,255,0.18)",
        "borderRadius":  "10px",
        "padding":       "10px 14px",
        "fontSize":      font_size,
        "fontWeight":    "600",
        "minWidth":      min_w,
        "textAlign":     "center",
        "whiteSpace":    "pre-wrap",
        "lineHeight":    "1.55",
        "boxShadow":     "0 2px 8px rgba(0,0,0,0.25)",
    }


# ── Graph builder ─────────────────────────────────────────────────────────────

def _build_graph(data: dict):
    from streamlit_flow.elements import StreamlitFlowNode, StreamlitFlowEdge

    nodes: list = []
    edges: list = []
    content_map: dict = {}

    excel_file   = data.get("excel_file", "unknown.xlsx")
    bq_table     = data.get("bq_table", "")
    dag_names    = data.get("dag_names", [])
    dag_details  = data.get("dag_details", [])
    table_name   = data.get("table_name", excel_file)
    composer_env = data.get("composer_env", "")

    dag_detail_map = {d["dag_id"]: d for d in dag_details}

    # ── Excel node ────────────────────────────────────────────────────────────
    excel_id = f"excel_{table_name}"
    label = f"📊  {excel_file}"
    if bq_table:
        label += f"\n→ {bq_table}"

    nodes.append(StreamlitFlowNode(
        id=excel_id, pos=(0, 0),
        data={"label": label},
        node_type="input",
        source_position="right", target_position="left",
        selectable=True,
        style=_style(_NODE_COLORS["excel"], min_w="180px"),
    ))
    content_map[excel_id] = {
        "type":          "excel",
        "file_name":     excel_file,
        "table_name":    table_name,
        "bq_table":      bq_table,
        "dag_names":     dag_names,
        "source_folder": data.get("source_folder", ""),
    }

    # ── DAG nodes ─────────────────────────────────────────────────────────────
    for dag_id in dag_names:
        dag_node_id = f"dag_{dag_id}"
        dag_info    = dag_detail_map.get(dag_id, {})
        recent_jobs = dag_info.get("recent_jobs", [])
        last_job    = recent_jobs[0] if recent_jobs else None
        last_state  = last_job.get("state") if last_job else None
        last_time   = _fmt_ago(last_job.get("start_time")) if last_job else ""
        last_dur    = _fmt_dur(last_job.get("duration_seconds")) if last_job else ""

        label = f"⚙  {dag_id}"
        if last_state:
            badge = f"{_STATE_ICON.get(last_state, '⬜')} {last_state}"
            if last_time:
                badge += f"  ·  {last_time}"
            label += f"\n{badge}"
        if last_dur:
            label += f"\n⏱ {last_dur}"

        dag_bg = _STATE_COLOR.get(last_state, _NODE_COLORS["dag"])
        nodes.append(StreamlitFlowNode(
            id=dag_node_id, pos=(0, 0),
            data={"label": label},
            node_type="default",
            source_position="right", target_position="left",
            selectable=True,
            style=_style(dag_bg, min_w="200px"),
        ))
        edges.append(StreamlitFlowEdge(
            id=f"e_{excel_id}__{dag_node_id}",
            source=excel_id, target=dag_node_id,
            edge_type="smoothstep", animated=True,
        ))
        content_map[dag_node_id] = {
            "type":         "dag",
            "dag_id":       dag_id,
            "recent_jobs":  recent_jobs,
            "error":        dag_info.get("error"),
            "composer_env": composer_env,
        }

        # ── Task nodes ────────────────────────────────────────────────────────
        tasks = dag_info.get("tasks", [])
        sql_by_task = {
            s["task_id"]: s["rendered_sql"]
            for s in dag_info.get("rendered_sqls", [])
        }

        # Identify root tasks (not downstream of any other task in this DAG)
        all_downstream: set = set()
        for t in tasks:
            for d in t.get("depends_on", []):   # depends_on = downstream_task_ids in Airflow
                all_downstream.add(d)
        root_task_ids = {t["task_id"] for t in tasks if t["task_id"] not in all_downstream}

        for task in tasks:
            task_id      = task["task_id"]
            task_nid     = f"task_{dag_id}__{task_id}"
            has_sql      = task_id in sql_by_task
            task_state   = task.get("state")
            op_short     = (
                (task.get("operator") or "")
                .replace("Operator", "")
                .replace("BigQuery", "BQ")
                .strip()
            )
            dur = _fmt_dur(task.get("duration_seconds"))

            label = f"{'🗄' if has_sql else '📋'}  {task_id}"
            if op_short:
                label += f"\n{op_short}"
            if task_state:
                badge = f"{_STATE_ICON.get(task_state, '⬜')} {task_state}"
                if dur:
                    badge += f"  ·  {dur}"
                label += f"\n{badge}"

            task_bg = _STATE_COLOR.get(task_state, _NODE_COLORS["task"])
            has_children = has_sql or bool(task.get("depends_on"))
            nodes.append(StreamlitFlowNode(
                id=task_nid, pos=(0, 0),
                data={"label": label},
                node_type="default" if has_children else "output",
                source_position="right", target_position="left",
                selectable=True,
                style=_style(task_bg, min_w="155px", font_size="11px"),
            ))

            # Root tasks connect from DAG node
            if task_id in root_task_ids:
                edges.append(StreamlitFlowEdge(
                    id=f"e_{dag_node_id}__{task_nid}",
                    source=dag_node_id, target=task_nid,
                    edge_type="smoothstep",
                ))

            # Each task connects to its downstream tasks
            for ds_id in task.get("depends_on", []):
                ds_nid = f"task_{dag_id}__{ds_id}"
                edges.append(StreamlitFlowEdge(
                    id=f"e_{task_nid}__{ds_nid}",
                    source=task_nid, target=ds_nid,
                    edge_type="smoothstep",
                ))

            content_map[task_nid] = {
                "type":             "task",
                "dag_id":           dag_id,
                "task_id":          task_id,
                "composer_env":     composer_env,
                "operator":         task.get("operator", ""),
                "state":            task_state,
                "duration_seconds": task.get("duration_seconds"),
                "has_sql":          has_sql,
                "sql":              sql_by_task.get(task_id),
            }

            # ── SQL node ──────────────────────────────────────────────────────
            if has_sql:
                sql_nid     = f"sql_{dag_id}__{task_id}"
                sql_preview = (sql_by_task[task_id] or "")[:40].replace("\n", " ").strip()
                nodes.append(StreamlitFlowNode(
                    id=sql_nid, pos=(0, 0),
                    data={"label": f"📝  {task_id}.sql\n{sql_preview}…"},
                    node_type="output",
                    source_position="right", target_position="left",
                    selectable=True,
                    style=_style(_NODE_COLORS["sql"], font_size="11px"),
                ))
                edges.append(StreamlitFlowEdge(
                    id=f"e_{task_nid}__{sql_nid}",
                    source=task_nid, target=sql_nid,
                    edge_type="smoothstep",
                ))
                content_map[sql_nid] = {
                    "type":    "sql",
                    "dag_id":  dag_id,
                    "task_id": task_id,
                    "sql":     sql_by_task[task_id],
                }

    return nodes, edges, content_map


# ── Content panel ─────────────────────────────────────────────────────────────

def _render_content_panel(info: dict) -> None:
    node_type = info["type"]
    st.divider()

    if node_type == "excel":
        st.markdown(f"#### 📊 {info['file_name']}")
        c1, c2 = st.columns(2)
        c1.markdown(f"**DuckDB table:** `{info['table_name']}`")
        c1.markdown(f"**Source folder:** `{info.get('source_folder') or '—'}`")
        c2.markdown(f"**BigQuery table:** `{info['bq_table'] or '—'}`")
        c2.markdown(f"**DAGs:** {', '.join(info['dag_names']) or '—'}")

        table_name = info.get("table_name")
        if table_name:
            with st.expander("📄 File Contents", expanded=True):
                try:
                    from core.duckdb_manager import get_manager
                    import pandas as pd
                    df = get_manager().execute(f'SELECT * FROM "{table_name}" LIMIT 500')
                    st.caption(f"{len(df):,} rows (limit 500)")
                    st.dataframe(df, hide_index=True, use_container_width=True)
                except Exception as exc:
                    st.info(f"Could not load file contents — table may not be loaded yet: {exc}")

    elif node_type == "dag":
        dag_id       = info["dag_id"]
        composer_env = info.get("composer_env", "")
        st.markdown(f"#### ⚙ {dag_id}")
        if info.get("error"):
            st.warning(f"Composer error: {info['error']}")
        jobs = info.get("recent_jobs", [])
        if jobs:
            last = jobs[0]
            c1, c2, c3 = st.columns(3)
            c1.metric("Last State", last.get("state", "—"))
            c2.metric("Last Run",   _fmt_ago(last.get("start_time")))
            c3.metric("Duration",   _fmt_dur(last.get("duration_seconds")) or "—")
            st.markdown("**Recent runs:**")
            import pandas as pd
            rows = [
                {
                    "Run ID":   j.get("run_id", ""),
                    "State":    j.get("state", ""),
                    "Start":    (j.get("start_time") or "")[:19].replace("T", " "),
                    "Duration": _fmt_dur(j.get("duration_seconds")),
                }
                for j in jobs
            ]
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
        else:
            st.info("No recent job runs found for this DAG.")

        src_key = f"lg_dag_src__{dag_id}"
        if src_key not in st.session_state:
            with st.spinner("Loading DAG source…"):
                try:
                    from tools.composer_tools import _fetch_dag_source
                    src = _fetch_dag_source(dag_id, composer_env or None)
                    st.session_state[src_key] = src or ""
                except Exception as exc:
                    st.session_state[src_key] = f"# Error fetching source: {exc}"
        source = st.session_state.get(src_key, "")
        if source and not source.startswith("# Error"):
            with st.expander("🐍 DAG Source", expanded=True):
                h = min(max(300, source.count("\n") * 18 + 40), 800)
                components.html(monaco.editor(source, language="python", height=h), height=h + 20)
                st.download_button(
                    "⬇ Download DAG source",
                    data=source.encode("utf-8"),
                    file_name=f"{dag_id}.py",
                    mime="text/plain",
                    key=f"dl_lg_dagsrc_{dag_id}",
                )
        elif source.startswith("# Error"):
            st.warning(source)
        else:
            st.info("DAG source not found (checked Airflow API, GCS, and Git).")

    elif node_type == "task":
        has_sql = info.get("has_sql")
        st.markdown(f"#### {'🗄' if has_sql else '📋'}  {info['task_id']}")
        c1, c2, c3 = st.columns(3)
        c1.markdown(f"**DAG:** `{info['dag_id']}`")
        c2.markdown(f"**Operator:** `{info.get('operator') or '—'}`")
        state = info.get("state")
        state_badge = f"{_STATE_ICON.get(state, '⬜')} `{state or '—'}`"
        c3.markdown(f"**State:** {state_badge}")
        if info.get("duration_seconds"):
            st.caption(f"Duration: {_fmt_dur(info['duration_seconds'])}")
        if has_sql and info.get("sql"):
            st.markdown("**Rendered SQL:**")
            h = _sql_height(info["sql"])
            components.html(monaco.editor(info["sql"], language="sql", height=h), height=h + 20)
            st.download_button(
                "⬇ Download SQL",
                data=info["sql"].encode("utf-8"),
                file_name=f"{info['dag_id']}__{info['task_id']}.sql",
                mime="text/plain",
                key=f"dl_lg_task_{info['dag_id']}__{info['task_id']}",
            )
        else:
            st.info("SQL not found in trace — click below to fetch it on demand.")
            env  = info.get("composer_env", "")
            did  = info["dag_id"]
            tid  = info["task_id"]
            if env and st.button("⬇ Fetch SQL", key=f"fetch_sql_{did}__{tid}"):
                st.session_state.chat_prefill = (
                    f"get task sql for {tid} in dag {did} in {env}"
                )
                st.rerun()

    elif node_type == "sql":
        st.markdown(f"#### 📝 {info['task_id']}.sql")
        st.caption(f"DAG: {info['dag_id']}")
        if info.get("sql"):
            h = _sql_height(info["sql"])
            components.html(monaco.editor(info["sql"], language="sql", height=h), height=h + 20)
            st.download_button(
                "⬇ Download SQL",
                data=info["sql"].encode("utf-8"),
                file_name=f"{info['dag_id']}__{info['task_id']}.sql",
                mime="text/plain",
                key=f"dl_lg_sql_{info['dag_id']}__{info['task_id']}",
            )
        else:
            st.info("No rendered SQL available.")


# ── Public entry point ────────────────────────────────────────────────────────

def render_lineage_graph(raw_json: str) -> None:
    """Render interactive lineage graph from trace_from_excel tool output."""
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse lineage data.")
        return

    status = data.get("status")
    if status in ("no_excel", "not_found"):
        st.warning(data.get("note", "No lineage data available."))
        return
    if "error" in data:
        st.error(f"Lineage error: {data['error']}")
        return

    try:
        from streamlit_flow import streamlit_flow
        from streamlit_flow.state import StreamlitFlowState
        from streamlit_flow.layouts import TreeLayout
    except ImportError:
        st.error("streamlit-flow-component not installed. Run: pip install streamlit-flow-component")
        return

    excel_file = data.get("excel_file", "unknown.xlsx")
    table_name = data.get("table_name", excel_file)
    state_key     = f"lineage_state__{table_name}"
    component_key = f"lineage_flow__{table_name}"

    nodes, edges, content_map = _build_graph(data)

    if state_key not in st.session_state:
        st.session_state[state_key] = StreamlitFlowState(nodes, edges)

    st.subheader(f"Lineage: {excel_file}")
    st.caption("🖱 Click any node to view rendered content and execution details.")

    new_state = streamlit_flow(
        key=component_key,
        state=st.session_state[state_key],
        height=560,
        fit_view=True,
        get_node_on_click=True,
        layout=TreeLayout(direction="right", node_node_spacing=110),
        show_minimap=True,
        show_controls=True,
        hide_watermark=True,
        min_zoom=0.25,
    )
    st.session_state[state_key] = new_state

    selected_id = new_state.selected_id
    if selected_id and selected_id in content_map:
        _render_content_panel(content_map[selected_id])
    else:
        st.caption("No node selected — click a node to explore.")
