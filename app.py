import base64
import json
import logging
import threading
from pathlib import Path

import streamlit as st

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

st.set_page_config(
    page_title="OSR Data Intelligence",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── CSS ──────────────────────────────────────────────────────────────────────

_css = (Path(__file__).parent / "static" / "app.css").read_text()
st.markdown(f"<style>{_css}</style>", unsafe_allow_html=True)


# ── Startup ingest (background) ───────────────────────────────────────────────

@st.cache_resource
def _excel_state() -> dict:
    state = {"loaded": 0, "skipped": 0, "done": False, "error": None}
    def _run():
        try:
            from tools.excel_tools import ingest_excel_files
            result = ingest_excel_files()
            state["loaded"] = result.get("loaded", 0)
            state["skipped"] = result.get("skipped", 0)
        except Exception as e:
            # Excel failure must not block the agent — Composer/BQ tools work independently
            state["error"] = str(e)
        finally:
            state["done"] = True
    threading.Thread(target=_run, daemon=True).start()
    return state

_excel_state()


# ── Session state ─────────────────────────────────────────────────────────────

if "show_runbook" not in st.session_state:
    st.session_state.show_runbook = False
if "messages" not in st.session_state:
    st.session_state.messages = []
if "agent" not in st.session_state:
    from agent.agent import build_agent
    st.session_state.agent = build_agent()
if "sql_bundle" not in st.session_state:
    st.session_state.sql_bundle = []


# ── Fixed header ──────────────────────────────────────────────────────────────

st.markdown("""
<div id="_app_header">
  <button id="_hbtn" title="Toggle sidebar">☰</button>
  <span class="_header-title">OSR</span>
  <span class="_header-sub">Data Intelligence Platform</span>
  <div class="_header-right">
    <button id="_runbook_btn" class="_header-action">📖 Run Book</button>
  </div>
</div>
""", unsafe_allow_html=True)


# ── JS: header button delegation to Streamlit ─────────────────────────────────

if "js_registered" not in st.session_state:
    st.session_state["js_registered"] = True
    _js_html = """
<script>
(function attach() {
  var pd = window.parent.document;
  if (!pd.body) { setTimeout(attach, 100); return; }

  function hideNativeBtn() {
    var btns = Array.from(pd.querySelectorAll('.stButton button'));
    var btn = btns.find(function(b) { return b.textContent.trim().includes('Run Book'); });
    if (btn) {
      var container = btn.closest('[data-testid="stElementContainer"]');
      if (container) container.style.display = 'none';
    }
  }
  hideNativeBtn();
  if (!pd.body.dataset.hideObserver) {
    pd.body.dataset.hideObserver = '1';
    new MutationObserver(hideNativeBtn).observe(pd.body, { childList: true, subtree: true });
  }

  if (pd.body.dataset.appListeners) return;
  pd.body.dataset.appListeners = '1';

  pd.body.addEventListener('click', function(e) {
    if (e.target.closest('#_hbtn')) {
      var closeBtn = pd.querySelector('[data-testid="stSidebarCollapseButton"] button')
                  || pd.querySelector('[data-testid="stSidebarCollapseButton"]');
      var openBtn  = pd.querySelector('[data-testid="collapsedControl"] button')
                  || pd.querySelector('[data-testid="collapsedControl"]')
                  || pd.querySelector('[data-testid="stExpandSidebarButton"]');
      var target = closeBtn || openBtn;
      if (target) target.click();
      return;
    }
    if (e.target.closest('#_runbook_btn')) {
      var rb = Array.from(pd.querySelectorAll('.stButton button')).find(function(b) { return b.innerText.includes('Run Book'); });
      if (rb) rb.click();
      return;
    }
  }, true);
})();
</script>
"""
    _js_src = "data:text/html;base64," + base64.b64encode(_js_html.encode()).decode()
    st.iframe(_js_src, height=1)


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div class="sidebar-logo">
      <div class="sidebar-logo-mark">OSR</div>
      <div class="sidebar-logo-sub">Data Intelligence Platform</div>
    </div>
    """, unsafe_allow_html=True)

    # Workspace context banner
    from core.workspace import get_pinned_workspace
    ws = get_pinned_workspace()
    if any(ws.values()):
        parts = [f"{k}: **{v}**" for k, v in ws.items() if v]
        st.markdown(
            f'<div class="workspace-banner">📌 {" · ".join(parts)}</div>',
            unsafe_allow_html=True,
        )

    # Glossary panel
    st.markdown('<div class="sidebar-section">📖 Glossary</div>', unsafe_allow_html=True)
    from core import persistence
    glossary = persistence.get_glossary()
    if glossary:
        g_search = st.text_input("Search glossary", key="g_search", placeholder="Search terms…", label_visibility="collapsed")
        filtered_glossary = {t: d for t, d in glossary.items() if not g_search or g_search.lower() in t.lower() or g_search.lower() in d.lower()}
        items_html = "".join(
            f'<div class="glossary-item"><span class="glossary-term">{t}</span>'
            f'<span class="glossary-def">{d}</span></div>'
            for t, d in filtered_glossary.items()
        )
        count_label = f"{len(filtered_glossary)} of {len(glossary)}" if g_search else str(len(glossary))
        st.markdown(
            f'<div class="glossary-count">{count_label} terms</div>'
            f'<div class="glossary-list">{items_html or "<div class=\'glossary-empty\'>No matches</div>"}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.caption("No glossary terms yet. Ask the agent to define a term.")

    with st.expander("＋ Add / Edit Term"):
        g_term = st.text_input("Term", key="g_term")
        g_defn = st.text_area("Definition", key="g_defn", height=68)
        if st.button("Save", key="save_term") and g_term and g_defn:
            persistence.update_glossary(g_term, g_defn)
            st.rerun()

    # Saved queries panel
    st.markdown('<div class="sidebar-section">💾 Saved Queries</div>', unsafe_allow_html=True)
    saved = persistence.get_saved_queries()
    if saved:
        sq_search = st.text_input("Search queries", key="sq_search", placeholder="Filter…")
        filtered = [q for q in saved if not sq_search or sq_search.lower() in q.get("name", "").lower()]
        for q in filtered[:8]:
            if st.button(f"💾 {q.get('name', 'Query')}", key=f"sq_{q.get('id', q.get('name'))}"):
                st.session_state.chat_prefill = q.get("sql", "")
    else:
        st.caption("No saved queries yet.")

    # Loaded tables
    _es = _excel_state()
    _label = "📊 Loaded Tables" if _es["done"] else "📊 Loaded Tables ⏳"
    st.markdown(f'<div class="sidebar-section">{_label}</div>', unsafe_allow_html=True)
    from core.duckdb_manager import get_manager
    db_tables = get_manager().list_tables()
    if db_tables:
        st.caption(f"{len(db_tables)} table(s) in DuckDB")
        for tbl in db_tables[:8]:
            st.caption(f"· {tbl}")
    elif not _es["done"]:
        st.caption("Indexing Excel files…")
    elif _es.get("error"):
        st.caption("Excel ingest skipped (no mapping files found).")
    else:
        st.caption("No Excel mapping files found. Composer & BigQuery tools are still available.")


# ── Hidden Streamlit action button (triggered via JS header button) ───────────
# CSS hides the rendered widget; JS clicks it via .st-key-runbook_btn selector.

if st.button("📖 Run Book", key="runbook_btn"):
    st.session_state.show_runbook = not st.session_state.show_runbook

_RUNBOOK_MD = (Path(__file__).parent / "runbook.md").read_text(encoding="utf-8")


# ── Main area ─────────────────────────────────────────────────────────────────

if st.session_state.show_runbook:
    with st.container(border=True):
        close_col, _ = st.columns([1, 11])
        with close_col:
            if st.button("✕ Close", key="close_runbook"):
                st.session_state.show_runbook = False
                st.rerun()
        st.markdown(_RUNBOOK_MD)

# Suggested prompts (only when chat is empty)
_SUGGESTED_PROMPTS = [
    "Show lineage for the RPS800 mapping file",
    "List all Composer environments and their DAGs",
    "Trace the RPS800 mapping file — show BQ table, DAGs, jobs, tasks and rendered SQL",
    "Compare deployed GCS code with Git for the dags/ folder",
    "Optimise the file dags/dag_rps800_load.py and download the result",
    "Show the execution diagram and task states for dag_rps800_load latest run",
]

if not st.session_state.messages:
    st.markdown(
        '<div style="text-align:center;padding:52px 0 20px;">'
        '<div style="font-size:36px;font-weight:800;color:#111827;letter-spacing:-0.02em;margin-bottom:10px;">'
        'OSR Data Intelligence</div>'
        '<div style="color:#6B7280;font-size:15px;max-width:520px;margin:0 auto;line-height:1.6;">'
        'Ask anything about your BigQuery tables, Composer DAGs, mapping sheets, schemas, or code.'
        '</div></div>',
        unsafe_allow_html=True,
    )
    cols = st.columns(3)
    for i, prompt in enumerate(_SUGGESTED_PROMPTS):
        with cols[i % 3]:
            if st.button(prompt, key=f"suggested_{i}", use_container_width=True):
                st.session_state.messages.append({"role": "user", "content": prompt})
                st.rerun()


# ── Renderer dispatcher ───────────────────────────────────────────────────────

import renderers.results_table as _rt
import renderers.diff_viewer as _dv
import renderers.validation_panel as _vp
import renderers.schema_tree as _st
import renderers.run_history_chart as _rhc
import renderers.performance_matrix as _pm
import renderers.reconciliation_panel as _rp
import renderers.optimised_file_viewer as _ofv
import renderers.lineage_graph as _lg


def dispatch_renderers(agent_output: dict) -> None:
    steps = agent_output.get("intermediate_steps", [])
    if not steps:
        return
    tools_called = {}
    for step in steps:
        try:
            tool_name = step[0].tool
            tool_output = step[1]
            tools_called[tool_name] = tool_output
        except Exception:
            continue

    if "trace_from_excel" in tools_called:
        _lg.render_lineage_graph(tools_called["trace_from_excel"])

    if "query_bigquery" in tools_called or "query_excel_data" in tools_called:
        source = "query_bigquery" if "query_bigquery" in tools_called else "query_excel_data"
        _rt.render(tools_called[source], agent=st.session_state.agent)

    if "optimise_sql" in tools_called:
        _dv.render(tools_called["optimise_sql"])

    if "validate_optimisation" in tools_called:
        _vp.render(tools_called["validate_optimisation"])

    if "introspect_bq_schema" in tools_called:
        _st.render(tools_called["introspect_bq_schema"])

    if "get_dag_run_history" in tools_called:
        _rhc.render(tools_called["get_dag_run_history"])

    if "list_airflow_jobs" in tools_called:
        _rhc.render(tools_called["list_airflow_jobs"])

    if "get_task_performance" in tools_called:
        _pm.render(tools_called["get_task_performance"])

    if "run_reconciliation" in tools_called:
        _rp.render(tools_called["run_reconciliation"])

    if "optimise_file" in tools_called:
        _ofv.render_optimised_file(tools_called["optimise_file"])

    if "optimise_folder" in tools_called:
        _ofv.render_optimised_folder(tools_called["optimise_folder"])

    if "compare_git_gcs" in tools_called:
        _ofv.render_git_gcs_diff(tools_called["compare_git_gcs"])


# ── Chat history ──────────────────────────────────────────────────────────────

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if "panels" in msg:
            dispatch_renderers(msg["panels"])


# ── Chat input ────────────────────────────────────────────────────────────────

# Handle suggested-prompt button clicks (prefill via session state)
_prefill = st.session_state.pop("chat_prefill", None)

# If a suggested prompt was clicked this run, process it as if typed
_send = _prefill

if prompt := st.chat_input(
    "Ask anything about your data, DAGs, schemas, or mappings…",
    key="main_chat_input",
):
    _send = prompt

if _send:
    st.session_state.messages.append({"role": "user", "content": _send})
    with st.chat_message("user"):
        st.markdown(_send)

    with st.chat_message("assistant"):
        with st.spinner("Thinking…"):
            from agent.agent import run_agent
            result = run_agent(st.session_state.agent, _send)
        st.markdown(result.get("output", ""))
        dispatch_renderers(result)

    st.session_state.messages.append({
        "role": "assistant",
        "content": result.get("output", ""),
        "panels": result,
    })
