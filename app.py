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

from agent.system_prompt import _PROMPT_VERSION as _AGENT_PROMPT_VER

if "show_runbook" not in st.session_state:
    st.session_state.show_runbook = False
if "messages" not in st.session_state:
    st.session_state.messages = []
if "agent" not in st.session_state or st.session_state.get("_agent_prompt_ver") != _AGENT_PROMPT_VER:
    from agent.agent import build_agent
    st.session_state.agent = build_agent()
    st.session_state._agent_prompt_ver = _AGENT_PROMPT_VER
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
  if (pd.getElementById('_app_events_script')) return;

  var script = pd.createElement('script');
  script.id = '_app_events_script';
  script.text = `
    function hideNativeBtn() {
      var container = document.querySelector('.st-key-runbook_btn');
      if (!container) {
        var btns = Array.from(document.querySelectorAll('button'));
        var btn = btns.find(function(b) { return b.textContent && b.textContent.includes('Run Book') && b.id !== '_runbook_btn'; });
        if (btn) container = btn.closest('[data-testid="stElementContainer"]');
      }
      if (container && container.style.opacity !== '0') {
        container.style.position = 'absolute';
        container.style.width = '0px';
        container.style.height = '0px';
        container.style.overflow = 'hidden';
        container.style.opacity = '0';
      }
    }
    hideNativeBtn();
    if (!document.body.dataset.hideObserver) {
      document.body.dataset.hideObserver = '1';
      new MutationObserver(hideNativeBtn).observe(document.body, { childList: true, subtree: true });
    }

    document.body.addEventListener('click', function(e) {
      if (e.target.closest('#_hbtn')) {
        e.preventDefault();
        e.stopPropagation();
        var closeBtn = document.querySelector('[data-testid="stSidebarCollapseButton"] button') || document.querySelector('[data-testid="stSidebarCollapseButton"]');
        var openBtn  = document.querySelector('[data-testid="collapsedControl"] button') || document.querySelector('[data-testid="collapsedControl"]') || document.querySelector('[data-testid="stSidebarExpandButton"] button') || document.querySelector('[data-testid="stSidebarExpandButton"]') || document.querySelector('[data-testid="stExpandSidebarButton"]');
        var target = closeBtn || openBtn;
        if (target) target.click();
        return;
      }
      if (e.target.closest('#_runbook_btn')) {
        e.preventDefault();
        e.stopPropagation();
        var rb = document.querySelector('.st-key-runbook_btn button');
        if (!rb) {
          var btns = Array.from(document.querySelectorAll('button'));
          rb = btns.find(function(b) { return b.textContent && b.textContent.includes('Run Book') && b.id !== '_runbook_btn'; });
        }
        if (rb) rb.click();
        return;
      }
    }, true);
  `;
  pd.head.appendChild(script);
})();
</script>
"""
    import streamlit.components.v1 as components
    components.html(_js_html, height=0, width=0)


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
            if st.button(prompt, key=f"suggested_{i}", width="stretch"):
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
import renderers.schema_audit_panel as _sap
import renderers.file_browser as _fb
import renderers.mapping_validation_panel as _mvp


def dispatch_renderers(agent_output: dict, is_history: bool = False) -> None:
    steps = agent_output.get("intermediate_steps", [])
    if not steps:
        return
    tools_called: dict[str, str] = {}
    # validate_mapping_rules may be called once per file in a batch — collect all calls
    validate_mapping_calls: list[str] = []
    for step in steps:
        try:
            tool_name   = step[0].tool
            tool_output = step[1]
            tools_called[tool_name] = tool_output   # last-call-wins for single-call tools
            if tool_name == "validate_mapping_rules":
                validate_mapping_calls.append(tool_output)
        except Exception:
            continue

    has_lineage = "trace_from_excel" in tools_called

    if has_lineage:
        _lg.render_lineage_graph(tools_called["trace_from_excel"], is_history=is_history)

    if "list_dags" in tools_called:
        _rt.render_dag_list(tools_called["list_dags"])

    if "get_task_sql" in tools_called:
        _rt.render_task_sql(tools_called["get_task_sql"])

    if "get_dag_rendered_files" in tools_called:
        _rt.render_dag_rendered_files(tools_called["get_dag_rendered_files"])

    # Suppress task graph / DAG details when the lineage graph already covers them
    if "get_dag_task_graph" in tools_called and not has_lineage:
        _rt.render_dag_task_graph(tools_called["get_dag_task_graph"], is_history=is_history)

    if "get_dag_details" in tools_called and not has_lineage:
        _rt.render_dag_details(tools_called["get_dag_details"])

    if "query_bigquery" in tools_called or "query_excel_data" in tools_called:
        source = "query_bigquery" if "query_bigquery" in tools_called else "query_excel_data"
        _rt.render(tools_called[source], agent=st.session_state.agent)

    if "optimise_sql" in tools_called:
        _dv.render(tools_called["optimise_sql"])

    if "optimise_sql_file" in tools_called:
        _dv.render(tools_called["optimise_sql_file"])

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

    if "optimise_dag" in tools_called:
        _ofv.render_dag_suggestions(tools_called["optimise_dag"])

    if "optimise_file" in tools_called:
        _ofv.render_optimised_file(tools_called["optimise_file"])

    if "optimise_folder" in tools_called:
        _ofv.render_optimised_folder(tools_called["optimise_folder"])

    _has_optimization = any(
        k in tools_called
        for k in ("optimise_sql", "optimise_sql_file", "optimise_file", "optimise_folder")
    )
    if "read_file" in tools_called and not _has_optimization:
        _ofv.render_file_content(tools_called["read_file"])

    if "compare_git_gcs" in tools_called:
        _ofv.render_git_gcs_diff(tools_called["compare_git_gcs"])

    if "run_schema_audit" in tools_called:
        _sap.render_schema_audit(tools_called["run_schema_audit"])

    if "browse_gcs" in tools_called:
        _fb.render_file_browser(tools_called["browse_gcs"])

    if "browse_git" in tools_called:
        _fb.render_file_browser(tools_called["browse_git"])

    # Single-file validate_mapping_rules (agent-driven, non-batch)
    _has_export = "export_mapping_results" in tools_called
    _batch_mode = len(validate_mapping_calls) > 1 or _has_export
    for _vm_output in validate_mapping_calls:
        _mvp.render_mapping_validation(_vm_output, compact=_batch_mode)

    if "validate_mapping_folder" in tools_called:
        _mvp.render_mapping_validation(tools_called["validate_mapping_folder"])

    if _has_export:
        _mvp.render_export_result(tools_called["export_mapping_results"])

    # Batch trigger: agent called discover_mapping_files → hand off to Streamlit loop
    if "discover_mapping_files" in tools_called and not is_history:
        import json as _json
        try:
            _disc = _json.loads(tools_called["discover_mapping_files"])
            if _disc.get("files"):
                st.session_state["_batch_pending"] = _disc
        except Exception:
            pass


# ── Batch validation loop (Streamlit-controlled, real-time progress) ─────────

import re as _re


def _is_batch_request(msg: str) -> bool:
    """Return True when the message is asking to validate a folder / all files.

    Rules (all must hold):
    - mentions validation or mapping or excel context
    - mentions a multi-file indicator (folder / all / batch / multiple)
    - does NOT name a specific .xlsx file (which would be a single-file request)
    """
    m = msg.lower()
    has_context = bool(_re.search(r"validat|mapping|excel", m))
    has_multi   = bool(_re.search(
        r"\bfolder\b|\ball\b|\bbatch\b|\bmultiple\b|\ball\s+files?\b"
        r"|\ball\s+excel\b|\ball\s+mapping\b|\bthe\s+folder\b",
        m,
    ))
    has_specific_file = bool(_re.search(r"\w+\.xlsx", m))
    return has_context and has_multi and not has_specific_file


def _discover_files_for_batch(user_message: str) -> dict | None:
    """One-shot LLM call: extract discover_mapping_files params, call the tool, return result.

    Bypasses the full ReAct loop so the UI isn't blocked while the agent loops through files.
    """
    import json
    from langchain_core.messages import SystemMessage, HumanMessage
    from agent.agent import _llm, _llm_credentials
    from tools.mapping_validation_tools import discover_mapping_files

    llm = _llm().bind_tools([discover_mapping_files])
    response = llm.invoke([
        SystemMessage(content=(
            "Extract folder/path and validation source parameters from the user's message "
            "and call discover_mapping_files() exactly once.\n"
            "source_mode must be 'local', 'git', or 'composer' — infer from context "
            "(default 'local' when no source is mentioned).\n"
            "Set composer_env, local_dag_path, git_ref as appropriate.\n"
            "Return only the tool call — no text."
        )),
        HumanMessage(content=user_message),
    ])
    if not response.tool_calls:
        return None
    try:
        raw = discover_mapping_files.invoke(response.tool_calls[0]["args"])
        return json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return None


_BATCH_KEEP = {"mapping_file", "summary", "dag_id", "source_mode",
               "composer_env", "sql_fetch_error", "error", "hint"}


def _slim_result(res: dict) -> dict:
    """Keep only scorecard-level fields for session state storage.

    bq_table_groups (potentially thousands of rule widgets) is dropped entirely —
    the batch consolidated view only needs per-file summary metrics. The Excel
    export (generated before slimming) retains all rule-level detail.
    """
    if not isinstance(res, dict):
        return res
    return {k: v for k, v in res.items() if k in _BATCH_KEEP}


def _run_batch_validation(batch: dict) -> dict:
    """Run per-file validation with real-time st.status() progress, then show consolidated view."""
    import json
    from pathlib import Path

    files          = batch.get("files", [])
    source_mode    = batch.get("source_mode", "local")
    composer_env   = batch.get("composer_env")
    local_dag_path = batch.get("local_dag_path")
    git_repo_path  = batch.get("git_repo_path")
    git_ref        = batch.get("git_ref")
    env_label      = batch.get("env_label", "local")
    warnings       = batch.get("warnings", [])

    from tools.mapping_validation_tools import _do_validate_mapping, _result_cache
    from tools.excel_tools import export_validation_excel, ingest_excel_files
    from core import config as _cfg

    for w in warnings:
        st.warning(w)

    # Re-ingest to guarantee all staged files are in the registry before the loop.
    # discover_mapping_files() already ingested them, but a silent failure there would
    # cause every _do_validate_mapping call to return an error dict immediately.
    try:
        ingest_excel_files()
    except Exception as _ie:
        st.warning(f"Pre-validation ingest warning: {_ie}")

    # Clear the full result cache so the latest SQL file discovery logic runs
    # on every batch — stale entries would silently return old sql_file values.
    _result_cache.clear()

    running = {
        "pass": 0, "fail": 0, "partial": 0,
        "not_applicable": 0, "not_evaluated": 0, "error": 0, "total": 0,
    }
    validated: list[dict] = []

    # Consolidated scorecard — updated after every file
    score_placeholder = st.empty()

    def _refresh_scorecards() -> None:
        with score_placeholder.container():
            st.markdown("**📊 Consolidated Validation Status**")
            c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
            c1.metric("🟢 PASS",     running["pass"])
            c2.metric("🔴 FAIL",     running["fail"])
            c3.metric("🟡 PARTIAL",  running["partial"])
            c4.metric("⚪ N/A",      running["not_applicable"])
            c5.metric("🔵 Not Eval", running["not_evaluated"])
            c6.metric("⚠️ Error",    running["error"])
            c7.metric("Total",       running["total"])

    _refresh_scorecards()

    file_errors: list[str] = []  # accumulated outside status so they survive collapse

    with st.status(f"Validating {len(files)} file(s)…", expanded=True) as _status:
        for idx, file_info in enumerate(files):
            file_name = file_info.get("file_name", "")
            dag_id    = file_info.get("dag_id")

            _status.update(label=f"⏳ Processing {file_name}  ({idx + 1}/{len(files)})…")
            st.markdown(
                f'<div style="background:#EFF6FF;border-left:4px solid #1D4ED8;'
                f'padding:8px 14px;border-radius:4px;margin:4px 0 8px;">'
                f'<b>⏳ Processing: {file_name}</b></div>',
                unsafe_allow_html=True,
            )

            result = _do_validate_mapping(
                file_name, composer_env, dag_id,
                None, None, False,
                source_mode, local_dag_path, git_repo_path, git_ref,
            )
            validated.append(result)

            if result.get("error"):
                err_msg  = result["error"]
                hint     = result.get("hint", "")
                avail    = result.get("available_columns")
                detail   = f" — {hint}" if hint else ""
                detail  += f" | columns: {avail}" if avail else ""
                full_err = f"{file_name}: {err_msg}{detail}"
                st.warning(f"⚠️ {full_err}")
                file_errors.append(full_err)
                running["error"] += 1
                # do NOT add to running["total"] — total counts rules, not files
                _refresh_scorecards()
                continue

            s = result.get("summary") or {}
            for k in running:
                running[k] += s.get(k, 0)

            _refresh_scorecards()

            st.markdown(
                f'<div style="background:#F0FFF4;border-left:4px solid #1B8A3E;'
                f'padding:8px 14px;border-radius:4px;margin:4px 0 8px;">'
                f'<b>✅ {file_name}</b> — '
                f'{s.get("total",0)} rules: '
                f'{s.get("pass",0)} PASS · {s.get("fail",0)} FAIL · '
                f'{s.get("partial",0)} PARTIAL · {s.get("not_applicable",0)} N/A · '
                f'{s.get("not_evaluated",0)} not eval</div>',
                unsafe_allow_html=True,
            )

        good = len(files) - running["error"]
        _status.update(
            label=f"✅ Validated {good}/{len(files)} file(s)" +
                  (f" — {running['error']} error(s)" if running["error"] else ""),
            state="complete",
            expanded=False,
        )

    # Show file errors OUTSIDE the collapsible status so they stay visible after collapse.
    if file_errors:
        with st.expander(f"⚠️ {len(file_errors)} file(s) failed — click to see errors", expanded=True):
            for e in file_errors:
                st.warning(e)

    # Generate Excel and build the export payload for render_export_result
    export_payload: dict = {
        "is_export":       True,
        "files_exported":  len(validated),
        "overall_summary": running,
        "results":         [_slim_result(r) for r in validated],
        "file_errors":     file_errors,  # persists through st.rerun() via session_state
    }
    try:
        out = export_validation_excel(validated, env_label, Path(_cfg.EXPORTS_ROOT))
        export_payload["export_path"] = str(out)
        export_payload["file_name"]   = out.name
    except Exception as exc:
        st.warning(f"Excel export failed: {exc}")

    return export_payload


# ── Chat input & state ────────────────────────────────────────────────────────

if "_pending_input" not in st.session_state:
    st.session_state._pending_input = None

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
    st.session_state._pending_input = _send

_active_prompt = st.session_state._pending_input


# ── Chat history ──────────────────────────────────────────────────────────────

# Find the index of the last assistant message so its panels stay interactive.
# All earlier assistant messages use the static summary (no streamlit-flow component)
# which prevents multiple flow instances from triggering competing reruns.
_last_assistant_idx = max(
    (i for i, m in enumerate(st.session_state.messages) if m["role"] == "assistant"),
    default=-1,
)

for _i, msg in enumerate(st.session_state.messages):
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if "panels" in msg:
            is_hist = (_i != _last_assistant_idx) or bool(_active_prompt)
            dispatch_renderers(msg["panels"], is_history=is_hist)
        if "batch_result" in msg:
            _mvp.render_export_result(msg["batch_result"])


# ── Process active prompt ─────────────────────────────────────────────────────

if _active_prompt:
    st.session_state.messages.append({"role": "user", "content": _active_prompt})
    with st.chat_message("user"):
        st.markdown(_active_prompt)

    if _is_batch_request(_active_prompt):
        # ── Batch path: one-shot param extraction → Streamlit-owned loop ──────
        # Phase 1: discovery — runs inside a chat bubble so the spinner is visible
        with st.chat_message("assistant"):
            with st.spinner("Finding mapping files…"):
                _disc = _discover_files_for_batch(_active_prompt)

            if _disc is None:
                # LLM couldn't extract params — fall back to the normal agent path
                with st.spinner("Thinking…"):
                    from agent.agent import run_agent
                    result = run_agent(st.session_state.agent, _active_prompt)
                st.markdown(result.get("output", ""))
                dispatch_renderers(result)
                _reply = result.get("output", "")
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": _reply,
                    "panels": result,
                })
                st.session_state._pending_input = None
                st.rerun()
            elif _disc.get("error"):
                _err = _disc["error"]
                st.error(_err)
                _reply = _err
                st.session_state.messages.append({"role": "assistant", "content": _reply})
            elif not _disc.get("files"):
                st.warning("No .xlsx mapping files found in the specified location.")
                _reply = "No mapping files found."
                st.session_state.messages.append({"role": "assistant", "content": _reply})
            else:
                n = _disc["total"]
                _reply = f"Found {n} file(s) — starting validation now."
                st.markdown(_reply)
                st.session_state.messages.append({"role": "assistant", "content": _reply})

        # Phase 2: per-file validation loop — rendered at TOP LEVEL so st.status()
        # and st.empty() update the browser in real time (chat containers batch renders).
        if _disc and not _disc.get("error") and _disc.get("files"):
            _export = _run_batch_validation(_disc)
            # Attach export payload to the last assistant message so the
            # consolidated result survives st.rerun() and renders from history.
            if _export and st.session_state.messages:
                st.session_state.messages[-1]["batch_result"] = _export

    else:
        # ── Normal agent path ─────────────────────────────────────────────────
        with st.chat_message("assistant"):
            with st.spinner("Thinking…"):
                from agent.agent import run_agent
                result = run_agent(st.session_state.agent, _active_prompt)
            st.markdown(result.get("output", ""))
            dispatch_renderers(result)

        st.session_state.messages.append({
            "role": "assistant",
            "content": result.get("output", ""),
            "panels": result,
        })

    st.session_state._pending_input = None
    st.rerun()
