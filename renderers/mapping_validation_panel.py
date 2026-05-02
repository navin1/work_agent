"""Mapping validation panel — traceability matrix of business rules vs SQL implementation."""

import json

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from core import monaco

_VERDICT = {
    "PASS":           ("🟢", "#1B8A3E", "#F0FFF4"),
    "FAIL":           ("🔴", "#C41230", "#FFF0F0"),
    "PARTIAL":        ("🟡", "#B38600", "#FFFBEB"),
    "NOT_APPLICABLE": ("⚪", "#6B7280", "#F9FAFB"),
    "NOT_EVALUATED":  ("🔵", "#1D4ED8", "#EFF6FF"),
    "ERROR":          ("⚠️", "#9333EA", "#FAF5FF"),
}
_CONFIDENCE = {
    "HIGH":   ("HIGH",   "#1B8A3E"),
    "MEDIUM": ("MEDIUM", "#B38600"),
    "LOW":    ("LOW ⚠️", "#C41230"),
}
_render_count = 0


# ── Shared primitives ─────────────────────────────────────────────────────────

def _render_banner(file_name: str, completed: bool) -> None:
    if completed:
        icon, label, bg, border = "✅", "Completed", "#F0FFF4", "#1B8A3E"
    else:
        icon, label, bg, border = "⏳", "Processing", "#EFF6FF", "#1D4ED8"
    st.markdown(
        f'<div style="background:{bg};border-left:4px solid {border};'
        f'padding:10px 16px;border-radius:4px;margin:10px 0 6px;">'
        f'<span style="font-weight:700;font-size:15px;">'
        f'📋 {label}: {file_name}</span></div>',
        unsafe_allow_html=True,
    )


def _render_scorecards(summary: dict) -> None:
    summary = summary or {}
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("🟢 PASS",     summary.get("pass",           0))
    c2.metric("🔴 FAIL",     summary.get("fail",           0))
    c3.metric("🟡 PARTIAL",  summary.get("partial",        0))
    c4.metric("⚪ N/A",      summary.get("not_applicable", 0))
    c5.metric("🔵 Not Eval", summary.get("not_evaluated",  0))
    c6.metric("Total",       summary.get("total",          0))


def _render_download_button(results: list[dict], env_label: str, key: str) -> None:
    try:
        import tempfile
        from tools.excel_tools import export_validation_excel

        with tempfile.TemporaryDirectory() as tmp:
            out        = export_validation_excel(results, env_label, tmp)
            file_bytes = out.read_bytes()
            file_name  = out.name

        st.download_button(
            label="⬇️ Download Results Excel",
            data=file_bytes,
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"dl_{key}",
        )
    except Exception as exc:
        st.caption(f"Excel export unavailable: {exc}")


# ── Detailed sub-components ───────────────────────────────────────────────────

def _render_file_context(data: dict) -> None:
    """Warnings, diagnostics, and context strip for a single file."""
    summary  = data.get("summary", {})
    sql_info = data.get("sql_structure", {})

    low_conf = summary.get("low_confidence", 0)
    if low_conf:
        st.warning(
            f"⚠️ **{low_conf} rule(s) have LOW confidence** — "
            "human review is required before these can be considered validated."
        )
    if data.get("sql_fetch_error"):
        st.warning(f"SQL fetch issue: {data['sql_fetch_error']}")
    if data.get("sql_debug"):
        dbg = data["sql_debug"]
        with st.expander("🔍 SQL fetch diagnostics", expanded=True):
            st.write(f"**Step failed:** `{dbg.get('step_failed', '?')}`")
            if dbg.get("hint"):
                st.info(dbg["hint"])
            if dbg.get("files_found"):
                st.write("**Files found:**")
                for f in dbg["files_found"]:
                    st.code(f, language=None)
            if dbg.get("sql_per_file"):
                st.write("**SQL extraction per file:**")
                for fpath, result in dbg["sql_per_file"].items():
                    if isinstance(result, list):
                        st.write(f"`{fpath}` → tasks: {result}")
                    else:
                        st.write(f"`{fpath}` → {result}")

    ctx_parts = []
    if data.get("mapping_file"):
        ctx_parts.append(f"File: **{data['mapping_file']}**")
    if data.get("dag_id"):
        ctx_parts.append(f"DAG: **{data['dag_id']}**")
    if data.get("composer_env"):
        ctx_parts.append(f"Env: **{data['composer_env']}**")
    if sql_info.get("tasks_evaluated"):
        ctx_parts.append(f"Tasks: **{len(sql_info['tasks_evaluated'])}**")
    if sql_info.get("cte_count"):
        ctx_parts.append(f"CTEs: **{sql_info['cte_count']}**")
    if sql_info.get("join_count"):
        ctx_parts.append(f"JOINs: **{sql_info['join_count']}**")
    if ctx_parts:
        st.caption(" · ".join(ctx_parts))

    if sql_info.get("parse_errors"):
        with st.expander(f"⚠️ {len(sql_info['parse_errors'])} SQL parse error(s)"):
            for err in sql_info["parse_errors"]:
                st.code(err or "(unknown)")


def _render_bq_groups(groups: list, file_label: str, uid: str, reviewed_key: str) -> None:
    for group in groups:
        bq_label = group.get("bq_table", "Unknown")
        rules    = group.get("rules", [])
        if not rules:
            continue

        pass_n = sum(1 for r in rules if r["verdict"] == "PASS")
        fail_n = sum(1 for r in rules if r["verdict"] == "FAIL")
        part_n = sum(1 for r in rules if r["verdict"] == "PARTIAL")
        na_n   = sum(1 for r in rules if r["verdict"] == "NOT_APPLICABLE")
        ne_n   = sum(1 for r in rules if r["verdict"] == "NOT_EVALUATED")
        low_n  = sum(
            1 for r in rules
            if r.get("confidence_tier") == "LOW"
            and r["verdict"] not in ("NOT_APPLICABLE", "NOT_EVALUATED")
        )
        low_badge = f" · ⚠️ {low_n} low-conf" if low_n else ""
        ne_badge  = f" · 🔵 {ne_n} no-sql"   if ne_n  else ""
        st.markdown(
            f'<div style="background:#F3F4F6;border-left:4px solid #374151;'
            f'padding:8px 14px;border-radius:4px;margin:16px 0 6px;">'
            f'<span style="font-weight:700;font-size:14px;">📦 {bq_label}</span>'
            f'&nbsp;&nbsp;<span style="color:#6B7280;font-size:12px;">'
            f'🟢 {pass_n} &nbsp;🔴 {fail_n} &nbsp;🟡 {part_n} &nbsp;⚪ {na_n}'
            f"{low_badge}{ne_badge}</span></div>",
            unsafe_allow_html=True,
        )

        filter_opts = ["All", "FAIL", "PARTIAL", "PASS", "NOT_APPLICABLE", "NOT_EVALUATED", "ERROR"]
        chosen = st.selectbox(
            "Filter",
            filter_opts,
            key=f"mvf_{uid}_{file_label}_{bq_label[:24]}",
            label_visibility="collapsed",
        )
        visible = rules if chosen == "All" else [r for r in rules if r["verdict"] == chosen]

        for rule in visible:
            rule_id = rule.get("rule_id", 0)
            verdict = rule.get("verdict", "ERROR")
            icon, color, bg = _VERDICT.get(verdict, ("❓", "#6B7280", "#F9FAFB"))
            conf             = rule.get("confidence_tier", "")
            conf_label, conf_color = _CONFIDENCE.get(conf, (conf, "#6B7280"))
            is_low    = conf == "LOW" and verdict not in ("NOT_APPLICABLE", "NOT_EVALUATED")
            reviewed  = rule_id in st.session_state.get(reviewed_key, set())

            target_str   = ", ".join(rule.get("target_columns") or []) or "(unknown)"
            rule_preview = rule.get("rule_text") or ""
            rule_preview = rule_preview[:80] + "…" if len(rule_preview) > 80 else rule_preview
            cache_badge  = " · 💾" if rule.get("cache_hit") else ""
            rev_badge    = " · ✅ reviewed" if reviewed else ""

            header = (
                f'`#{rule_id}` **{target_str}** '
                f'<span style="color:{color};font-weight:700;">{icon} {verdict}</span>'
            )
            if conf:
                header += f' <span style="color:{conf_color};font-size:11px;">{conf_label}</span>'
            if cache_badge or rev_badge:
                header += (
                    f' <span style="color:#9CA3AF;font-size:11px;">{cache_badge}{rev_badge}</span>'
                )

            with st.expander(header):
                if is_low and not reviewed:
                    st.warning(
                        "⚠️ **LOW confidence** — this verdict requires human review "
                        "before being considered validated."
                    )

                tab_rule, tab_sql, tab_reason, tab_raw = st.tabs(
                    ["Rule", "SQL Evidence", "AI Reasoning", "Raw"]
                )

                with tab_rule:
                    col_a, col_b = st.columns(2)
                    with col_a:
                        st.markdown("**Target column(s)**")
                        st.code(", ".join(rule.get("target_columns") or []) or "(none)")
                        st.markdown("**Source column(s)**")
                        st.code(", ".join(rule.get("source_columns") or []) or "(not specified)")
                    with col_b:
                        st.markdown("**Rule type**")
                        st.code(rule.get("rule_type") or "(unknown)")
                        st.markdown("**Confidence tier**")
                        st.markdown(
                            f'<span style="color:{conf_color};font-weight:700;font-size:14px;">'
                            f"{conf_label}</span>",
                            unsafe_allow_html=True,
                        )
                    st.markdown("**Full rule text**")
                    st.info(rule.get("rule_text") or "(empty — marked as N/A)")

                with tab_sql:
                    evidence = (rule.get("evidence") or "").strip()
                    if evidence:
                        h = min(max(120, evidence.count("\n") * 20 + 80), 500)
                        components.html(
                            monaco.editor(evidence, language="sql", height=h), height=h + 20
                        )
                    else:
                        st.info("No specific SQL evidence was extracted for this rule.")
                    rel_ctes = rule.get("relevant_ctes") or []
                    rel_cls  = rule.get("relevant_clauses") or []
                    if rel_ctes:
                        st.caption(f"Relevant CTEs evaluated: {', '.join(rel_ctes)}")
                    if rel_cls:
                        st.caption(f"SQL clauses checked: {', '.join(rel_cls)}")

                with tab_reason:
                    reason = (rule.get("reason") or "").strip()
                    if reason:
                        st.markdown(
                            f'<div style="background:{bg};border:1px solid {color};'
                            f'border-radius:6px;padding:10px 14px;margin-bottom:10px;">'
                            f'<span style="color:{color};font-weight:700;">{icon} {verdict}</span><br>'
                            f'<span style="font-size:13px;">{reason}</span></div>',
                            unsafe_allow_html=True,
                        )
                    flags = rule.get("flags") or []
                    if flags:
                        st.markdown("**Specific issues found:**")
                        for flag in flags:
                            st.markdown(f"- {flag}")

                    if is_low:
                        if reviewed:
                            st.success("✅ Marked as human-reviewed this session.")
                        else:
                            if st.button(
                                "✅ Mark as Human-Reviewed",
                                key=f"rev_{uid}_{file_label}_{rule_id}",
                            ):
                                if reviewed_key not in st.session_state:
                                    st.session_state[reviewed_key] = set()
                                st.session_state[reviewed_key].add(rule_id)
                                st.rerun()

                with tab_raw:
                    raw_str = json.dumps(rule, indent=2, default=str)
                    h = min(max(200, raw_str.count("\n") * 18), 500)
                    components.html(
                        monaco.editor(raw_str, language="json", height=h), height=h + 20
                    )


def _render_mapping_details(data: dict, uid: str) -> None:
    """'Mapping Details for: file.xlsx' collapsible — column config + BQ groups."""
    file_label = data.get("mapping_file", "")
    col_config = data.get("column_config", {})
    groups     = data.get("bq_table_groups", [])

    reviewed_key = f"mv_reviewed_{data.get('duckdb_table', uid)}_{file_label}"
    if reviewed_key not in st.session_state:
        st.session_state[reviewed_key] = set()

    with st.expander(f"📑 Mapping Details for: {file_label}", expanded=False):
        # Column role table
        det  = col_config.get("detected_by", {})
        rows = []
        for role in ("target", "source", "logic", "bq_table"):
            col_name = col_config.get(role) or "(not found)"
            how      = det.get(role, "auto")
            rows.append({"Role": role, "Mapped to column": col_name, "Detected by": how})
        supp = col_config.get("logic_supplementary") or []
        if supp:
            rows.append({
                "Role":            "logic_supplementary",
                "Mapped to column": ", ".join(supp),
                "Detected by":     "auto",
            })
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
        st.caption(
            "Override any role by setting `mapping_columns` in `config/excel_mapping.json`."
        )

        if groups:
            st.markdown("---")
            _render_bq_groups(groups, file_label, uid, reviewed_key)


# ── Public entry points ───────────────────────────────────────────────────────

def render_mapping_validation(raw_json: str, compact: bool = False) -> None:
    """Render a single validate_mapping_rules result.

    compact=True  → banner + scorecards only (used during 3-step batch flow; full
                    details appear later in render_export_result).
    compact=False → full layout: banner + scorecards + context + Mapping Details + download.
    """
    global _render_count
    _render_count += 1
    uid = str(_render_count)

    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse mapping validation result.")
        return

    if "error" in data:
        st.error(f"Mapping validation error: {data['error']}")
        if "available_columns" in data:
            st.caption("Columns found in the Excel file:")
            st.code(", ".join(data["available_columns"]))
            st.info(
                "Set **mapping_columns** in `config/excel_mapping.json` for this file "
                "to specify which columns hold target, source, and logic."
            )
        if "column_config" in data:
            with st.expander("Column detection details"):
                st.json(data["column_config"])
        return

    # ── Legacy bulk path (validate_mapping_folder) ────────────────────────────
    if data.get("is_bulk"):
        n_files = data.get("files_processed", 0)
        st.markdown(f"### 📊 Consolidated Validation Status ({n_files} file(s))")
        _render_scorecards(data.get("overall_summary", {}))
        for w in data.get("warnings", []):
            st.warning(w)
        for res in data.get("results", []):
            st.divider()
            _render_banner(res.get("mapping_file", ""), completed=True)
            _render_scorecards(res.get("summary", {}))
            _render_file_context(res)
            _render_mapping_details(res, uid)
        st.divider()
        _render_download_button(
            data.get("results", []), data.get("env_label", "batch"), uid
        )
        return

    # ── Single file ───────────────────────────────────────────────────────────
    file_label = data.get("mapping_file", "")
    _render_banner(file_label, completed=True)
    _render_scorecards(data.get("summary", {}))

    if not compact:
        _render_file_context(data)
        _render_mapping_details(data, uid)
        env_label = data.get("composer_env") or data.get("source_mode") or "local"
        _render_download_button([data], env_label, uid)


def render_export_result(raw_json: str) -> None:
    """Consolidated batch view: header + accumulated scorecards + all files as 'Completed' + download."""
    global _render_count
    _render_count += 1
    uid = str(_render_count)

    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        return

    if data.get("error"):
        st.error(data["error"])
        return

    n = data.get("files_exported", 0)
    st.markdown(f"### 📊 Consolidated Validation Status ({n} file(s))")

    # Accumulated scorecards (grand total)
    ovr = data.get("overall_summary", {})
    _render_scorecards(ovr)

    # File-level errors (persisted from batch loop — survive st.rerun())
    file_errors = data.get("file_errors", [])
    if file_errors:
        with st.expander(f"⚠️ {len(file_errors)} file(s) could not be validated", expanded=True):
            for e in file_errors:
                st.warning(e)

    # Per-file completed sections
    results = data.get("results", [])
    for res in results:
        st.divider()
        _render_banner(res.get("mapping_file", ""), completed=True)
        _render_scorecards(res.get("summary", {}))
        _render_file_context(res)
        _render_mapping_details(res, uid)

    # Download at bottom
    st.divider()
    from pathlib import Path as _Path
    export_path = data.get("export_path")
    if export_path:
        ep = _Path(export_path)
        if ep.is_file():
            st.download_button(
                label="⬇️ Download All Results — Excel",
                data=ep.read_bytes(),
                file_name=ep.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_export_{ep.stem}_{uid}",
            )
    elif results:
        # Fallback: generate inline if export_path is missing
        env_label = data.get("env_label", "batch")
        _render_download_button(results, env_label, uid)
