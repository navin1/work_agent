"""Renderer for optimise_file and compare_git_gcs tool outputs."""
import difflib
import json
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

from core import monaco

_IMPACT_BADGES = {"High": "🔴", "Medium": "🟡", "Low": "🟢"}
_render_count = 0

_EXT_LANG = {"sql": "sql", "py": "python", "yaml": "yaml", "yml": "yaml", "json": "json", "sh": "shell"}


def render_file_content(raw_json: str) -> None:
    """Render raw file content from read_file tool in a Monaco editor."""
    global _render_count
    _render_count += 1
    uid = str(_render_count)

    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse file content.")
        return

    if "error" in data:
        st.error(f"Read error: {data['error']}")
        return

    file_path = data.get("file_path", "")
    content   = data.get("content", "")
    size      = data.get("size_bytes", 0)
    ext       = data.get("extension", "").lower()
    lang      = _EXT_LANG.get(ext, "plaintext")

    st.caption(f"`{file_path}` · {size:,} bytes")
    components.html(
        monaco.editor(content, language=lang, height=500),
        height=520,
    )
    st.download_button(
        f"⬇ Download {Path(file_path).name}",
        data=content.encode("utf-8"),
        file_name=Path(file_path).name,
        mime="text/plain",
        key=f"dl_read_{uid}",
    )


def render_optimised_file(raw_json: str) -> None:
    """Render optimised file result: side-by-side diff, change list, download button."""
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse optimisation result.")
        return

    global _render_count
    _render_count += 1
    uid = str(_render_count)

    if "error" in data:
        st.error(f"Optimisation error: {data['error']}")
        return

    file_name = data.get("file_name", "file")
    file_type = data.get("file_type", "sql")
    original = data.get("original_content", "")
    optimised = data.get("optimised_content", "")
    changes = data.get("changes", [])
    score = data.get("overall_confidence_score")
    summary = data.get("overall_summary", "")
    export_path = data.get("export_path")

    lang = "sql" if file_type == "sql" else "python"

    st.subheader(f"Optimised: {file_name}")

    # Metrics row
    c1, c2, c3 = st.columns(3)
    c1.metric("Changes", len(changes))
    c2.metric("Confidence", f"{score}%" if score is not None else "—")
    high_impact = sum(1 for c in changes if c.get("estimated_impact") == "High")
    c3.metric("High-Impact", high_impact)

    if summary:
        st.info(summary)

    # Download button — use content from response to avoid a redundant disk read
    if export_path:
        dl_name = Path(export_path).name
        st.download_button(
            label=f"⬇ Download {dl_name}",
            data=optimised.encode("utf-8"),
            file_name=dl_name,
            mime="text/plain",
            type="primary",
            key=f"dl_opt_file_{uid}",
        )

    tab1, tab2, tab3 = st.tabs(["Diff", "Changes", "Side-by-Side"])

    with tab1:
        diff_lines = list(difflib.unified_diff(
            original.splitlines(keepends=True),
            optimised.splitlines(keepends=True),
            fromfile="original",
            tofile="optimised",
        ))
        if diff_lines:
            diff_text = "".join(diff_lines)
            components.html(
                monaco.editor(diff_text, language="diff", height=500),
                height=520,
            )
        else:
            st.success("No changes — file is already optimal.")

    with tab2:
        if changes:
            for i, ch in enumerate(changes):
                impact = ch.get("estimated_impact", "")
                conf = ch.get("confidence", "")
                badge = _IMPACT_BADGES.get(impact, "")
                with st.expander(f"{badge} {ch.get('change_type','Change')} — {impact} impact · {conf} confidence", expanded=i == 0):
                    st.markdown(f"**Reason:** {ch.get('reason','')}")
                    col_a, col_b = st.columns(2)
                    with col_a:
                        st.markdown("**Before**")
                        st.code(ch.get("original_snippet", ""), language=lang)
                    with col_b:
                        st.markdown("**After**")
                        st.code(ch.get("optimised_snippet", ""), language=lang)
        else:
            st.info("No individual changes recorded.")

    with tab3:
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**Original**")
            components.html(
                monaco.editor(original, language=lang, height=500),
                height=520,
            )
        with col_b:
            st.markdown("**Optimised**")
            components.html(
                monaco.editor(optimised, language=lang, height=500),
                height=520,
            )


def _render_doc_md_panel(doc_md: dict, dag_id: str) -> None:
    """Render the doc_md panel: overview, Control-M job link, impacted tables/views card grid."""
    from core import config as _cfg

    st.markdown("#### 📋 DAG Documentation")

    # ── Overview ──────────────────────────────────────────────────────────────
    overview = doc_md.get("overview", "").strip()
    if overview:
        st.markdown("**Overview**")
        st.info(overview)

    # ── Control-M job ─────────────────────────────────────────────────────────
    job_name = doc_md.get("control_m_job", "").strip()
    if not job_name:
        job_name = dag_id.upper().replace("-", "_")

    confluence_url = _cfg.CONFLUENCE_BASE_URL.rstrip("/") if _cfg.CONFLUENCE_BASE_URL else ""
    folder         = _cfg.CONTROLM_FOLDER or "—"
    server         = _cfg.CONTROLM_SERVER or "—"

    if confluence_url:
        job_href   = f"{confluence_url}/{job_name}"
        job_anchor = (
            f'<a href="{job_href}" target="_blank" style="'
            f'color:#0052CC;font-weight:700;font-size:15px;text-decoration:none;">'
            f'{job_name}</a>'
        )
    else:
        job_anchor = f'<span style="font-weight:700;font-size:15px;">{job_name}</span>'

    # Confluence "C" icon badge
    c_icon = (
        '<span style="display:inline-flex;align-items:center;justify-content:center;'
        'width:22px;height:22px;background:#0052CC;color:#fff;border-radius:4px;'
        'font-size:12px;font-weight:900;margin-right:8px;">C</span>'
    )

    st.markdown("**Control-M Job**")
    st.markdown(
        f'<div style="background:#F0F4FF;border:1px solid #C7D2FE;border-radius:8px;'
        f'padding:12px 16px;display:inline-block;min-width:360px;">'
        f'<div style="display:flex;align-items:center;margin-bottom:6px;">'
        f'{c_icon}{job_anchor}</div>'
        f'<div style="font-size:12px;color:#374151;">'
        f'<span style="margin-right:16px;">📁 <strong>Folder:</strong> {folder}</span>'
        f'<span>🖥️ <strong>Server:</strong> {server}</span>'
        f'</div></div>',
        unsafe_allow_html=True,
    )

    # ── Impacted tables & views ───────────────────────────────────────────────
    objects = doc_md.get("impacted_objects", [])
    if not objects:
        return

    st.markdown("**Impacted Tables & Views**")

    _TYPE_STYLE = {
        "table": ("#DBEAFE", "#1E40AF", "TABLE"),
        "view":  ("#D1FAE5", "#065F46", "VIEW"),
    }
    _OP_STYLE = {
        "read":       ("#EDE9FE", "#5B21B6", "READ"),
        "write":      ("#FEE2E2", "#991B1B", "WRITE"),
        "read/write": ("#FFF7ED", "#92400E", "READ/WRITE"),
    }

    def _obj_card(obj: dict) -> str:
        obj_type = obj.get("type", "table").lower()
        operation = obj.get("operation", "read").lower()
        name = obj.get("name", "—")
        desc = obj.get("description", "")
        t_bg, t_color, t_label = _TYPE_STYLE.get(obj_type, _TYPE_STYLE["table"])
        o_bg, o_color, o_label = _OP_STYLE.get(operation, _OP_STYLE["read"])
        return (
            f'<div style="border:1px solid #E5E7EB;border-radius:8px;padding:10px 12px;'
            f'background:#FAFAFA;height:100%;">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">'
            f'<span style="background:{t_bg};color:{t_color};font-size:10px;font-weight:700;'
            f'padding:2px 7px;border-radius:4px;">{t_label}</span>'
            f'<span style="background:{o_bg};color:{o_color};font-size:10px;font-weight:700;'
            f'padding:2px 7px;border-radius:4px;">{o_label}</span>'
            f'</div>'
            f'<div style="font-family:monospace;font-size:12px;font-weight:700;color:#1F2937;'
            f'word-break:break-all;margin-bottom:4px;">{name}</div>'
            f'<div style="font-size:11px;color:#6B7280;line-height:1.4;">{desc}</div>'
            f'</div>'
        )

    cols = st.columns(3)
    for idx, obj in enumerate(objects[:10]):
        with cols[idx % 3]:
            st.markdown(_obj_card(obj), unsafe_allow_html=True)
            st.markdown("")  # spacing

    # Legend
    st.markdown(
        '<div style="margin-top:8px;font-size:11px;color:#6B7280;">'
        '<strong>Legend — Type:</strong> '
        '<span style="background:#DBEAFE;color:#1E40AF;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:700;">TABLE</span> '
        '<span style="background:#D1FAE5;color:#065F46;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:700;">VIEW</span> '
        '&nbsp;&nbsp;<strong>Operation:</strong> '
        '<span style="background:#EDE9FE;color:#5B21B6;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:700;">READ</span> '
        '<span style="background:#FEE2E2;color:#991B1B;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:700;">WRITE</span> '
        '<span style="background:#FFF7ED;color:#92400E;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:700;">READ/WRITE</span>'
        '</div>',
        unsafe_allow_html=True,
    )


def render_dag_suggestions(raw_json: str) -> None:
    """Render optimise_dag result: doc_md panel + optimised file diff + suggestions."""
    global _render_count
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse DAG optimisation result.")
        return

    if "error" in data:
        st.error(f"DAG optimisation error: {data['error']}")
        return

    _render_count += 1
    uid = str(_render_count)

    dag_id      = data.get("dag_id", "")
    original    = data.get("original_content", "")
    optimised   = data.get("optimised_content", "")
    export_path = data.get("export_path", "")
    suggestions = data.get("suggestions", [])
    if isinstance(suggestions, dict):
        suggestions = suggestions.get("suggestions", [])
    doc_md = data.get("doc_md", {})

    st.subheader(f"DAG Optimisation: {dag_id}")

    # ── Doc MD panel ─────────────────────────────────────────────────────────
    _render_doc_md_panel(doc_md, dag_id)
    st.divider()

    # ── Optimised file view ───────────────────────────────────────────────────
    if optimised:
        if export_path:
            st.download_button(
                label=f"⬇ Download {Path(export_path).name}",
                data=optimised.encode("utf-8"),
                file_name=Path(export_path).name,
                mime="text/plain",
                type="primary",
                key=f"dl_dag_{uid}",
            )

        tab_diff, tab_side = st.tabs(["Diff", "Side-by-Side"])

        with tab_diff:
            diff_lines = list(difflib.unified_diff(
                original.splitlines(keepends=True),
                optimised.splitlines(keepends=True),
                fromfile="original",
                tofile="optimised",
            ))
            if diff_lines:
                components.html(
                    monaco.editor("".join(diff_lines), language="diff", height=500),
                    height=520,
                )
            else:
                st.success("No changes — DAG is already optimal.")

        with tab_side:
            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown("**Original**")
                components.html(
                    monaco.editor(original, language="python", height=500),
                    height=520,
                )
            with col_b:
                st.markdown("**Optimised**")
                components.html(
                    monaco.editor(optimised, language="python", height=500),
                    height=520,
                )

        st.divider()

    # ── Suggestions ───────────────────────────────────────────────────────────
    if not suggestions:
        st.info("No additional suggestions — all changes are reflected in the optimised file above.")
        return

    _CAT_LABELS = {
        "dag_loading":   "🔧 DAG Loading",
        "modernisation": "⚡ Modernisation",
        "structural":    "🏗️ Structural",
    }

    by_category: dict = {}
    for s in suggestions:
        cat = s.get("category", "general")
        by_category.setdefault(cat, []).append(s)

    st.markdown("**Optimisation Suggestions**")
    for cat, items in by_category.items():
        label = _CAT_LABELS.get(cat, cat.title())
        st.markdown(f"_{label}_ — {len(items)} suggestion{'s' if len(items) != 1 else ''}")
        for s in items:
            conf = s.get("confidence", "")
            badge = _IMPACT_BADGES.get(conf, "")
            with st.expander(f"{badge} {s.get('description', 'Suggestion')} — {conf} confidence"):
                st.markdown(f"**Reason:** {s.get('reason', '')}")
                col_a, col_b = st.columns(2)
                with col_a:
                    st.markdown("**Current**")
                    st.code(s.get("current_code", ""), language="python")
                with col_b:
                    st.markdown("**Suggested**")
                    st.code(s.get("suggested_code", ""), language="python")


def render_optimised_folder(raw_json: str) -> None:
    """Render optimise_folder result: summary metrics, zip download, per-file expandable panels."""
    global _render_count
    _render_count += 1
    uid = str(_render_count)

    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse folder optimisation result.")
        return

    if "error" in data:
        st.error(f"Folder optimisation error: {data['error']}")
        return

    folder_path = data.get("folder_path", "")
    results = data.get("results", [])
    export_path = data.get("export_path")

    st.subheader(f"Folder Optimised: `{folder_path}`")

    c1, c2, c3 = st.columns(3)
    c1.metric("Files Found", data.get("total_files", 0))
    c2.metric("Optimised", data.get("optimised", 0))
    c3.metric("Errors", data.get("errors", 0))

    if export_path and Path(export_path).exists():
        zip_bytes = Path(export_path).read_bytes()
        st.download_button(
            label=f"⬇ Download All ({Path(export_path).name})",
            data=zip_bytes,
            file_name=Path(export_path).name,
            mime="application/zip",
            type="primary",
            key=f"dl_opt_folder_{uid}",
        )

    for r in results:
        status = r.get("status", "ok")
        icon = "✅" if status == "ok" else "❌"
        changes = r.get("changes", [])
        score = r.get("overall_confidence_score")
        label = f"{icon} {r.get('file_name','')} — {len(changes)} change(s)"
        if score is not None:
            label += f" · {score}% confidence"
        with st.expander(label):
            if status == "error":
                st.error(r.get("error", "Unknown error"))
                continue
            if r.get("overall_summary"):
                st.info(r["overall_summary"])
            lang = "sql" if r.get("file_type") == "sql" else "python"
            tab1, tab2 = st.tabs(["Changes", "Side-by-Side"])
            with tab1:
                if changes:
                    for ch in changes:
                        impact = ch.get("estimated_impact", "")
                        badge = _IMPACT_BADGES.get(impact, "")
                        with st.expander(
                            f"{badge} {ch.get('change_type','Change')} — {impact} · {ch.get('confidence','')} confidence"
                        ):
                            st.markdown(f"**Reason:** {ch.get('reason','')}")
                            col_a, col_b = st.columns(2)
                            with col_a:
                                st.markdown("**Before**")
                                st.code(ch.get("original_snippet", ""), language=lang)
                            with col_b:
                                st.markdown("**After**")
                                st.code(ch.get("optimised_snippet", ""), language=lang)
                else:
                    st.info("No changes — file already optimal.")
            with tab2:
                col_a, col_b = st.columns(2)
                with col_a:
                    st.markdown("**Original**")
                    st.code(r.get("original_content", ""), language=lang)
                with col_b:
                    st.markdown("**Optimised**")
                    st.code(r.get("optimised_content", ""), language=lang)


def render_git_gcs_diff(raw_json: str) -> None:
    """Render Git vs GCS comparison: summary metrics and per-file diffs."""
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse comparison result.")
        return

    if "error" in data:
        st.error(f"Comparison error: {data['error']}")
        return

    summary = data.get("summary", {})
    folder = data.get("folder", "")
    diffs = data.get("diffs", {})

    st.subheader(f"Git vs GCS: `{folder}`")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Only in Git", summary.get("only_in_git", 0), help="Not yet deployed to GCS")
    c2.metric("Only in GCS", summary.get("only_in_gcs", 0), help="Deployed but removed from Git")
    c3.metric("Drifted", summary.get("different", 0), delta=summary.get("different", 0) or None,
              delta_color="inverse", help="Same file, different content")
    c4.metric("In Sync", summary.get("identical", 0))

    tab1, tab2, tab3, tab4 = st.tabs(["Drifted Files", "Only in Git", "Only in GCS", "Identical"])

    with tab1:
        if diffs:
            for rel, info in diffs.items():
                if "error" in info:
                    st.warning(f"`{rel}` — {info['error']}")
                    continue
                with st.expander(f"📄 {rel}"):
                    cols = st.columns(2)
                    cols[0].caption(f"Git: {info.get('git_path','')}")
                    cols[1].caption(f"GCS: {info.get('gcs_path','')}")
                    sz_diff = info.get("gcs_size_bytes", 0) - info.get("git_size_bytes", 0)
                    st.caption(f"Size Δ: {sz_diff:+,} bytes")
                    diff_text = info.get("unified_diff", "")
                    if diff_text:
                        components.html(
                            monaco.editor(diff_text, language="diff", height=300),
                            height=320,
                        )
                    else:
                        st.info("No textual diff available.")
        else:
            st.success("No content drift detected.")

    with tab2:
        items = data.get("only_in_git", [])
        if items:
            for f in items:
                st.markdown(f"- `{f}`")
        else:
            st.info("All Git files are deployed to GCS.")

    with tab3:
        items = data.get("only_in_gcs", [])
        if items:
            for f in items:
                st.markdown(f"- `{f}`")
        else:
            st.info("No extra files in GCS.")

    with tab4:
        items = data.get("identical", [])
        if items:
            for f in items:
                st.markdown(f"- `{f}`")
        else:
            st.info("No identical files found.")
