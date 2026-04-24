"""Renderer for optimise_file and compare_git_gcs tool outputs."""
import difflib
import json
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

from core import monaco

_IMPACT_BADGES = {"High": "🔴", "Medium": "🟡", "Low": "🟢"}


def render_optimised_file(raw_json: str) -> None:
    """Render optimised file result: side-by-side diff, change list, download button."""
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse optimisation result.")
        return

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
                monaco.editor(original, language=lang, height=500, read_only=True),
                height=520,
            )
        with col_b:
            st.markdown("**Optimised**")
            components.html(
                monaco.editor(optimised, language=lang, height=500, read_only=True),
                height=520,
            )


def render_optimised_folder(raw_json: str) -> None:
    """Render optimise_folder result: summary metrics, zip download, per-file expandable panels."""
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
