"""Monaco DiffEditor renderer for SQL optimisation results."""
import json
import zipfile
import io
import streamlit as st
import streamlit.components.v1 as components

from core import monaco, sql_formatter
from core.audit import log_audit


def render(raw_json: str) -> None:
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse optimisation result.")
        return

    if "error" in data:
        st.error(f"Optimisation error: {data['error']}")
        return

    original_sql = data.get("original_sql", "")
    optimised_sql = data.get("optimised_sql", "")
    changes = data.get("changes", [])
    confidence = data.get("overall_confidence_score", 0)
    summary = data.get("overall_summary", "")

    # Confidence badge
    if confidence >= 80:
        badge_color = "#1B8A3E"
        badge_label = "HIGH"
    elif confidence >= 50:
        badge_color = "#B38600"
        badge_label = "MEDIUM"
    else:
        badge_color = "#C41230"
        badge_label = "LOW"

    st.markdown(
        f'<span style="background:{badge_color};color:#fff;padding:3px 10px;border-radius:12px;'
        f'font-size:12px;font-weight:700;">Confidence: {badge_label} ({confidence}/100)</span>',
        unsafe_allow_html=True,
    )
    if summary:
        st.caption(summary)

    # Monaco diff editor
    fmt_orig = sql_formatter.format_sql(original_sql)
    fmt_opt = sql_formatter.format_sql(optimised_sql)
    diff_html = monaco.diff_editor(fmt_orig, fmt_opt, language="sql", height=480)
    components.html(diff_html, height=500)

    # Change explanation cards
    if changes:
        st.markdown("**Changes**")
        for i, ch in enumerate(changes):
            impact = ch.get("estimated_impact", "")
            conf = ch.get("confidence", "")
            color = {"High": "#1B8A3E", "Medium": "#B38600", "Low": "#6B7280"}.get(impact, "#6B7280")
            with st.expander(f"{ch.get('change_type', f'Change {i+1}')}  ·  Impact: {impact}  ·  Confidence: {conf}"):
                st.write(ch.get("reason", ""))
                c1, c2 = st.columns(2)
                with c1:
                    st.caption("Original")
                    components.html(monaco.editor(ch.get("original_snippet", ""), "sql", 120), height=140)
                with c2:
                    st.caption("Optimised")
                    components.html(monaco.editor(ch.get("optimised_snippet", ""), "sql", 120), height=140)

    # Action buttons
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        st.download_button(
            "📋 Copy Optimised SQL",
            data=fmt_opt,
            file_name="optimised.sql",
            mime="text/plain",
            key=f"copy_opt_{id(raw_json)}",
        )
    with col2:
        if st.button("➕ Add to Bundle", key=f"bundle_{id(raw_json)}"):
            if "sql_bundle" not in st.session_state:
                st.session_state.sql_bundle = []
            st.session_state.sql_bundle.append({"original": fmt_orig, "optimised": fmt_opt, "changes": changes})
            st.success(f"Added to bundle ({len(st.session_state.sql_bundle)} items)")
    with col3:
        if st.button("✗ Reject", key=f"reject_{id(raw_json)}"):
            log_audit("diff_viewer", "ui", "reject_optimisation", user_action="rejected by user")
            st.info("Optimisation rejected and logged.")

    # Export bundle
    if st.session_state.get("sql_bundle"):
        st.markdown(f"**Bundle: {len(st.session_state.sql_bundle)} items**")
        if st.button("📦 Export Bundle as ZIP", key=f"export_bundle_{id(raw_json)}"):
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w") as zf:
                for idx, item in enumerate(st.session_state.sql_bundle):
                    zf.writestr(f"optimised_{idx+1}.sql", item["optimised"])
                    zf.writestr(f"original_{idx+1}.sql", item["original"])
                    zf.writestr(f"changes_{idx+1}.json", json.dumps(item["changes"], indent=2))
            st.download_button("⬇ Download ZIP", buf.getvalue(), "optimisation_bundle.zip",
                               mime="application/zip", key=f"dl_zip_{id(raw_json)}")
