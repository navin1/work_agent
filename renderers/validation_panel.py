"""Validation checklist panel renderer."""
import json
import streamlit as st


_STATUS_STYLE = {
    "pass": ("✅", "#1B8A3E", "#F0FFF4"),
    "warn": ("⚠️", "#B38600", "#FFFBEB"),
    "fail": ("❌", "#C41230", "#FFF0F0"),
}


def render(raw_json: str) -> None:
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse validation result.")
        return

    if "error" in data:
        st.error(f"Validation error: {data['error']}")
        return

    verdict = data.get("overall_verdict", "UNKNOWN")
    checklist = data.get("checklist", [])
    comparison = data.get("comparison_result", {})

    # Overall verdict badge
    verdict_color = "#1B8A3E" if verdict == "SAFE" else "#C41230"
    verdict_icon = "🟢" if verdict == "SAFE" else "🔴"
    st.markdown(
        f'<div style="background:{verdict_color};color:#fff;padding:10px 20px;border-radius:8px;'
        f'font-size:20px;font-weight:800;display:inline-block;margin-bottom:12px;">'
        f'{verdict_icon} {verdict}</div>',
        unsafe_allow_html=True,
    )

    # Comparison stats
    if comparison:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Row Count", "✓ Match" if comparison.get("row_count_match") else "✗ Mismatch")
        c2.metric("Columns", "✓ Match" if comparison.get("column_match") else "✗ Mismatch")
        c3.metric("Data Hash", "✓ Match" if comparison.get("data_hash_match") else "✗ Mismatch")
        c4.metric("Status", comparison.get("status", "?"))

    # Checklist badges
    if checklist:
        st.markdown("**Structural Checklist**")
        cols = st.columns(min(4, len(checklist)))
        for i, item in enumerate(checklist):
            status = item.get("status", "pass").lower()
            icon, color, bg = _STATUS_STYLE.get(status, ("❓", "#6B7280", "#F9FAFB"))
            reason = item.get("reason", "")
            label = item.get("item", f"item_{i}").replace("_", " ").title()
            with cols[i % len(cols)]:
                st.markdown(
                    f'<div style="background:{bg};border:1px solid {color};border-radius:6px;'
                    f'padding:8px 10px;margin:4px 0;font-size:12px;">'
                    f'<span style="color:{color};font-weight:700;">{icon} {label}</span>'
                    f'{"<br><span style=color:#6B7280;font-size:11px;>" + reason + "</span>" if reason else ""}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    # Fail details
    fails = [c for c in checklist if c.get("status") == "fail"]
    if fails:
        st.error(f"**{len(fails)} check(s) failed:** " + ", ".join(c.get("item", "") for c in fails))
