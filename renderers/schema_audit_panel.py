"""Renderer for run_schema_audit tool output."""
import json
from pathlib import Path

import streamlit as st

_render_count = 0


_STATUS_COLORS = {
    "🟢": "#C6EFCE",
    "🟡": "#FFEB9C",
    "🟠": "#FFD580",
    "🔵": "#BDD7EE",
}

_STATUS_LABELS = {
    "🟢": "Match",
    "🟡": "Type Mismatch",
    "🟠": "BQ Only",
    "🔵": "MySQL Only",
}


def _metric_card(label: str, value, color: str = "#6366F1") -> str:
    return (
        f'<div style="background:{color}22;border:1px solid {color}55;border-radius:8px;'
        f'padding:12px 18px;text-align:center;">'
        f'<div style="font-size:22px;font-weight:700;color:{color};">{value}</div>'
        f'<div style="font-size:11px;color:#6B7280;margin-top:2px;">{label}</div>'
        f'</div>'
    )


def _render_batch(label: str, data: dict, uid: str) -> None:
    st.markdown(f"#### {label}")
    total   = data.get("total_columns", 0)
    matched = data.get("match", 0)
    pct     = round(matched / total * 100, 1) if total else 0

    cols = st.columns(5)
    metrics = [
        ("Total Columns", total,                        "#6366F1"),
        ("Match",         f"{matched} ({pct}%)",        "#22C55E"),
        ("Type Mismatch", data.get("type_mismatch", 0), "#EAB308"),
        ("BQ Only",       data.get("bq_only", 0),       "#F97316"),
        ("MySQL Only",    data.get("mysql_only", 0),     "#3B82F6"),
    ]
    for col, (lbl, val, color) in zip(cols, metrics):
        with col:
            st.markdown(_metric_card(lbl, val, color), unsafe_allow_html=True)

    st.markdown("")

    # Download buttons
    dl_cols = st.columns(2)
    excel_path = data.get("output_file", "")
    ddl_path   = data.get("ddl_json", "")

    with dl_cols[0]:
        if excel_path and Path(excel_path).exists():
            with open(excel_path, "rb") as fh:
                st.download_button(
                    f"⬇ Download {label} Excel",
                    data=fh.read(),
                    file_name=Path(excel_path).name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"dl_excel_{label}_{uid}",
                    width="stretch",
                )
        else:
            st.caption("Excel file not available")

    with dl_cols[1]:
        if ddl_path and Path(ddl_path).exists():
            with open(ddl_path, "rb") as fh:
                st.download_button(
                    f"⬇ Download {label} DDL JSON",
                    data=fh.read(),
                    file_name=Path(ddl_path).name,
                    mime="application/json",
                    key=f"dl_ddl_{label}_{uid}",
                    width="stretch",
                )
        else:
            st.caption("DDL JSON file not available")


def render_schema_audit(raw_json: str) -> None:
    global _render_count
    _render_count += 1
    uid = str(_render_count)

    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Schema audit: could not parse result")
        return

    if "error" in data:
        st.error(f"Schema audit failed: {data['error']}")
        return

    st.markdown("### Schema Audit — MySQL → BigQuery Reconciliation")

    overview_cols = st.columns(3)
    with overview_cols[0]:
        st.markdown(_metric_card("Total Tables", data.get("tables_found", 0), "#6366F1"), unsafe_allow_html=True)
    with overview_cols[1]:
        st.markdown(_metric_card("Prod Tables", data.get("prod_tables", 0), "#22C55E"), unsafe_allow_html=True)
    with overview_cols[2]:
        st.markdown(_metric_card("UAT Tables", data.get("uat_tables", 0), "#3B82F6"), unsafe_allow_html=True)

    st.markdown("---")

    if "prod" in data:
        _render_batch("Prod", data["prod"], uid)
        if "uat" in data:
            st.markdown("---")

    if "uat" in data:
        _render_batch("UAT", data["uat"], uid)

    if "prod_skipped" in data:
        st.info(f"Prod batch skipped: {data['prod_skipped']}")

    # Status legend
    st.markdown("")
    legend_html = " &nbsp; ".join(
        f'<span style="background:{bg};padding:2px 8px;border-radius:4px;font-size:12px;">{emoji} {lbl}</span>'
        for emoji, (lbl, bg) in {
            k: (_STATUS_LABELS[k], _STATUS_COLORS[k]) for k in _STATUS_COLORS
        }.items()
    )
    st.markdown(f'<div style="margin-top:4px;">{legend_html}</div>', unsafe_allow_html=True)
