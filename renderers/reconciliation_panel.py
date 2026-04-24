"""Reconciliation panel renderer with summary cards, table, and detail tabs."""
import json
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from core import monaco


_STATUS_COLORS = {
    "in_sync": "#1B8A3E",
    "content_drift": "#C41230",
    "schema_drift": "#C41230",
    "undeclared": "#B38600",
    "not_deployed": "#B38600",
    "no_source": "#C41230",
    "git_only": "#6B7280",
    "gcs_orphan": "#6B7280",
    "mapping_ghost": "#C41230",
    "bq_missing": "#B38600",
}


def render(raw_json: str, agent=None) -> None:
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse reconciliation result.")
        return

    if "error" in data:
        st.error(f"Reconciliation error: {data['error']}")
        return

    summary = data.get("summary", {})
    critical = data.get("critical_findings", [])
    results = data.get("results", [])
    total = data.get("total", 0)
    cache_age = data.get("cache_age_minutes", 0)

    in_sync = summary.get("in_sync", 0)
    warnings = sum(summary.get(k, 0) for k in ["undeclared", "not_deployed", "gcs_orphan", "git_only", "bq_missing"])
    critical_n = sum(summary.get(k, 0) for k in ["content_drift", "schema_drift", "no_source", "mapping_ghost"])

    # Summary cards
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("✅ In Sync", in_sync)
    col2.metric("⚠️ Warnings", warnings)
    col3.metric("🔴 Critical", critical_n)
    col4.metric("Total", total)

    if cache_age > 0:
        st.caption(f"Cached result · {cache_age:.1f} min ago")

    if critical:
        st.error(f"**{len(critical)} critical findings** require attention.")

    # Filterable results table
    if results:
        df = pd.DataFrame(results)
        status_opts = ["All"] + sorted(df["status"].unique().tolist())
        chosen = st.selectbox("Filter by status", status_opts, key="recon_filter")
        if chosen != "All":
            df_show = df[df["status"] == chosen]
        else:
            df_show = df

        st.dataframe(df_show, hide_index=True, use_container_width=True)

        # Detail expander per row
        for _, row in df_show.iterrows():
            name = row["logical_name"]
            status = row["status"]
            color = _STATUS_COLORS.get(status, "#6B7280")
            with st.expander(
                f"`{name}`  ·  "
                + f'<span style="color:{color};font-weight:700;">{status}</span>'
                + ("  · ✓ acknowledged" if row.get("acknowledged") else ""),
            ):
                tab_diff, tab_mapping, tab_raw = st.tabs(["Diff", "Mapping vs Code", "Raw"])

                with tab_diff:
                    from tools.reconciliation_tools import get_reconciliation_detail
                    detail_raw = get_reconciliation_detail.run({"logical_name": name})
                    try:
                        detail = json.loads(detail_raw)
                        diff_text = detail.get("content_diff", "(no diff available)")
                        if diff_text:
                            components.html(monaco.editor(diff_text, "plaintext", 300), height=320)
                        else:
                            st.info("No content difference.")
                    except Exception:
                        st.write(detail_raw)

                with tab_mapping:
                    map_r = None
                    try:
                        detail = json.loads(detail_raw)
                        map_r = detail.get("mapping_record")
                    except Exception:
                        pass
                    if map_r:
                        st.json(map_r)
                    else:
                        st.info("No mapping record found.")

                with tab_raw:
                    try:
                        detail = json.loads(detail_raw)
                        components.html(monaco.editor(json.dumps(detail, indent=2, default=str), "json", 350), height=370)
                    except Exception:
                        st.write(detail_raw)

                if not row.get("acknowledged"):
                    reason = st.text_input("Acknowledge reason", key=f"ack_reason_{name}")
                    if st.button("✓ Acknowledge", key=f"ack_{name}") and reason:
                        from tools.reconciliation_tools import acknowledge_reconciliation_finding
                        acknowledge_reconciliation_finding.run({"logical_name": name, "reason": reason})
                        st.success("Acknowledged.")
                        st.rerun()
