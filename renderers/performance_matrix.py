"""Task performance matrix renderer with Plotly horizontal bars."""
import json
import pandas as pd
import plotly.graph_objects as go
import streamlit as st


_HEALTH_COLORS = {"healthy": "#1B8A3E", "warning": "#B38600", "critical": "#C41230"}
_HEALTH_ICONS = {"healthy": "🟢", "warning": "🟡", "critical": "🔴"}


def render(raw_json: str) -> None:
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse performance data.")
        return

    if "error" in data:
        st.error(f"Performance error: {data['error']}")
        return

    perf = data.get("performance", [])
    dag_id = data.get("dag_id", "DAG")

    if not perf:
        st.info("No performance data available.")
        return

    df = pd.DataFrame(perf)
    df["color"] = df["health_status"].map(_HEALTH_COLORS).fillna("#6B7280")

    # Health summary
    counts = df["health_status"].value_counts().to_dict()
    critical_n = counts.get("critical", 0)
    warning_n = counts.get("warning", 0)

    if critical_n > 0:
        overall_icon = "🔴"
    elif warning_n > 0:
        overall_icon = "🟡"
    else:
        overall_icon = "🟢"

    st.markdown(
        f'<div style="font-size:24px;font-weight:800;margin-bottom:8px;">'
        f'{overall_icon} {dag_id} Performance</div>',
        unsafe_allow_html=True,
    )

    if critical_n > 0:
        st.error(f"⚠️ {critical_n} task(s) are in critical state — exceeding threshold.")

    # Horizontal bar chart
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df["task_id"],
        x=df["avg_duration_s"],
        orientation="h",
        marker_color=df["color"].tolist(),
        text=[f"{v:.1f}s" for v in df["avg_duration_s"]],
        textposition="outside",
        name="Avg Duration",
    ))
    fig.add_trace(go.Bar(
        y=df["task_id"],
        x=df["p95_duration_s"],
        orientation="h",
        marker_color=[c + "66" for c in df["color"].tolist()],
        text=[f"p95: {v:.1f}s" for v in df["p95_duration_s"]],
        textposition="outside",
        name="P95 Duration",
        opacity=0.6,
    ))

    fig.update_layout(
        barmode="overlay",
        paper_bgcolor="#1F2937", plot_bgcolor="#1F2937",
        font=dict(color="#E5E7EB", family="JetBrains Mono"),
        xaxis=dict(title="Duration (s)", gridcolor="#374151"),
        yaxis=dict(gridcolor="#374151", tickfont=dict(color="#9CA3AF")),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=40, r=60, t=20, b=40),
        height=max(200, len(df) * 40 + 80),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Summary table
    cols_to_show = [c for c in ["task_id", "avg_duration_s", "max_duration_s", "p95_duration_s", "success_rate", "run_count", "health_status"] if c in df.columns]
    st.dataframe(df[cols_to_show], hide_index=True, use_container_width=True)
