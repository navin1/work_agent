"""DAG run history chart renderer."""
import json
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st


_STATUS_COLORS = {
    "success": "#1B8A3E",
    "failed": "#C41230",
    "running": "#B38600",
    "queued": "#6B7280",
    "skipped": "#9CA3AF",
}


def render(raw_json: str, agent=None) -> None:
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse run history.")
        return

    if "error" in data:
        st.error(f"Run history error: {data['error']}")
        return

    runs = data.get("runs", [])
    dag_id = data.get("dag_id", "DAG")

    if not runs:
        st.info("No run history available.")
        return

    df = pd.DataFrame(runs)
    df["run_number"] = range(len(df), 0, -1)
    df["duration"] = pd.to_numeric(df.get("duration_seconds", pd.Series([None] * len(df))), errors="coerce")
    df["color"] = df["status"].map(_STATUS_COLORS).fillna("#6B7280")

    st.subheader(f"Run History: {dag_id}")

    # Line chart with markers
    fig = go.Figure()

    # Trend line
    valid = df.dropna(subset=["duration"])
    if len(valid) > 1:
        fig.add_trace(go.Scatter(
            x=valid["run_number"], y=valid["duration"],
            mode="lines", line=dict(color="#E05C00", dash="dash", width=1),
            name="Trend", showlegend=False,
        ))

    for status, color in _STATUS_COLORS.items():
        mask = df["status"] == status
        if mask.any():
            sub = df[mask]
            fig.add_trace(go.Scatter(
                x=sub["run_number"], y=sub["duration"],
                mode="markers", marker=dict(color=color, size=10, symbol="circle"),
                name=status.capitalize(),
                text=sub["run_id"],
                hovertemplate="%{text}<br>Duration: %{y:.0f}s<extra></extra>",
            ))

    fig.update_layout(
        paper_bgcolor="#1F2937", plot_bgcolor="#1F2937",
        font=dict(color="#E5E7EB", family="JetBrains Mono"),
        xaxis=dict(title="Run #", gridcolor="#374151", tickfont=dict(color="#9CA3AF")),
        yaxis=dict(title="Duration (s)", gridcolor="#374151", tickfont=dict(color="#9CA3AF")),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=40, r=20, t=20, b=40),
        height=320,
    )
    st.plotly_chart(fig, width="stretch")

    # Run table
    display_cols = [c for c in ["run_id", "logical_date", "start_time", "end_time", "duration_seconds", "status", "triggered_by"] if c in df.columns]
    st.dataframe(df[display_cols], hide_index=True, width="stretch")
