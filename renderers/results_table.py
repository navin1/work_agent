"""Results table renderer with export buttons and explain action."""
import json
import io
import pandas as pd
import streamlit as st


def render(raw_json: str, agent=None) -> None:
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse query result.")
        return

    if "error" in data:
        st.error(f"Query error: {data['error']}")
        return

    columns = data.get("columns", [])
    rows = data.get("rows", [])
    row_count = data.get("row_count", len(rows))
    stats = data.get("stats", {})

    if not columns:
        st.info("No data returned.")
        return

    df = pd.DataFrame(rows, columns=columns)

    # Stats row
    stat_parts = [f"**{row_count:,}** rows"]
    if stats.get("bytes_processed"):
        mb = stats["bytes_processed"] / 1_000_000
        stat_parts.append(f"**{mb:.1f} MB** scanned")
    if stats.get("cache_hit"):
        stat_parts.append("cache hit ✓")
    if stats.get("execution_time_ms"):
        stat_parts.append(f"**{stats['execution_time_ms']}ms**")
    st.caption("  ·  ".join(stat_parts))

    st.dataframe(df, hide_index=True, use_container_width=True)

    # Export buttons
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        csv_buf = io.StringIO()
        df.to_csv(csv_buf, index=False)
        st.download_button("⬇ CSV", csv_buf.getvalue(), "results.csv", mime="text/csv", key=f"csv_{id(raw_json)}")
    with col2:
        st.download_button("⬇ JSON", json.dumps({"columns": columns, "rows": rows}, default=str),
                           "results.json", mime="application/json", key=f"json_{id(raw_json)}")
    with col3:
        if agent and st.button("🤖 Explain this result", key=f"explain_{id(raw_json)}"):
            sample = df.head(5).to_dict(orient="records")
            from agent.agent import run_agent
            result = run_agent(agent, f"Explain this query result: {row_count} rows returned. Sample data: {json.dumps(sample, default=str)}")
            st.markdown(result.get("output", ""))
