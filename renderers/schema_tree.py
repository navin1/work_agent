"""BigQuery schema tree renderer with collapsible expanders and Monaco JSON view."""
import json
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from core import monaco


def _render_node(node: dict, depth: int = 0) -> None:
    name = node.get("name", "?")
    ftype = node.get("field_type", "")
    mode = node.get("mode", "")
    children = node.get("fields", [])
    path = node.get("path", name)
    label = f"{'  ' * depth}**{name}** `{ftype}`" + (f" `{mode}`" if mode else "")
    if children:
        with st.expander(label, expanded=depth < 2):
            for child in children:
                _render_node(child, depth + 1)
    else:
        st.markdown(f"{'&nbsp;' * (depth * 4)}{label} — `{path}`", unsafe_allow_html=True)


def render(raw_json: str) -> None:
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse schema.")
        return

    if "error" in data:
        st.error(f"Schema error: {data['error']}")
        return

    table_name = data.get("table") or data.get("table_id") or "Schema"
    tree = data.get("schema_tree", [])
    leaves = data.get("flat_fields", [])

    st.subheader(f"Schema: {table_name}")
    if data.get("row_count") is not None:
        st.caption(f"{data['row_count']:,} rows · {data.get('field_count', len(leaves))} fields")

    tab1, tab2, tab3 = st.tabs(["Tree", "Flat Fields", "Raw JSON"])

    with tab1:
        for node in tree:
            _render_node(node)

    with tab2:
        if leaves:
            df = pd.DataFrame(leaves)
            st.dataframe(df, hide_index=True, use_container_width=True)
        else:
            st.info("No leaf fields available.")

    with tab3:
        json_str = json.dumps(data, indent=2, default=str)
        components.html(monaco.editor(json_str, language="json", height=400), height=420)
