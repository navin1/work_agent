"""File browser renderer — GCS and Git.

Shows a clickable file listing table. Selecting a row fetches and displays
the file content in a Monaco editor (language auto-detected from extension).
"""
import json
import io

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from core import monaco

# ── Language detection ────────────────────────────────────────────────────────

_EXT_LANG = {
    ".py":    "python",
    ".sql":   "sql",
    ".yaml":  "yaml",
    ".yml":   "yaml",
    ".json":  "json",
    ".sh":    "shell",
    ".bash":  "shell",
    ".md":    "markdown",
    ".txt":   "plaintext",
    ".csv":   "plaintext",
    ".xml":   "xml",
    ".html":  "html",
    ".js":    "javascript",
    ".ts":    "typescript",
}


def _lang(filename: str) -> str:
    from pathlib import Path
    return _EXT_LANG.get(Path(filename).suffix.lower(), "plaintext")


def _fmt_size(size) -> str:
    if size is None:
        return "—"
    if size < 1024:
        return f"{size} B"
    if size < 1024 ** 2:
        return f"{size / 1024:.1f} KB"
    return f"{size / 1024 ** 2:.1f} MB"


def _fmt_date(iso: str | None) -> str:
    if not iso:
        return "—"
    return iso[:10]


# ── Content fetcher ───────────────────────────────────────────────────────────

def _fetch_content(source: str, item: dict, bucket: str) -> str:
    if source == "gcs":
        from tools.browse_tools import fetch_gcs_file
        return fetch_gcs_file(bucket, item["path"])
    else:
        from tools.browse_tools import fetch_git_file
        return fetch_git_file(item["path"])


# ── Main renderer ─────────────────────────────────────────────────────────────

def render_file_browser(raw_json: str) -> None:
    try:
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except Exception:
        st.error("Could not parse file browser result.")
        return

    if "error" in data:
        st.error(f"Browse error: {data['error']}")
        return

    source       = data.get("source", "")
    display_path = data.get("display_path", data.get("path", ""))
    items        = data.get("items", [])
    bucket       = data.get("bucket", "")

    icon = "🪣" if source == "gcs" else "🐙"
    st.markdown(f"#### {icon} `{display_path}`")

    if not items:
        st.info("No files found at this path.")
        return

    files = [i for i in items if i.get("type") == "file"]
    dirs  = [i for i in items if i.get("type") != "file"]

    # ── Directory listing ─────────────────────────────────────────────────────
    if dirs:
        st.caption(f"📁 {len(dirs)} folder(s)")
        for d in dirs:
            st.markdown(f"📁 `{d['name']}`")

    # ── File table with row selection ─────────────────────────────────────────
    if not files:
        st.info("No files at this level (only sub-folders).")
        return

    st.caption(f"📄 {len(files)} file(s) — click a row to view contents")

    rows = [
        {
            "File":      f["name"],
            "Size":      _fmt_size(f.get("size")),
            "Modified":  _fmt_date(f.get("updated") or f.get("sha", "")[:7] or None),
        }
        for f in files
    ]
    df = pd.DataFrame(rows)

    browser_key = f"fb_sel_{source}_{display_path}"

    sel = st.dataframe(
        df,
        hide_index=True,
        use_container_width=True,
        on_select="rerun",
        selection_mode="single-row",
        key=browser_key,
    )

    selected_rows = sel.selection.rows if hasattr(sel, "selection") else []
    if not selected_rows:
        st.caption("No file selected — click a row to view its contents.")
        return

    selected_item = files[selected_rows[0]]
    file_name     = selected_item["name"]
    cache_key     = f"fb_content_{source}_{selected_item['path']}"

    st.divider()
    st.markdown(f"#### 📄 `{file_name}`")

    if cache_key not in st.session_state:
        with st.spinner(f"Loading {file_name}…"):
            try:
                st.session_state[cache_key] = _fetch_content(source, selected_item, bucket)
            except Exception as exc:
                st.session_state[cache_key] = f"# Error loading file: {exc}"

    content = st.session_state[cache_key]

    if content.startswith("# Error"):
        st.error(content)
        return

    # CSV → dataframe; everything else → Monaco editor
    if file_name.endswith(".csv"):
        try:
            df_csv = pd.read_csv(io.StringIO(content))
            st.caption(f"{len(df_csv):,} rows × {len(df_csv.columns)} columns")
            st.dataframe(df_csv, hide_index=True, use_container_width=True)
        except Exception:
            st.code(content[:5000], language="plaintext")
    else:
        lang   = _lang(file_name)
        lines  = content.count("\n") + 1
        height = min(max(300, lines * 18 + 40), 900)
        components.html(monaco.editor(content, language=lang, height=height), height=height + 20)

    st.download_button(
        "⬇ Download",
        data=content.encode("utf-8"),
        file_name=file_name,
        mime="text/plain",
        key=f"dl_fb_{source}_{selected_item['path'].replace('/', '_')}",
    )
