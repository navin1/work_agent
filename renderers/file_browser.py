"""File browser renderer — GCS and Git.

Shows a clickable file listing. Clicking a file name fetches and displays
the file content in a Monaco editor (language auto-detected from extension).
"""
import json
import io

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

    if not files:
        st.info("No files at this level (only sub-folders).")
        return

    # ── Keys for session-state-based selection ────────────────────────────────
    # Include a hash of display_path so multiple browser instances don't collide.
    _key_tag = f"{source}_{abs(hash(display_path))}"
    sel_key  = f"fb_selected_{_key_tag}"

    # ── File list header ──────────────────────────────────────────────────────
    st.caption(f"📄 {len(files)} file(s) — click a file name to view its contents")

    h1, h2, h3 = st.columns([5, 1, 2])
    h1.markdown("**File**")
    h2.markdown("**Size**")
    h3.markdown("**Modified**")

    # ── File rows — each name is a button ─────────────────────────────────────
    for f in files:
        c1, c2, c3 = st.columns([5, 1, 2])
        with c1:
            btn_key = f"fb_btn_{_key_tag}_{f['path']}"
            if st.button(f"📄 {f['name']}", key=btn_key, use_container_width=True):
                st.session_state[sel_key] = f
                # Clear any cached content so a fresh fetch always runs
                content_key = f"fb_content_{_key_tag}_{f['path']}"
                st.session_state.pop(content_key, None)
        with c2:
            st.caption(_fmt_size(f.get("size")))
        with c3:
            st.caption(_fmt_date(f.get("updated") or f.get("sha", "")[:7] or None))

    # ── Content viewer ────────────────────────────────────────────────────────
    selected_item = st.session_state.get(sel_key)
    if not selected_item:
        return

    file_name   = selected_item["name"]
    content_key = f"fb_content_{_key_tag}_{selected_item['path']}"

    st.divider()
    st.markdown(f"#### 📄 `{file_name}`")

    if content_key not in st.session_state:
        with st.spinner(f"Loading {file_name}…"):
            try:
                st.session_state[content_key] = _fetch_content(source, selected_item, bucket)
            except Exception as exc:
                st.session_state[content_key] = f"__ERROR__: {exc}"

    content = st.session_state[content_key]

    if content.startswith("__ERROR__:"):
        st.error(content[len("__ERROR__:"):].strip())
        return

    if file_name.endswith(".csv"):
        try:
            import pandas as pd
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

    _safe_key = selected_item["path"].replace("/", "_")

    col_dl, col_opt, _ = st.columns([1, 1, 5])
    with col_dl:
        st.download_button(
            "⬇ Download",
            data=content.encode("utf-8"),
            file_name=file_name,
            mime="text/plain",
            key=f"dl_fb_{_key_tag}_{_safe_key}",
        )

    with col_opt:
        _ext = file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
        if _ext in ("sql", "py", "yaml", "yml", "sh"):
            if source == "gcs":
                _file_ref = selected_item.get("gcs_path") or f"gs://{bucket}/{selected_item['path']}"
            else:
                _file_ref = selected_item["path"]
            _prompt = (
                f"Optimise the SQL file at {_file_ref}"
                if _ext == "sql"
                else f"Optimise the file {_file_ref}"
            )
            if st.button("⚡ Optimise", key=f"opt_fb_{_key_tag}_{_safe_key}"):
                st.session_state.chat_prefill = _prompt
                st.rerun()
