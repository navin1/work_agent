"""Monaco Editor HTML generators for read-only and diff views."""

_CDN = "https://cdn.jsdelivr.net/npm/monaco-editor@0.45.0/min/vs"


def _clean(code: str) -> str:
    """Strip non-standard whitespace that causes visible artefacts in Monaco."""
    from core.sql_formatter import _normalize_sql
    return _normalize_sql(code)


def editor(code: str, language: str = "sql", height: int = 400) -> str:
    code = _clean(code)
    escaped = code.replace("\\", "\\\\").replace("`", "\\`").replace("${", "\\${")
    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body {{ margin: 0; padding: 0; background: #1e1e1e; }}
  #container {{ width: 100%; height: {height}px; }}
</style>
</head>
<body>
<div id="container"></div>
<script src="{_CDN}/loader.js"></script>
<script>
require.config({{ paths: {{ vs: '{_CDN}' }} }});
require(['vs/editor/editor.main'], function() {{
  monaco.editor.create(document.getElementById('container'), {{
    value: `{escaped}`,
    language: '{language}',
    theme: 'vs-dark',
    readOnly: true,
    minimap: {{ enabled: false }},
    scrollBeyondLastLine: false,
    fontFamily: 'JetBrains Mono, monospace',
    fontSize: 13,
    lineNumbers: 'on',
    wordWrap: 'on',
    automaticLayout: true,
  }});
}});
</script>
</body>
</html>"""


def diff_editor(original: str, modified: str, language: str = "sql", height: int = 500) -> str:
    original = _clean(original)
    modified = _clean(modified)
    esc_orig = original.replace("\\", "\\\\").replace("`", "\\`").replace("${", "\\${")
    esc_mod = modified.replace("\\", "\\\\").replace("`", "\\`").replace("${", "\\${")
    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body {{ margin: 0; padding: 0; background: #1e1e1e; }}
  #controls {{
    background: #252526;
    border-bottom: 1px solid #3c3c3c;
    padding: 6px 12px;
    display: flex;
    gap: 8px;
    align-items: center;
  }}
  #controls button {{
    background: #3c3c3c;
    border: 1px solid #555;
    color: #ccc;
    padding: 3px 10px;
    border-radius: 4px;
    cursor: pointer;
    font-size: 12px;
    font-family: 'JetBrains Mono', monospace;
  }}
  #controls button.active {{
    background: #E05C00;
    border-color: #E05C00;
    color: #fff;
  }}
  #container {{ width: 100%; height: {height}px; }}
</style>
</head>
<body>
<div id="controls">
  <button id="btn-split" class="active" onclick="setMode(false)">Split</button>
  <button id="btn-unified" onclick="setMode(true)">Unified</button>
</div>
<div id="container"></div>
<script src="{_CDN}/loader.js"></script>
<script>
var diffEditor;
require.config({{ paths: {{ vs: '{_CDN}' }} }});
require(['vs/editor/editor.main'], function() {{
  var orig = monaco.editor.createModel(`{esc_orig}`, '{language}');
  var mod  = monaco.editor.createModel(`{esc_mod}`, '{language}');
  diffEditor = monaco.editor.createDiffEditor(document.getElementById('container'), {{
    theme: 'vs-dark',
    readOnly: true,
    renderSideBySide: true,
    minimap: {{ enabled: false }},
    fontFamily: 'JetBrains Mono, monospace',
    fontSize: 13,
    scrollBeyondLastLine: false,
    automaticLayout: true,
  }});
  diffEditor.setModel({{ original: orig, modified: mod }});
}});
function setMode(unified) {{
  if (!diffEditor) return;
  diffEditor.updateOptions({{ renderSideBySide: !unified }});
  document.getElementById('btn-split').className = unified ? '' : 'active';
  document.getElementById('btn-unified').className = unified ? 'active' : '';
}}
</script>
</body>
</html>"""
