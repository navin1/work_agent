# OSR Data Intelligence Platform

Run: `streamlit run app.py`
Stack: Python, Gemini (Vertex AI), LangChain, Streamlit, DuckDB, Pydantic

## Architecture

Two layers coexist during migration:

**Skills layer** (new — being built):
`base.py` → `kernel.py` → `skills/primitives/` → `skills/domain/`
Every new capability goes here. See @SKILLS.md and @KERNEL.md.

**Tools layer** (existing — do not extend):
`tools/*.py` — 35 LangChain `@tool` functions used by the original agent.
Primitives are allowed to import internal functions from `tools/` to avoid duplication.

## Key rules

- New capabilities → skills architecture only, never new `@tool` functions
- Primitives call other primitives via `kernel.invoke()`, not direct imports
- Domain skill `InputModel` docstrings and field descriptions must be written for the LLM,
  not for the engineer — they become the tool schema the LLM reads during dispatch
- `KernelContext` holds only credentials + config, nothing else

## Config

Copy `.env.example` to `.env`. Key vars: `AGENT_MODEL`, `GOOGLE_CLOUD_PROJECT`,
`GOOGLE_CLOUD_LOCATION`, `LOCAL_GIT_REPO_PATH`, `LOCAL_DAG_ROOT`.

@SKILLS.md
@KERNEL.md
