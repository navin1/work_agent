## OSR Data Intelligence — Run Book

Ask questions in plain English. The agent plans, calls the right tools, and returns a synthesised answer.

---

### Excel / Mapping Data

| Example | What it does |
|---|---|
| `What mapping files are loaded?` | Lists all ingested Excel files with their BQ table references and DAG associations |
| `Query the rps800_mapping table where status = active` | Runs DuckDB SQL across Excel data |
| `Which mapping files link to DAG dag_rps800_daily?` | Looks up DAG associations from registry |
| `Get the BQ table for mapping file reconciliation.xlsx` | Returns the target BigQuery table |

---

### BigQuery

| Example | What it does |
|---|---|
| `List datasets in project-a` | Browses available datasets |
| `Show tables in project-a.my_dataset` | Lists tables with row counts |
| `How many orders last month?` | Runs a SELECT query |
| `Get stats for BQ job abc-123` | Fetches bytes scanned, slot_ms, cache hit |

**Rules:** SELECT only · no automatic LIMIT · no DDL/DML

---

### Cloud Composer / Airflow

| Example | What it does |
|---|---|
| `List DAGs in prod` | Shows all DAGs with schedule and pause state |
| `Show run history for dag_rps800_load in prod` | Returns last 10 runs with durations |
| `Get error logs for dag_rps800_load run 2024-01-01` | Fetches failed task logs |
| `Get the SQL from task load_orders in dag_rps800_load` | Extracts and formats Jinja-rendered SQL |
| `Show task performance for dag_rps800_load` | Performance matrix with health status |
| `Compare current dag_rps800_load to last snapshot` | Diff against stored weekly snapshot |

---

### SQL Optimisation

| Example | What it does |
|---|---|
| `Find performance issues in: SELECT * FROM orders` | AST-based flag analysis |
| `Optimise the SQL from task load_orders` | AI optimisation + validation (always runs all three steps) |
| `Optimise all SQLs in dag_rps800_load` | Bulk optimise every task's SQL in one call |
| `Validate that the optimised SQL matches the original output` | SHA-256 row hash comparison |

**Guarantee:** Optimised SQL is never shown without a SAFE/UNSAFE verdict.

---

### File Optimisation (SQL / Python / DAG)

| Example | What it does |
|---|---|
| `Optimise gs://my-bucket/sql/rps800/load.sql` | Fetch from GCS, AI-optimise, download result |
| `Optimise dags/dag_rps800_load.py` | Fetch from Git, optimise DAG structure + Python code |
| `Optimise sql/rps285/weekly_summary.sql` | Git path, SQL performance optimisation |
| `Optimise /home/user/sql/load.sql` | Local absolute path |
| `Optimise ./sql/rps800/load.sql` | Local relative path |
| `Optimise all files in gs://my-bucket/sql/rps800/` | Bulk optimise entire GCS folder → zip download |
| `Optimise all files in sql/rps800/` | Bulk optimise entire Git folder → zip download |
| `Optimise all files in ./sql/` | Bulk optimise local folder → zip download |

**Guarantee:** Optimisation NEVER changes functional output, business logic, column names, or return values — performance and best practices only.

**DAG optimisation** (`optimise_dag`) additionally generates a `dag_doc_md` Markdown block that wires into the Airflow UI **Details** tab and **Graph** view header. It includes:
- Overview of the DAG's purpose
- Control-M job / folder / server references
- Impacted BigQuery tables and views

**Output:** Single file → saved to `exports/` with a **Download** button. Folder → zip archive with **Download All** button.

---

### File Browser

| Example | What it does |
|---|---|
| `List files in gs://my-bucket/sql/rps800/` | Shows a clickable file table for a GCS prefix |
| `Browse the dags/ folder in Git` | Lists files and sub-folders in the configured Git repo |
| `What files are in gs://my-bucket/dags/` | Same as browse — lists files at the given GCS location |

**Click any file name** in the browser table to load its contents into a Monaco editor with syntax highlighting. Supports CSV (shown as a data table), SQL, Python, YAML, JSON, Markdown, and more.

**Note:** To view a *specific* file (with a known path and extension), say `show` or `read` rather than `list` — the agent will fetch it directly without listing the folder first.

---

### Git vs GCS Comparison

| Example | What it does |
|---|---|
| `Compare deployed code in dags/ folder` | Shows files only in Git, only in GCS, and drifted |
| `Compare dags/dag_rps800_load.py between Git and GCS` | Line-by-line diff for a single file |
| `What SQL files in sql/rps800/ have drifted from Git?` | Content-diff for every file in the folder |

**Status types:** `only_in_git` (not deployed) · `only_in_gcs` (removed from Git) · `different` (content drift) · `identical` (in sync)

---

### Schema Audit (MySQL → BigQuery)

| Example | What it does |
|---|---|
| `Run schema audit` | Full MySQL→BQ column reconciliation for all streamed tables |
| `Audit the schema` | Same — produces colour-coded Excel + DDL JSON for prod and UAT |

**Status types:** 🟢 Match · 🟡 Type Mismatch · 🟠 BQ Only (extra column in BQ) · 🔵 MySQL Only (column missing from BQ)

**Output:** Per-batch colour-coded Excel (Summary sheet + one sheet per table) and BigQuery DDL JSON — both available as downloads.

**Required .env vars:** `SCHEMA_METADATA_PROJECT`, `SCHEMA_HEADER_VIEW`, `SCHEMA_DETAIL_VIEW`
**Optional:** `SCHEMA_BQ_PROJECT_PROD`, `SCHEMA_BQ_PROJECT_UAT`

---

### Schema Introspection

| Example | What it does |
|---|---|
| `Introspect schema of project-a.dataset.orders` | Full recursive BQ schema with dot-paths |

---

### Lineage Graph

| Example | What it does |
|---|---|
| `Show lineage for RPS800 mapping file` | Interactive flow diagram: Excel → DAG(s) → Tasks → SQL |
| `Trace the rps800_mapping file` | Same — click any node to see rendered SQL or execution details |

**Nodes:**
- 📊 **Excel** — file name, BQ table target
- ⚙ **DAG** — last run state, time, duration; click for full run history table
- 🗄 **Task** — operator, state, duration; click for rendered SQL in Monaco
- 📝 **SQL** — SQL file for a task; click to view full rendered SQL

**Features:** Zoom · pan · minimap · fit-to-view · last execution time on each DAG node

---

### Reconciliation

| Example | What it does |
|---|---|
| `Run reconciliation on RPS800` | Three-way Git vs GCS vs Mapping comparison |
| `Show detail for load_orders reconciliation` | Diff, mapping, history in one panel |
| `Acknowledge drift in load_orders — intentional refactor` | Marks finding as acknowledged |

**Status types:** `in_sync` · `content_drift` · `not_deployed` · `no_source` · `mapping_ghost` · `bq_missing` · `undeclared`

---

### Workspace & Memory

| Example | What it does |
|---|---|
| `Use prod environment from now on` | Pins composer_env=prod for all future calls |
| `Use project-a as my default BQ project` | Pins bq_project |
| `Save this query as "monthly orders"` | Persists to saved_queries.json |
| `Define RPS800 as Revenue Processing System 800` | Adds glossary term for auto-expansion |

---

### Tips
- Include table names, DAG IDs, or project names when you know them
- Ask follow-up questions — the agent remembers the last 20 exchanges
- Every answer cites which tool produced which part
- Suggested prompts appear when the chat is empty

---

## Architecture Overview *(developer reference)*

```
app.py                    ← Streamlit entrypoint, session state, chat loop,
                            renderer dispatcher (dispatch_renderers)
agent/
  agent.py                ← LangGraph ReAct agent builder + run_agent()
  system_prompt.py        ← Dynamic prompt (workspace, glossary, loaded tables)
  preprocessor.py         ← Glossary expansion before prompt hits the LLM
tools/
  __init__.py             ← ALL_TOOLS registry (manual — must be kept in sync)
  bigquery_tools.py       ← BQ query, dataset/table list, job stats
  browse_tools.py         ← browse_gcs, browse_git, fetch helpers
  code_tools.py           ← read_file, compare_git_gcs, optimise_file/folder
  composer_tools.py       ← 12 Airflow tools: DAGs, runs, tasks, SQL, logs
  excel_tools.py          ← DuckDB ingest, query, registry, lineage trace
  optimizer_tools.py      ← SQL flags, optimise_sql, optimise_dag, optimise_all
  reconciliation_tools.py ← Three-way Git/GCS/mapping reconciliation
  schema_tools.py         ← BQ schema introspection, MySQL→BQ schema audit
  testing_tools.py        ← compare_query_outputs, validate_optimisation
  user_tools.py           ← Saved queries, glossary, workspace pin, favorites
core/
  config.py               ← All env vars and constants
  auth.py                 ← GCP credential provider
  persistence.py          ← JSON-backed registry, glossary, saved queries
  duckdb_manager.py       ← Singleton DuckDB connection
  workspace.py            ← Pinned workspace read/write
  audit.py                ← Structured audit log per tool call
  llm.py                  ← LLM client factory
  monaco.py               ← Monaco editor HTML builder
  sql_formatter.py        ← SQL pretty-printer
  json_utils.py           ← safe_json serialiser
renderers/
  results_table.py        ← DAG list, task SQL, BQ/Excel query results
  optimised_file_viewer.py← Diff viewer, DAG doc_md panel, file content, folder
  lineage_graph.py        ← Streamlit-flow lineage graph
  file_browser.py         ← GCS/Git file browser with click-to-view
  diff_viewer.py          ← Inline SQL before/after diff
  reconciliation_panel.py ← Reconciliation findings UI
  schema_audit_panel.py   ← Schema audit colour-coded results
  schema_tree.py          ← BQ schema tree viewer
  performance_matrix.py   ← Task performance heat-map
  run_history_chart.py    ← DAG run history chart
  validation_panel.py     ← Optimisation validation verdict
```

### Known architectural debt (address as scope grows)

| Issue | Impact | Fix |
|---|---|---|
| `dispatch_renderers` is a 30-branch `if`-chain in `app.py` | Every new tool adds 3 lines; order encodes implicit priority | Convert to `RENDERER_MAP = {"tool_name": fn}` dict |
| `tools/__init__.py` is manually maintained | New tool requires 2 edits; easy to drift | `@register_tool` decorator or auto-discovery |
| `composer_tools.py` at 1 177 lines, 12 tools | Hard to navigate, slow to load | Split: `composer_dags.py` / `composer_jobs.py` / `composer_logs.py` |
| `app.py` mixes CSS, session state, sidebar, chat, dispatch | Growing file, hard to test | Split: `ui/sidebar.py` · `ui/chat.py` · `ui/dispatcher.py` |
| System prompt is one 231-line string | Domain rules buried; hard to update one area | Compose from `_bq_rules()`, `_composer_rules()`, `_optimisation_rules()` |
| No automated tests | Regressions silently break tool output shapes | Add `tests/` with unit tests for tool output parsers and renderers |
