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

**Output:** Single file → saved to `exports/` with a **Download** button. Folder → zip archive with **Download All** button.

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
