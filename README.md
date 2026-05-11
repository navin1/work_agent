# EDA OSR Data Intelligence Platform

A conversational AI platform for data engineers. Ask questions in plain English about BigQuery, Cloud Composer DAGs, Excel mapping files, and data pipelines — the agent plans, calls the right skill, and returns a synthesised answer with supporting evidence.

## Stack

| Layer | Technology |
|---|---|
| UI | Streamlit |
| Agent | Gemini (Vertex AI) via LangChain + custom skill kernel |
| Local query | DuckDB (Excel/mapping files ingested on startup) |
| Data warehouse | Google BigQuery |
| Orchestration | Cloud Composer / Airflow |
| Source control | GitHub API + local git |
| File storage | Google Cloud Storage |

## Setup

```bash
# 1. Clone and create a virtual environment
python -m venv .venv && source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure
cp .env.example .env
# Edit .env — minimum required vars listed below

# 4. Authenticate with GCP
gcloud auth application-default login

# 5. Run
streamlit run app.py
```

### Minimum required `.env` vars

| Variable | Purpose |
|---|---|
| `AGENT_MODEL` | Gemini model ID (e.g. `gemini-2.5-flash`) |
| `GOOGLE_CLOUD_PROJECT` | GCP project for Vertex AI |
| `GOOGLE_CLOUD_LOCATION` | Vertex AI location (e.g. `global`) |
| `GOOGLE_GENAI_USE_VERTEXAI` | Set to `TRUE` |
| `BQ_BILLING_PROJECT` | Project charged for BQ slot usage |
| `BQ_ALLOWED_PROJECTS` | Comma-separated projects the agent may read |
| `COMPOSER_ENVS` | Named Composer environments (see `.env.example`) |

See `.env.example` for the full list including Git, GCS, schema audit, and local-code validation vars.

## What it can do

| Capability | Example prompt |
|---|---|
| **Mapping validation** | `Validate mapping rules for rps800_reconciliation` |
| **BigQuery** | `How many orders last month in project-a.sales.orders?` |
| **Cloud Composer** | `Show run history for dag_rps800_load in prod` |
| **SQL optimisation** | `Optimise all SQLs in dag_rps800_load` |
| **File browser** | `List files in gs://my-bucket/sql/rps800/` |
| **Git ↔ GCS diff** | `Compare deployed code in the dags/ folder` |
| **Schema audit** | `Run schema audit` |
| **Lineage graph** | `Show lineage for RPS800 mapping file` |
| **Reconciliation** | `Run reconciliation on RPS800` |

See [runbook.md](runbook.md) for the full prompt reference, parameter details, and verdict/confidence explanations.

## Architecture

```
app.py                   Streamlit UI + renderer dispatcher
kernel_bootstrap.py      Wires all skills into a Kernel instance
kernel.py                Registry, invoke(), LLM dispatch loop
base.py                  BaseSkill / BaseInput / BaseOutput contracts
skills/
  primitives/            Internal helpers (SQL, LLM, Excel) — not LLM targets
  domain/                LLM dispatch targets (12 skills covering all capabilities)
tools/                   Legacy @tool functions — primitives delegate here
core/                    Config, auth, persistence, DuckDB, system prompt
renderers/               Per-result-type Streamlit panels
config/
  excel_mapping.json     Excel file → BQ table / DAG / column-role config
user_data/               Persisted glossary, saved queries, validation cache
```

Dispatch flow: user message → glossary expansion → LLM picks a domain skill → skill invoked → follow-up LLM call produces the chat answer → `app.py` routes the structured result to the matching renderer.

## Data layout

```
data/
  mapping/   Excel mapping files (one subfolder per system, e.g. RPS800/)
  master/    Master reference Excel files
exports/     Generated outputs (optimised files, schema audit Excel/JSON)
```

## Key config file

`config/excel_mapping.json` maps each Excel file stem to its BigQuery tables and DAG names, and lets you override which column plays which role (target, source, logic, bq_table). See the *Excel / Mapping Data* section of [runbook.md](runbook.md) for the schema.
