"""Typed environment variable loader."""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

AGENT_MODEL: str = os.getenv("AGENT_MODEL", "gemini-2.5-pro")
GOOGLE_CLOUD_PROJECT: str = os.getenv("GOOGLE_CLOUD_PROJECT", "")
GOOGLE_CLOUD_LOCATION: str = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")

# Project charged for BigQuery slot usage — may differ from the data projects being queried.
# Falls back to the first entry in BQ_ALLOWED_PROJECTS if not set.
BQ_BILLING_PROJECT: str = os.getenv("BQ_BILLING_PROJECT", "")

BQ_ALLOWED_PROJECTS: list[str] = [
    p.strip() for p in os.getenv("BQ_ALLOWED_PROJECTS", "").split(",") if p.strip()
]

GIT_MODE: str = os.getenv("GIT_MODE", "api")
GIT_API_BASE_URL: str = os.getenv("GIT_API_BASE_URL", "https://api.github.com")
GIT_REPO: str = os.getenv("GIT_REPO", "")
GIT_BRANCH: str = os.getenv("GIT_BRANCH", "main")
GIT_API_TOKEN: str = os.getenv("GIT_API_TOKEN", "")
GIT_FILE_EXTENSIONS: list[str] = [
    e.strip() for e in os.getenv("GIT_FILE_EXTENSIONS", ".sql,.py,.yaml").split(",") if e.strip()
]
GIT_ROOT_PATHS: list[str] = [
    p.strip() for p in os.getenv("GIT_ROOT_PATHS", "dags/,sql/,config/").split(",") if p.strip()
]

GCS_BUCKETS: list[str] = [
    b.strip() for b in os.getenv("GCS_BUCKETS", "").split(",") if b.strip()
]
GCS_PREFIXES: list[str] = [
    p.strip() for p in os.getenv("GCS_PREFIXES", "").split(",") if p.strip()
]
GCS_FILE_EXTENSIONS: list[str] = [
    e.strip() for e in os.getenv("GCS_FILE_EXTENSIONS", ".sql,.py,.yaml").split(",") if e.strip()
]

# DAG_FOLDER: root folder(s) in GCS/Git where DAG Python files live (supports subfolders).
# Comma-separated. Falls back to GCS_PREFIXES if not set.
DAG_FOLDER: str = os.getenv("DAG_FOLDER", "")

# Schema Audit — MySQL-to-BigQuery reconciliation
SCHEMA_METADATA_PROJECT: str = os.getenv("SCHEMA_METADATA_PROJECT", "")
SCHEMA_BQ_PROJECT_PROD: str  = os.getenv("SCHEMA_BQ_PROJECT_PROD", "")
SCHEMA_BQ_PROJECT_UAT: str   = os.getenv("SCHEMA_BQ_PROJECT_UAT", "")
SCHEMA_HEADER_VIEW: str      = os.getenv("SCHEMA_HEADER_VIEW", "")
SCHEMA_DETAIL_VIEW: str      = os.getenv("SCHEMA_DETAIL_VIEW", "")
SCHEMA_AUDIT_OUTPUT_DIR: str = os.getenv("SCHEMA_AUDIT_OUTPUT_DIR", str(Path(__file__).parent.parent / "exports"))

DATA_ROOT: str = os.getenv("DATA_ROOT", str(Path(__file__).parent.parent / "data"))
USER_DATA_ROOT: str = os.getenv("USER_DATA_ROOT", str(Path(__file__).parent.parent / "user_data"))
EXPORTS_ROOT: str = os.getenv("EXPORTS_ROOT", str(Path(__file__).parent.parent / "exports"))
AUDIT_LOG_PATH: str = os.getenv("AUDIT_LOG_PATH", str(Path(__file__).parent.parent / "audit_log.jsonl"))
LARGE_FILE_ROW_THRESHOLD: int = int(os.getenv("LARGE_FILE_ROW_THRESHOLD", "50000"))
RECONCILIATION_CACHE_TTL_MINUTES: int = int(os.getenv("RECONCILIATION_CACHE_TTL_MINUTES", "30"))


def _parse_composer_envs() -> dict[str, str]:
    raw = os.getenv("COMPOSER_ENVS", "")
    result = {}
    for part in raw.split(","):
        part = part.strip()
        if ":" in part:
            name, url = part.split(":", 1)
            result[name.strip()] = url.strip()
    return result


COMPOSER_ENVS: dict[str, str] = _parse_composer_envs()


def get_composer_sdk_info(env_name: str) -> dict:
    prefix = f"COMPOSER_{env_name.upper()}"
    return {
        "airflow_version": os.getenv(f"{prefix}_AIRFLOW_VERSION", "2.6.3"),
        "bq_sdk": os.getenv(f"{prefix}_BQ_SDK", "google-cloud-bigquery==3.11.0"),
        "python_version": os.getenv(f"{prefix}_PYTHON_VERSION", "3.10"),
    }
