"""Typed environment variable loader."""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env", override=True)

AGENT_MODEL: str = os.getenv("AGENT_MODEL", "gemini-2.5-pro")
GOOGLE_CLOUD_PROJECT: str = os.getenv("GOOGLE_CLOUD_PROJECT", "")
GOOGLE_CLOUD_LOCATION: str = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
# Path to a GCP service-account JSON file. When set, credentials are loaded from this file
# instead of relying on Application Default Credentials (gcloud auth / metadata server).
GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

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

# SSL verification for outbound HTTP requests.
# Set to "false" to disable (corporate proxy), or a path to a CA bundle file.
def _parse_ssl_verify() -> bool | str:
    val = os.getenv("HTTP_SSL_VERIFY", "true").strip().lower()
    if val == "false":
        return False
    if val != "true":
        return val  # treat as CA bundle path
    return True

HTTP_SSL_VERIFY: bool | str = _parse_ssl_verify()

DATA_ROOT: str = os.getenv("DATA_ROOT", str(Path(__file__).parent.parent / "data"))
USER_DATA_ROOT: str = os.getenv("USER_DATA_ROOT", str(Path(__file__).parent.parent / "user_data"))
CONFIG_ROOT: str = os.getenv("CONFIG_ROOT", str(Path(__file__).parent.parent / "config"))
EXPORTS_ROOT: str = os.getenv("EXPORTS_ROOT", str(Path(__file__).parent.parent / "exports"))
AUDIT_LOG_PATH: str = os.getenv("AUDIT_LOG_PATH", str(Path(__file__).parent.parent / "audit_log.jsonl"))
LARGE_FILE_ROW_THRESHOLD: int = int(os.getenv("LARGE_FILE_ROW_THRESHOLD", "50000"))
RECONCILIATION_CACHE_TTL_MINUTES: int = int(os.getenv("RECONCILIATION_CACHE_TTL_MINUTES", "30"))

# Control-M / Confluence integration
CONFLUENCE_BASE_URL: str = os.getenv("CONFLUENCE_BASE_URL", "")
CONTROLM_FOLDER: str     = os.getenv("CONTROLM_FOLDER", "")
CONTROLM_SERVER: str     = os.getenv("CONTROLM_SERVER", "")

# Fallback SDK versions used when no composer_env is provided to optimisation tools.
DEFAULT_AIRFLOW_VERSION: str = os.getenv("DEFAULT_AIRFLOW_VERSION", "2.6.3")
DEFAULT_BQ_SDK: str          = os.getenv("DEFAULT_BQ_SDK", "google-cloud-bigquery==3.11.0")
DEFAULT_PYTHON_VERSION: str  = os.getenv("DEFAULT_PYTHON_VERSION", "3.10")


def get_default_sdk_info() -> dict:
    """Fallback SDK info dict for tools that don't receive a composer_env."""
    return {
        "airflow_version": DEFAULT_AIRFLOW_VERSION,
        "bq_sdk": DEFAULT_BQ_SDK,
        "python_version": DEFAULT_PYTHON_VERSION,
    }


def _parse_composer_envs() -> dict[str, dict]:
    """Parse COMPOSER_ENVS into a dict of config entries.

    Supported formats (per entry):
      alias:https://airflow-url              — URL provided directly
      alias:project/location/env-name        — fetch everything from Composer API
    """
    raw = os.getenv("COMPOSER_ENVS", "")
    result = {}
    for part in raw.split(","):
        part = part.strip()
        if ":" not in part:
            continue
        alias, value = part.split(":", 1)
        alias = alias.strip()
        value = value.strip()
        if value.startswith("http"):
            result[alias] = {"mode": "url", "airflow_url": value.rstrip("/")}
        else:
            segments = value.split("/")
            if len(segments) == 3:
                result[alias] = {"mode": "api", "project": segments[0], "location": segments[1], "env_name": segments[2]}
    return result


COMPOSER_ENVS: dict[str, dict] = _parse_composer_envs()

_composer_info_cache: dict[str, dict] = {}


def get_composer_info(env_name: str) -> dict:
    """Fetch and cache Composer environment info.

    For 'api' mode: calls the Cloud Composer Management API to get the Airflow URL,
    Airflow version, Python version, and installed PyPI packages.
    For 'url' mode: uses the URL directly; version info is unavailable.

    Returns dict with: airflow_url, airflow_version, python_version, bq_sdk.
    On API failure, _error contains the exception message.
    """
    if env_name in _composer_info_cache:
        return _composer_info_cache[env_name]

    entry = COMPOSER_ENVS.get(env_name)
    if not entry:
        raise ValueError(f"Composer env '{env_name}' not found. Available: {list(COMPOSER_ENVS)}")

    if entry["mode"] == "url":
        info = {
            "airflow_url": entry["airflow_url"],
            "airflow_version": "unknown",
            "python_version": "unknown",
            "bq_sdk": "google-cloud-bigquery",
        }
        _composer_info_cache[env_name] = info
        return info

    # mode == "api"
    project, location, name = entry["project"], entry["location"], entry["env_name"]
    try:
        from google.cloud.orchestration.airflow.service_v1 import EnvironmentsClient
        from core.auth import get_credentials
        creds, _ = get_credentials()
        client = EnvironmentsClient(credentials=creds)
        env = client.get_environment(
            name=f"projects/{project}/locations/{location}/environments/{name}"
        )
        sc = env.config.software_config
        pypi = dict(sc.pypi_packages)
        bq_pkg = next(
            (f"{k}{v}" for k, v in pypi.items() if "google-cloud-bigquery" in k),
            "google-cloud-bigquery",
        )
        info = {
            "airflow_url": env.config.airflow_uri.rstrip("/"),
            "airflow_version": sc.airflow_version,
            "python_version": sc.python_version,
            "bq_sdk": bq_pkg,
        }
    except Exception as exc:
        info = {
            "airflow_url": None,
            "airflow_version": "unknown",
            "python_version": "unknown",
            "bq_sdk": "google-cloud-bigquery",
            "_error": str(exc),
        }

    _composer_info_cache[env_name] = info
    return info


def get_composer_sdk_info(env_name: str) -> dict:
    info = get_composer_info(env_name)
    return {
        "airflow_version": info["airflow_version"],
        "bq_sdk": info["bq_sdk"],
        "python_version": info["python_version"],
    }
