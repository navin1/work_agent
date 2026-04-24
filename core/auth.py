"""GCP credentials loader."""
import os
from core import config

try:
    import google.auth
    from google.oauth2 import service_account
    _HAS_GOOGLE_AUTH = True
except ImportError:
    _HAS_GOOGLE_AUTH = False


def get_credentials(scopes=None):
    if not _HAS_GOOGLE_AUTH:
        return None, None
    if scopes is None:
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    sa_path = config.GOOGLE_APPLICATION_CREDENTIALS
    if sa_path and os.path.exists(sa_path):
        creds = service_account.Credentials.from_service_account_file(sa_path, scopes=scopes)
        project = creds.project_id if hasattr(creds, "project_id") else None
        return creds, project
    try:
        creds, project = google.auth.default(scopes=scopes)
        return creds, project
    except Exception:
        return None, None
