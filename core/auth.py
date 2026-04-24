"""GCP credentials loader — delegates entirely to ADC (google.auth.default)."""
try:
    import google.auth
    _HAS_GOOGLE_AUTH = True
except ImportError:
    _HAS_GOOGLE_AUTH = False


def get_credentials(scopes=None):
    if not _HAS_GOOGLE_AUTH:
        return None, None
    if scopes is None:
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    try:
        creds, project = google.auth.default(scopes=scopes)
        return creds, project
    except Exception:
        return None, None
