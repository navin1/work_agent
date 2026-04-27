"""GCS and Git file browser tools."""
import json
import base64
import time

from langchain.tools import tool
from core import config
from core.json_utils import safe_json
from core.audit import log_audit


# ── GCS ───────────────────────────────────────────────────────────────────────

@tool
def browse_gcs(path: str) -> str:
    """List files and sub-folders at a GCS path.

    path: 'gs://bucket-name/some/prefix'  or  'bucket-name/some/prefix'.
    Returns a file listing with name, size, and last-modified for each entry.
    The agent should call this when the user asks to list or browse files in GCS."""
    start = time.time()
    try:
        from google.cloud import storage
        from core.auth import get_credentials

        raw = path.strip()
        if raw.startswith("gs://"):
            raw = raw[5:]
        bucket_name, _, prefix = raw.partition("/")
        prefix = prefix.strip("/")
        list_prefix = (prefix + "/") if prefix else ""

        creds, _ = get_credentials()
        client = storage.Client(credentials=creds)

        iterator = client.list_blobs(bucket_name, prefix=list_prefix, delimiter="/")
        files = []
        for blob in iterator:
            rel = blob.name[len(list_prefix):]
            if not rel:
                continue
            files.append({
                "name":     rel,
                "path":     blob.name,
                "gcs_path": f"gs://{bucket_name}/{blob.name}",
                "size":     blob.size,
                "updated":  blob.updated.isoformat() if blob.updated else None,
                "type":     "file",
            })

        dirs = []
        for pfx in (iterator.prefixes or []):
            rel = pfx[len(list_prefix):]
            dirs.append({
                "name":     rel,
                "path":     pfx,
                "gcs_path": f"gs://{bucket_name}/{pfx}",
                "size":     None,
                "updated":  None,
                "type":     "dir",
            })

        log_audit("browse_tools", "gcs", path,
                  row_count=len(files) + len(dirs),
                  duration_ms=int((time.time() - start) * 1000))
        return safe_json({
            "source":  "gcs",
            "bucket":  bucket_name,
            "prefix":  list_prefix,
            "display_path": path,
            "items":   dirs + files,
        })
    except Exception as exc:
        return safe_json({"error": str(exc)})


def fetch_gcs_file(bucket_name: str, blob_path: str) -> str:
    """Download a single GCS blob and return its text content."""
    from google.cloud import storage
    from core.auth import get_credentials
    creds, _ = get_credentials()
    client = storage.Client(credentials=creds)
    return client.bucket(bucket_name).blob(blob_path).download_as_text(encoding="utf-8")


# ── Git ───────────────────────────────────────────────────────────────────────

@tool
def browse_git(path: str) -> str:
    """List files and sub-folders at a path in the configured Git repository.

    path: folder path such as 'dags/', 'dags/subfolder', 'sql/raw-zone'.
    Returns a file listing with name, size, and sha for each entry.
    The agent should call this when the user asks to list or browse files in Git."""
    start = time.time()
    try:
        if not config.GIT_API_TOKEN or not config.GIT_REPO:
            return safe_json({"error": "Git not configured (GIT_REPO / GIT_API_TOKEN missing)."})

        import requests
        import warnings
        import urllib3
        if config.HTTP_SSL_VERIFY is False:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            warnings.filterwarnings("ignore", message="Unverified HTTPS request")

        session = requests.Session()
        session.verify = config.HTTP_SSL_VERIFY
        session.headers.update({
            "Authorization": f"token {config.GIT_API_TOKEN}",
            "Accept":        "application/vnd.github.v3+json",
        })

        clean_path = path.strip("/")
        url = f"{config.GIT_API_BASE_URL}/repos/{config.GIT_REPO}/contents/{clean_path}"
        resp = session.get(url, params={"ref": config.GIT_BRANCH}, timeout=20)
        resp.raise_for_status()
        entries = resp.json()

        if isinstance(entries, dict):
            # Single file returned — wrap it
            entries = [entries]

        items = [
            {
                "name":         e.get("name", ""),
                "path":         e.get("path", ""),
                "type":         e.get("type", "file"),   # "file" | "dir"
                "size":         e.get("size"),
                "sha":          e.get("sha", ""),
                "download_url": e.get("download_url"),
            }
            for e in entries
            if isinstance(e, dict)
        ]
        # Dirs first, then files
        items.sort(key=lambda x: (0 if x["type"] == "dir" else 1, x["name"].lower()))

        log_audit("browse_tools", "git", path,
                  row_count=len(items),
                  duration_ms=int((time.time() - start) * 1000))
        return safe_json({
            "source":       "git",
            "repo":         config.GIT_REPO,
            "branch":       config.GIT_BRANCH,
            "path":         clean_path,
            "display_path": f"{config.GIT_REPO}/{clean_path}@{config.GIT_BRANCH}",
            "items":        items,
        })
    except Exception as exc:
        return safe_json({"error": str(exc)})


def fetch_git_file(file_path: str) -> str:
    """Download a single file from the configured Git repository."""
    import requests
    import warnings
    import urllib3
    if config.HTTP_SSL_VERIFY is False:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    session = requests.Session()
    session.verify = config.HTTP_SSL_VERIFY
    session.headers.update({
        "Authorization": f"token {config.GIT_API_TOKEN}",
        "Accept":        "application/vnd.github.v3.raw",
    })
    url = f"{config.GIT_API_BASE_URL}/repos/{config.GIT_REPO}/contents/{file_path}"
    resp = session.get(url, params={"ref": config.GIT_BRANCH}, timeout=30)
    resp.raise_for_status()

    # With Accept: .raw the body is plain text; fall back to base64 JSON if needed
    ct = resp.headers.get("Content-Type", "")
    if "json" in ct:
        data = resp.json()
        if isinstance(data, dict) and data.get("encoding") == "base64":
            return base64.b64decode(data["content"].replace("\n", "")).decode("utf-8", errors="replace")
    return resp.text
