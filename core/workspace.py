"""Pinned workspace state."""
from core import persistence


def get_pinned_workspace() -> dict:
    return persistence.get_workspace_pins()


def set_pinned_workspace(
    composer_env: str = None,
    dag_id: str = None,
    bq_project: str = None,
) -> None:
    current = dict(get_pinned_workspace())
    if composer_env is not None:
        if composer_env:
            current["composer_env"] = composer_env
        else:
            current.pop("composer_env", None)
    if dag_id is not None:
        if dag_id:
            current["dag_id"] = dag_id
        else:
            current.pop("dag_id", None)
    if bq_project is not None:
        if bq_project:
            current["bq_project"] = bq_project
        else:
            current.pop("bq_project", None)
    persistence.save_workspace_pins(current)
