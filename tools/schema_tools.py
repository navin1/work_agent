"""Schema introspection tools (BigQuery only)."""
import json
import time

from langchain.tools import tool

from core.audit import log_audit


def _bq_field_to_dict(field, prefix: str = "") -> dict:
    """Recursively convert a BQ schema field to a nested dict."""
    path = f"{prefix}{field.name}" if not prefix else f"{prefix}.{field.name}"
    result = {
        "name": field.name,
        "path": path,
        "field_type": field.field_type,
        "mode": field.mode,
        "description": field.description or "",
        "fields": [],
    }
    if field.field_type in ("RECORD", "STRUCT") and field.fields:
        for subfield in field.fields:
            result["fields"].append(_bq_field_to_dict(subfield, prefix=path))
    return result


def _flatten_fields(field_dict: dict, leaf_list: list) -> None:
    if not field_dict.get("fields"):
        leaf_list.append({
            "path": field_dict["path"],
            "field_type": field_dict["field_type"],
            "mode": field_dict["mode"],
        })
    else:
        for sub in field_dict["fields"]:
            _flatten_fields(sub, leaf_list)


@tool
def introspect_bq_schema(project_id: str, dataset_id: str, table_id: str) -> str:
    """Recursively fetch full BigQuery table schema to ALL nesting levels.
    NEVER truncates RECORD/STRUCT/REPEATED fields. Recurses until no further nesting exists.
    Returns JSON with nested tree AND flat list of all leaf fields with full dot-path."""
    start = time.time()
    try:
        from tools.bigquery_tools import _validate_project, _get_client
        err = _validate_project(project_id)
        if err:
            return json.dumps({"error": err})
        client = _get_client(project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        table = client.get_table(table_ref)

        tree = [_bq_field_to_dict(f) for f in table.schema]
        leaves: list[dict] = []
        for node in tree:
            _flatten_fields(node, leaves)

        result = {
            "table": table_ref,
            "row_count": table.num_rows,
            "size_bytes": table.num_bytes,
            "schema_tree": tree,
            "flat_fields": leaves,
            "field_count": len(leaves),
        }
        log_audit("schema_tools", table_ref, "introspect_bq_schema",
                  duration_ms=int((time.time()-start)*1000))
        return json.dumps(result, default=str)
    except Exception as exc:
        return json.dumps({"error": str(exc)})
