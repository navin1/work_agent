"""MappingSkill — validate Excel mapping rules against DAG SQL.

Orchestrates ExcelSkill → SQLSkill → LLMSkill.
Registered as a domain skill; the kernel dispatches here via LLM tool calling.

Flow:
  1. ExcelSkill  → load file, resolve column roles, get configured BQ tables + DAG names
  2. SQLSkill    → fetch DAG SQL, deconstruct, build annotated SQL with SOURCE_FILE headers
  3. Rule parse  → extract + classify rules from DuckDB table (no LLM needed)
  4. LLMSkill    → bulk-evaluate rules against annotated SQL (once per BQ group)
  5. Assemble    → merge LLM verdicts + DML-confirmed file fallback into final output
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Literal

from pydantic import Field

from base import BaseInput, BaseOutput, BaseSkill
from skills.primitives.excel_skill import ExcelIngestInput, ExcelIngestOutput
from skills.primitives.llm_skill import LLMInput
from skills.primitives.sql_skill import SQLFetchInput, SQLFetchOutput


# ── Input / Output ────────────────────────────────────────────────────────────

class MappingInput(BaseInput):
    """Validate Excel mapping/transformation rules against DAG SQL implementation.

    Use when the user wants to check, validate, or verify that mapping rules,
    business logic, or transformation rules in an Excel file are correctly
    implemented in a DAG's SQL code.
    """
    mapping_file: str = Field(..., description="Excel mapping file name (.xlsx or stem)")
    dag_id: str = Field(..., description="Airflow DAG ID to validate against")
    source_mode: Literal["local", "git", "composer"] = Field(
        "git", description="Where to fetch DAG SQL from"
    )
    task_id: str | None = Field(None, description="Validate one task only; all tasks if omitted")
    target_column_filter: str | None = Field(
        None, description="Filter rules to a specific target column name"
    )
    force_refresh: bool = Field(False, description="Bypass the validation result cache")
    composer_env: str | None = Field(None, description="Composer environment alias (composer mode)")
    git_ref: str | None = Field(None, description="Git branch or commit SHA (git mode)")
    git_repo_path: str | None = Field(None, description="Local git repo path override")
    local_dag_path: str | None = Field(None, description="Local DAG root path override")


class RuleResult(BaseOutput):
    rule_id: int | str
    target_columns: list[str]
    source_columns: list[str]
    rule_text: str
    rule_type: str
    confidence_tier: str
    verdict: str
    reason: str
    evidence: str
    sql_file: str
    match_type: str
    flags: list[str]
    cache_hit: bool


class BQGroup(BaseOutput):
    bq_table: str
    rule_count: int
    rules: list[RuleResult]


class MappingOutput(BaseOutput):
    mapping_file: str
    dag_id: str | None
    source_mode: str
    summary: dict
    bq_table_groups: list[BQGroup]
    sql_structure: dict
    error: str | None = None


# ── Helpers (rule parsing — no LLM, deterministic) ───────────────────────────

_NA_PATTERN = re.compile(
    r"^\s*(n/?a|not\s+applicable|tbd|to\s+be\s+defined|"
    r"populated\s+by\s+upstream|upstream|same\s+as\s+source|"
    r"direct\s+copy|direct|as[\s\-]is|none|null|-+)\s*$",
    re.IGNORECASE,
)

_DML_REGEX = re.compile(
    r'\b(INSERT|MERGE|UPDATE|DELETE|TRUNCATE|CREATE(\s+OR\s+REPLACE)?\s+TABLE)\b',
    re.IGNORECASE,
)


def _classify_rule(rule_text: str, target_cols: list[str]) -> tuple[str, str]:
    text = rule_text.lower()
    if len(target_cols) > 2 or any(kw in text for kw in ["allocat", "proportion", "weight", "distribut"]):
        return "complex_allocation", "LOW"
    if any(kw in text for kw in ["join", "lookup", "link", "match to", "relate"]):
        if any(kw in text for kw in ["sum", "avg", "count", "group", "aggregat", "total"]):
            return "join_aggregation", "MEDIUM"
        return "join", "HIGH"
    if any(kw in text for kw in ["sum", "total", "aggregat", "count", "average", "avg", "max", "min"]):
        return "aggregation", "MEDIUM"
    if any(kw in text for kw in ["filter", "exclude", "where", "only", "except", "null", "not null"]):
        return "filter_condition", "MEDIUM"
    if any(kw in text for kw in ["if ", "then ", "else ", "case ", "when "]):
        return "conditional", "HIGH"
    if any(kw in text for kw in ["direct", "copy", "same as", "as is", "rename", "map directly"]):
        return "direct_mapping", "HIGH"
    return "transformation", "MEDIUM"


def _build_validation_prompt(
    rules: list[dict],
    annotated_sql: str,
    jinja_note: str,
) -> str:
    prompt_rules = [
        {
            "rule_id": r["rule_id"],
            "target": r["target_columns"],
            "source": r["source_columns"],
            "rule_text": r["rule_text"],
        }
        for r in rules if not r.get("_na")
    ]
    note_section = f"NOTE: {jinja_note}\n\n" if jinja_note else ""
    return (
        "You are a Data QA Engineer. The SQL below consists of one or more scripts, "
        "each preceded by a SOURCE_FILE comment identifying its origin file. "
        "Validate every business rule against the SQL.\n\n"
        f"{note_section}"
        "FULL SQL SCRIPT:\n```sql\n"
        f"{annotated_sql}\n"
        "```\n\n"
        "BUSINESS RULES TO VALIDATE:\n```json\n"
        f"{json.dumps(prompt_rules)}\n"
        "```\n\n"
        "Return a JSON array with one object per rule. Required keys:\n"
        "[\n"
        '  {"rule_id": "<from input>", "verdict": "PASS|FAIL|PARTIAL",\n'
        '   "reason": "1-2 sentences",\n'
        '   "evidence": "exact SQL snippet or empty string",\n'
        '   "source_file": "<value of the nearest SOURCE_FILE header above the evidence, or empty string>",\n'
        '   "flags": []}\n'
        "]"
    )


def _master_files(
    bq_label: str,
    task_sqls: dict[str, str],
    task_files: dict[str, str],
    file_stem: str,
) -> tuple[list[str], str]:
    """DML-confirmed file fallback when the LLM returns no source_file."""
    from tools.mapping_validation_tools import get_search_variants, is_valid_fqn

    if is_valid_fqn(bq_label):
        variants = get_search_variants(bq_label)
        short = variants[-1].lower()
    else:
        short = file_stem.lower()
        variants = []

    found: set[str] = set()
    match_type = "Unresolved"

    if variants:
        for tid, content in task_sqls.items():
            if _DML_REGEX.search(content) and any(
                re.search(rf'\b{re.escape(v)}\b', content, re.IGNORECASE) for v in variants
            ):
                fval = task_files.get(tid, "")
                parts = [fp for fp in fval.split(", ") if fp]
                narrowed = [fp for fp in parts if short in Path(fp).stem.lower()] if len(parts) > 1 else parts
                found.update(narrowed)
        if found:
            match_type = "Direct"

    if not found and short:
        for fval in task_files.values():
            for fp in fval.split(", "):
                if fp and short in Path(fp).stem.lower():
                    found.add(fp)
        if found:
            match_type = "Filename"

    return sorted(found), match_type


# ── Skill ─────────────────────────────────────────────────────────────────────

class MappingSkill(BaseSkill):
    name = "MappingSkill"
    description = MappingInput.__doc__.strip()
    InputModel = MappingInput
    OutputModel = MappingOutput
    dispatch_key = "validate_mapping_rules"

    async def execute(self, input: MappingInput) -> MappingOutput:
        # ── 1. Ingest Excel ───────────────────────────────────────────────────
        excel: ExcelIngestOutput = await self.kernel.invoke(
            "ExcelSkill",
            ExcelIngestInput(file_name=input.mapping_file),
        )
        if excel.error:
            return MappingOutput(
                mapping_file=input.mapping_file, dag_id=input.dag_id,
                source_mode=input.source_mode, summary={}, bq_table_groups=[],
                sql_structure={}, error=excel.error,
            )

        resolved_dag_id = input.dag_id or (excel.dag_names[0] if excel.dag_names else None)

        # ── 2. Fetch SQL ──────────────────────────────────────────────────────
        sql_out: SQLFetchOutput = await self.kernel.invoke(
            "SQLSkill",
            SQLFetchInput(
                dag_id=resolved_dag_id or "",
                source_mode=input.source_mode,
                task_id=input.task_id,
                composer_env=input.composer_env,
                local_dag_path=input.local_dag_path,
                git_repo_path=input.git_repo_path,
                git_ref=input.git_ref,
            ),
        )
        has_sql = bool(sql_out.tasks_evaluated)

        # ── 3. Parse + classify rules ─────────────────────────────────────────
        raw_rules = self._parse_rules(excel)
        if input.target_column_filter:
            flt = input.target_column_filter.lower()
            raw_rules = [r for r in raw_rules if any(flt in c.lower() for c in r["target_columns"])]

        for rule in raw_rules:
            if not rule["rule_text"] or _NA_PATTERN.match(rule["rule_text"]):
                rule["_na"] = True
                rule["rule_type"] = "not_applicable"
                rule["confidence_tier"] = ""
            else:
                rtype, conf = _classify_rule(rule["rule_text"], rule["target_columns"])
                rule["rule_type"] = rtype
                rule["confidence_tier"] = conf
                rule["_na"] = False

        # ── 4. Group rules by BQ table ────────────────────────────────────────
        bq_groups: dict[str, list[dict]] = {}
        col_config = excel.col_config
        bqtable_col = col_config.get("bq_table")
        db_rows = self._load_rows(excel.table_name)

        for rule in raw_rules:
            bq_hint = rule.get("bq_table_hint")
            if bq_hint:
                bq_groups.setdefault(bq_hint, []).append(rule)
            elif excel.bq_tables:
                for bq in excel.bq_tables:
                    bq_groups.setdefault(bq, []).append(rule)
            else:
                bq_groups.setdefault("(no BQ table configured)", []).append(rule)

        # ── 5. Evaluate each group via LLM ────────────────────────────────────
        summary: dict = {
            "total": 0, "pass": 0, "fail": 0, "partial": 0,
            "not_applicable": 0, "not_evaluated": 0, "error": 0, "low_confidence": 0,
        }
        output_groups: list[BQGroup] = []

        for bq_label, group_rules in bq_groups.items():
            fallback_files, fallback_match = _master_files(
                bq_label, self._task_sqls(sql_out), sql_out.task_files, excel.file_stem
            )

            # Build per-group prompt + cache key
            prompt_rules_ids = [r["rule_id"] for r in group_rules if not r.get("_na")]
            bulk_verdicts: dict = {}

            if has_sql and prompt_rules_ids:
                prompt = _build_validation_prompt(group_rules, sql_out.annotated_sql, sql_out.jinja_note)
                rules_hash = hashlib.sha256(json.dumps(prompt_rules_ids).encode()).hexdigest()
                sql_hash = hashlib.sha256(sql_out.annotated_sql.encode()).hexdigest()
                cache_key = f"mapping:{rules_hash}:{sql_hash}"

                llm_out = await self.kernel.invoke(
                    "LLMSkill",
                    LLMInput(prompt=prompt, cache_key=cache_key, force_refresh=input.force_refresh),
                )
                if isinstance(llm_out.parsed_json, list):
                    for res in llm_out.parsed_json:
                        rid = res.get("rule_id")
                        if rid is not None:
                            try:
                                rid = int(rid)
                            except (TypeError, ValueError):
                                rid = str(rid)
                            bulk_verdicts[rid] = {
                                "verdict":     res.get("verdict", "PARTIAL"),
                                "reason":      res.get("reason", ""),
                                "evidence":    res.get("evidence", ""),
                                "source_file": res.get("source_file", ""),
                                "flags":       res.get("flags", []),
                                "cache_hit":   llm_out.cache_hit,
                            }

            # ── Assemble rule results ─────────────────────────────────────────
            evaluated: list[RuleResult] = []
            for rule in group_rules:
                summary["total"] += 1
                base = dict(
                    rule_id=rule["rule_id"],
                    target_columns=rule["target_columns"],
                    source_columns=rule["source_columns"],
                    rule_text=rule["rule_text"],
                    rule_type=rule.get("rule_type", ""),
                    confidence_tier=rule.get("confidence_tier", ""),
                    verdict="", reason="", evidence="",
                    sql_file="", match_type="",
                    flags=[], cache_hit=False,
                )

                if rule["_na"]:
                    base["verdict"] = "NOT_APPLICABLE"
                    base["reason"] = "Rule text indicates no transformation logic required."
                    base["match_type"] = "N/A"
                    summary["not_applicable"] += 1

                elif not has_sql:
                    base["verdict"] = "NOT_EVALUATED"
                    base["reason"] = sql_out.fetch_error or "No SQL available."
                    base["match_type"] = "N/A"
                    summary["not_evaluated"] += 1

                else:
                    vdata = bulk_verdicts.get(rule["rule_id"]) or bulk_verdicts.get(str(rule["rule_id"]))
                    if vdata:
                        base.update(vdata)
                        v = base["verdict"].lower().replace(" ", "_")
                        if v in summary:
                            summary[v] += 1
                        if rule.get("confidence_tier") == "LOW":
                            summary["low_confidence"] += 1
                    else:
                        base["verdict"] = "ERROR"
                        base["reason"] = "LLM omitted this rule from its response."
                        summary["error"] += 1

                    # LLM-attributed file → DML-confirmed fallback → Unresolved
                    llm_file = base.pop("source_file", "")
                    if llm_file:
                        base["sql_file"] = llm_file
                        base["match_type"] = "LLM-attributed"
                    elif fallback_files:
                        base["sql_file"] = ", ".join(fallback_files)
                        base["match_type"] = fallback_match
                    else:
                        base["sql_file"] = ""
                        base["match_type"] = "Unresolved"

                evaluated.append(RuleResult(**base))

            output_groups.append(BQGroup(bq_table=bq_label, rule_count=len(evaluated), rules=evaluated))

        merged = sql_out.merged_structure
        return MappingOutput(
            mapping_file=input.mapping_file,
            dag_id=resolved_dag_id,
            source_mode=input.source_mode,
            summary=summary,
            bq_table_groups=output_groups,
            sql_structure={
                "tasks_evaluated": sql_out.tasks_evaluated,
                "cte_count":       len(merged.get("ctes", {})),
                "join_count":      len(merged.get("joins", [])),
                "parse_errors":    [
                    s["parse_error"]
                    for s in sql_out.structures.values()
                    if s.get("parse_error")
                ],
            },
        )

    # ── Private helpers ───────────────────────────────────────────────────────

    def _load_rows(self, table_name: str) -> list[dict]:
        from core.duckdb_manager import get_manager
        try:
            df = get_manager().execute(f"SELECT * FROM {table_name}")
            return df.to_dict("records")
        except Exception:
            return []

    def _task_sqls(self, sql_out: SQLFetchOutput) -> dict[str, str]:
        """Reconstruct task_sqls from structures for DML-scanning in _master_files."""
        return {
            tid: s.get("raw_sql", "")
            for tid, s in sql_out.structures.items()
        }

    def _parse_rules(self, excel: ExcelIngestOutput) -> list[dict]:
        """Extract and merge multi-row rules from the DuckDB table."""
        from core.duckdb_manager import get_manager

        col = excel.col_config
        target_col   = col.get("target")
        source_col   = col.get("source")
        logic_col    = col.get("logic")
        supp_cols    = col.get("logic_supplementary") or []
        bqtable_col  = col.get("bq_table")
        multirow_col = col.get("multi_row_key") or target_col
        ruleid_col   = col.get("rule_id")

        try:
            df = get_manager().execute(f"SELECT * FROM {excel.table_name}")
        except Exception:
            return []

        rows = df.to_dict("records")

        def _cell(row: dict, c: str | None) -> str:
            if c is None:
                return ""
            v = row.get(c)
            return "" if v is None or (isinstance(v, float) and str(v) == "nan") else str(v).strip()

        group_map: dict[str, dict] = {}
        for idx, row in enumerate(rows):
            group_key = _cell(row, multirow_col) or _cell(row, target_col) or str(idx)
            if group_key not in group_map:
                logic_parts = [_cell(row, logic_col)] if logic_col else []
                for sc in supp_cols:
                    v = _cell(row, sc)
                    if v:
                        logic_parts.append(v)
                group_map[group_key] = {
                    "rule_id":       _cell(row, ruleid_col) if ruleid_col else "",
                    "target_columns": [t.strip() for t in _cell(row, target_col).split(",") if t.strip()],
                    "source_columns": [s.strip() for s in _cell(row, source_col).split(",") if s.strip()],
                    "logic_parts":    logic_parts,
                    "bq_table_hint":  _cell(row, bqtable_col) if bqtable_col else None,
                }
            else:
                # Continuation row — append extra logic parts
                if logic_col:
                    v = _cell(row, logic_col)
                    if v:
                        group_map[group_key]["logic_parts"].append(v)

        rules = []
        for fallback_idx, (gkey, grp) in enumerate(group_map.items(), start=1):
            try:
                rid: int | str = int(grp["rule_id"]) if grp["rule_id"] else fallback_idx
            except ValueError:
                rid = grp["rule_id"] or fallback_idx

            rules.append({
                "rule_id":        rid,
                "target_columns": grp["target_columns"],
                "source_columns": grp["source_columns"],
                "rule_text":      " | ".join(grp["logic_parts"]),
                "bq_table_hint":  grp.get("bq_table_hint"),
            })

        return rules
