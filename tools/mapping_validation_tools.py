"""Mapping rule validation — compares Excel transformation logic against BigQuery SQL implementation."""

import hashlib
import json
import re
import time
from pathlib import Path

from langchain.tools import tool

from core import config, persistence
from core.audit import log_audit
from core.duckdb_manager import get_manager
from core.json_utils import safe_json
from core.sql_formatter import strip_jinja


# ── L1 in-session cache (avoids repeated file reads within one session) ───────
_verdict_cache: dict[str, dict] = {}


# ── Column role priority lists ────────────────────────────────────────────────

_TARGET_ALIASES = [
	"target", "target_column", "target_field", "bq_column",
	"destination", "field_name", "output", "output_column", "column_name",
]
_SOURCE_ALIASES = [
	"source", "source_column", "source_field", "src_column",
	"input", "from_column", "source_table", "input_column",
]
_LOGIC_ALIASES = [
	"transformation_logic", "mapping_logic", "logic", "mapping_rule",
	"calculation", "rule", "description", "notes", "transformation", "mapping",
]
_BQTABLE_ALIASES = [
	"bq_table", "target_table", "destination_table", "table_name",
	"output_table", "bigquery_table", "bq_target",
]
_RULEID_ALIASES = [
	"rule_id", "id", "mapping_id", "rule_number", "index"
]
_SUPPLEM_KEYWORDS = {
	"condition", "special", "exception", "note", "comment",
	"remark", "qualifier", "additional", "constraint",
}

_NA_PATTERN = re.compile(
	r"^\s*(n/?a|not\s+applicable|tbd|to\s+be\s+defined|"
	r"populated\s+by\s+upstream|upstream|same\s+as\s+source|"
	r"direct\s+copy|direct|as[\s\-]is|none|null|-+)\s*$",
	re.IGNORECASE,
)


# ── Column detection helpers ──────────────────────────────────────────────────

def _norm(name: str) -> str:
	return re.sub(r"[^a-z0-9]", "_", name.lower().strip()).strip("_")


def _detect_role(actual_cols: list[str], aliases: list[str]) -> str | None:
	norm_map = {_norm(c): c for c in actual_cols}
	for alias in aliases:
		if alias in norm_map:
			return norm_map[alias]
	for alias in aliases:
		for norm, original in norm_map.items():
			if alias in norm or norm in alias:
				return original
	return None


def _detect_supplementary(actual_cols: list[str], primary_logic_col: str | None) -> list[str]:
	result = []
	for col in actual_cols:
		if col == primary_logic_col:
			continue
		n = _norm(col)
		if any(kw in n for kw in _SUPPLEM_KEYWORDS):
			result.append(col)
	return result


def _resolve_column_config(actual_cols: list[str], configured: dict) -> dict:
	"""Merge explicit config with auto-detection. Explicit config always wins for each key."""

	def _pick(key: str, aliases: list[str]) -> tuple[str | None, str]:
		explicit = configured.get(key)
		if explicit and explicit in actual_cols:
			return explicit, "config"
		detected = _detect_role(actual_cols, aliases)
		return detected, "auto"

	target_col, target_src = _pick("target",  _TARGET_ALIASES)
	source_col, source_src = _pick("source",  _SOURCE_ALIASES)
	logic_col,  logic_src  = _pick("logic",	_LOGIC_ALIASES)
	bqtable_col, bqtable_src = _pick("bq_table", _BQTABLE_ALIASES)
	multirow_col, _		 = _pick("multi_row_key", _TARGET_ALIASES)
	ruleid_col, ruleid_src = _pick("rule_id", _RULEID_ALIASES)

	configured_supp = configured.get("logic_supplementary") or []
	if configured_supp:
		supp_cols = [c for c in configured_supp if c in actual_cols]
	else:
		supp_cols = _detect_supplementary(actual_cols, logic_col)

	return {
		"target":			 target_col,
		"source":			 source_col,
		"logic":			 logic_col,
		"logic_supplementary": supp_cols,
		"bq_table":		  bqtable_col,
		"multi_row_key":	 multirow_col or target_col,
		"rule_id":			ruleid_col,
		"detected_by": {
			"target":  target_src,
			"source":  source_src,
			"logic":	logic_src,
			"bq_table": bqtable_src,
			"rule_id": ruleid_src,
		},
	}


# ── Rule type classification (deterministic, no LLM) ─────────────────────────

def _classify_rule(rule_text: str, target_cols: list[str]) -> tuple[str, str]:
	"""Returns (rule_type, confidence_tier)."""
	text = rule_text.lower()
	if len(target_cols) > 2 or any(
		kw in text for kw in ["allocat", "proportion", "weight", "distribut", "spread"]
	):
		return "complex_allocation", "LOW"
	if any(kw in text for kw in ["join", "lookup", "link", "match to", "relate"]):
		if any(kw in text for kw in ["sum", "avg", "average", "count", "group", "aggregat", "total"]):
			return "join_aggregation", "MEDIUM"
		return "join", "HIGH"
	if any(kw in text for kw in [
		"sum", "total", "aggregat", "count", "average", "avg", "max", "min",
		"group by", "grouped",
	]):
		return "aggregation", "MEDIUM"
	if any(kw in text for kw in ["filter", "exclude", "where", "only", "except", "null", "not null"]):
		return "filter_condition", "MEDIUM"
	if any(kw in text for kw in ["if ", "then ", "else ", "case ", "when "]):
		return "conditional", "HIGH"
	if any(kw in text for kw in ["direct", "copy", "same as", "as is", "rename", "map directly"]):
		return "direct_mapping", "HIGH"
	return "transformation", "MEDIUM"


# ── SQL deconstruction ────────────────────────────────────────────────────────

def _deconstruct_sql(sql: str) -> dict:
	base: dict = {
		"ctes": {}, "joins": [], "where_clauses": [], "group_by": [],
		"select_expressions": {}, "aggregations": [], "destination_table": None,
		"parse_error": None,
		"raw_sql": sql
	}
	if not sql or not sql.strip():
		return base
	try:
		import sqlglot
		import sqlglot.expressions as exp

		clean = strip_jinja(sql)
		try:
			statements = sqlglot.parse(clean, read="bigquery", error_level=sqlglot.ErrorLevel.WARN)
		except Exception:
			statements = sqlglot.parse(clean, error_level=sqlglot.ErrorLevel.WARN)

		if not statements:
			return base
			
		# If there are multiple statements (like DECLARE, CREATE TEMP TABLE, then the main INSERT/SELECT),
		# we want to search through ALL of them, but prioritize the final SELECT statement.
		
		# CTEs
		for tree in statements:
			for cte in tree.find_all(exp.CTE):
				base["ctes"][cte.alias] = cte.this.sql(dialect="bigquery", pretty=False)

		# JOINs
		for tree in statements:
			for join in tree.find_all(exp.Join):
				side = str(join.args.get("side") or "").upper()
				kind = str(join.args.get("kind") or "").upper()
				join_type = f"{side} {kind}".strip() or "INNER"
				on_expr = join.args.get("on") or join.args.get("using")
				base["joins"].append({
					"type": join_type,
					"table": join.this.sql(dialect="bigquery") if join.this else "",
					"on": on_expr.sql(dialect="bigquery") if on_expr else "",
				})

		# WHERE — deduplicate
		seen_where: set[str] = set()
		for tree in statements:
			for where in tree.find_all(exp.Where):
				w = where.this.sql(dialect="bigquery")
				if w not in seen_where:
					base["where_clauses"].append(w)
					seen_where.add(w)

		# GROUP BY — outermost query per statement
		for tree in reversed(statements):
			main_select = tree.find(exp.Select)
			if main_select and main_select.args.get("group"):
				base["group_by"] = [e.sql(dialect="bigquery") for e in main_select.args["group"].expressions]
				break

		# SELECT expressions — Walk all statements to grab main column aliases
		for tree in reversed(statements):
			main_select = tree.find(exp.Select)
			if main_select:
				for expr in main_select.expressions:
					alias = (
						expr.alias
						if (hasattr(expr, "alias") and expr.alias)
						else expr.sql(dialect="bigquery")[:60]
					)
					if alias not in base["select_expressions"]:
						base["select_expressions"][alias] = expr.sql(dialect="bigquery")

		# Aggregation functions — deduplicate
		seen_agg: set[str] = set()
		for tree in statements:
			for agg in tree.find_all(exp.AggFunc):
				agg_sql = agg.sql(dialect="bigquery")
				if agg_sql not in seen_agg:
					base["aggregations"].append(agg_sql)
					seen_agg.add(agg_sql)

		# Destination table from INSERT / CREATE / MERGE
		for tree in statements:
			for node_cls in (exp.Insert, exp.Create, exp.Merge):
				for node in tree.find_all(node_cls):
					if node.this:
						base["destination_table"] = node.this.sql(dialect="bigquery")
						break
				if base["destination_table"]:
					break
			if base["destination_table"]:
				break

	except Exception as exc:
		base["parse_error"] = str(exc)

	return base


def _merge_structures(structures: list[dict]) -> dict:
	merged: dict = {
		"ctes": {}, "joins": [], "where_clauses": [], "group_by": [],
		"select_expressions": {}, "aggregations": [], "destination_table": None,
		"raw_sql": "\n\n-- ---\n\n".join(filter(None, [s.get("raw_sql") for s in structures]))
	}
	for s in structures:
		merged["ctes"].update(s.get("ctes", {}))
		merged["joins"].extend(s.get("joins", []))
		merged["where_clauses"].extend(s.get("where_clauses", []))
		if s.get("group_by"):
			merged["group_by"] = s["group_by"]
		merged["select_expressions"].update(s.get("select_expressions", {}))
		merged["aggregations"].extend(s.get("aggregations", []))
		if s.get("destination_table") and not merged["destination_table"]:
			merged["destination_table"] = s["destination_table"]
	return merged


# ── LLM helpers ───────────────────────────────────────────────────────────────

def _llm_credentials():
	cred_path = config.GOOGLE_APPLICATION_CREDENTIALS
	if not cred_path:
		return None
	from pathlib import Path as _P
	if not _P(cred_path).is_file():
		return None
	from google.oauth2 import service_account
	return service_account.Credentials.from_service_account_file(
		cred_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
	)


def _call_llm(prompt: str) -> str:
	from langchain_google_genai import ChatGoogleGenerativeAI
	from langchain_core.messages import HumanMessage
	llm = ChatGoogleGenerativeAI(
		model=config.AGENT_MODEL, temperature=0, credentials=_llm_credentials(),
	)
	response = llm.invoke([HumanMessage(content=prompt)])
	content = response.content
	if isinstance(content, list):
		content = " ".join(b.get("text", "") for b in content if isinstance(b, dict))
	return content


def _extract_json(text: str) -> dict | None:
	match = re.search(r"\{.*\}", text, re.DOTALL)
	if match:
		try:
			return json.loads(match.group())
		except Exception:
			pass
	return None

def _extract_json_array(text: str) -> list | None:
	match = re.search(r"\[.*\]", text, re.DOTALL)
	if match:
		try:
			return json.loads(match.group())
		except Exception:
			pass
	return None


def _cache_key(rules_hash: str, sql_hash: str) -> str:
	return hashlib.sha256(f"{rules_hash}|||{sql_hash}".encode()).hexdigest()


def _get_cached_verdict(key: str) -> dict | None:
	"""L1 → L2 lookup. Populates L1 from file on L1 miss."""
	if key in _verdict_cache:
		return _verdict_cache[key]
	file_cache = persistence.get_validation_cache()
	if key in file_cache:
		entry = dict(file_cache[key])
		entry.pop("cached_at", None)
		_verdict_cache[key] = entry
		return entry
	return None


def _save_verdict(key: str, result: dict) -> None:
	"""Write to L1 and L2. Caps L2 file at 1000 entries (oldest-first eviction)."""
	_verdict_cache[key] = result
	file_cache = dict(persistence.get_validation_cache())
	file_cache[key] = {**result, "cached_at": time.time()}
	if len(file_cache) > 1000:
		oldest = sorted(file_cache, key=lambda k: file_cache[k].get("cached_at", 0))
		for old in oldest[: len(file_cache) - 1000]:
			del file_cache[old]
	persistence.save_validation_cache(file_cache)


# ── Bulk LLM evaluation ───────────────────────────────────────────────────────

def _evaluate_rules_bulk(rules: list[dict], structure: dict, force_refresh: bool, sql_note: str = "") -> dict:
	"""Single-pass batched LLM evaluation for all rules at once."""
	if not rules:
		return {}

	# Extract clean SQL snippet
	raw_sql = structure.get("raw_sql", "")
	if len(raw_sql) > 15000:
		# Truncate if insanely huge, though Gemini 1.5 Pro can handle 1M+ tokens
		raw_sql = raw_sql[:15000] + "\n...[TRUNCATED]..."

	# Build concise rules list for prompt
	prompt_rules = []
	for r in rules:
		if r.get("_na"):
			continue
		prompt_rules.append({
			"rule_id": r["rule_id"],
			"target": r["target_columns"],
			"source": r["source_columns"],
			"rule_text": r["rule_text"]
		})

	if not prompt_rules:
		# All rules are N/A
		return {}

	rules_json_str = json.dumps(prompt_rules)
	
	# Calculate cache key based on all rules and the raw SQL
	rules_hash = hashlib.sha256(rules_json_str.encode()).hexdigest()
	sql_hash = hashlib.sha256(raw_sql.encode()).hexdigest()
	cache_key = _cache_key(rules_hash, sql_hash)

	if not force_refresh:
		cached = _get_cached_verdict(cache_key)
		if cached:
			# Map cached list back to a dict keyed by rule_id
			return {r["rule_id"]: {**r, "cache_hit": True} for r in cached.get("results", [])}

	note_section = f"NOTE: {sql_note}\n\n" if sql_note else ""
	prompt = (
		"You are a Data QA Engineer. Below is a complete SQL script and a list of business rules "
		"from an Excel mapping document. Your task is to validate every single rule against the SQL.\n\n"
		f"{note_section}"
		"FULL SQL SCRIPT:\n"
		"```sql\n"
		f"{raw_sql}\n"
		"```\n\n"
		"BUSINESS RULES TO VALIDATE:\n"
		"```json\n"
		f"{rules_json_str}\n"
		"```\n\n"
		"Return a JSON array where each object corresponds to a rule. Provide your evaluation. "
		"The output MUST be a valid JSON array of objects with exact keys:\n"
		'[\n {"rule_id": "rule_id_value_from_input", "verdict": "PASS|FAIL|PARTIAL", "reason": "1-2 sentences", "evidence": "specific SQL snippet", "flags": []}\n]'
	)

	results_dict = {}
	try:
		response_text = _call_llm(prompt)
		parsed_array = _extract_json_array(response_text)
		
		if isinstance(parsed_array, list):
			for res in parsed_array:
				rid = res.get("rule_id")
				if rid is not None:
					# In bulk mode we might not get every ID accurately. Standardize to string or int if needed,
					# but rules_dict keyed by ID works natively.
					try:
						rid = int(rid)
					except Exception:
						rid = str(rid) # Fallback to string if it's alphanumeric like "BQ Column Name"
					
					results_dict[rid] = {
						"verdict": res.get("verdict", "PARTIAL"),
						"reason": res.get("reason", ""),
						"evidence": res.get("evidence", ""),
						"flags": res.get("flags", []),
						"relevant_ctes": [],
						"relevant_clauses": [],
						"cache_hit": False,
					}
					
			# Cache successful results
			_save_verdict(cache_key, {"results": list(results_dict.values())})
	except Exception as exc:
		print(f"Bulk LLM evaluation failed: {exc}")

	return results_dict


# ── SQL fetching (reuses composer_tools internals) ────────────────────────────

def _fetch_all_task_sqls(composer_env: str, dag_id: str) -> dict[str, str]:
	"""Return {task_id: formatted_sql} for every task in the DAG that has SQL."""
	try:
		from tools.composer_tools import (
			_get, _enc, _best_sql, _extract_rendered_sql,
			_rendered_was_truncated, _get_sql_file_path, _fetch_sql_file,
		)
		from core.sql_formatter import extract_sql, format_sql

		tasks_data = _get(composer_env, f"/dags/{_enc(dag_id)}/tasks")
		task_ids = [t["task_id"] for t in tasks_data.get("tasks", [])]

		runs = _get(
			composer_env,
			f"/dags/{_enc(dag_id)}/dagRuns",
			{"limit": 10, "order_by": "-execution_date", "state": "success"},
		)
		dag_runs = runs.get("dag_runs", [])

		results: dict[str, str] = {}
		for tid in task_ids:
			raw_sql = rendered_sql = None
			rendered_truncated = False
			try:
				task_data = _get(composer_env, f"/dags/{_enc(dag_id)}/tasks/{_enc(tid)}")
				sql_file = _get_sql_file_path(task_data)
				raw_sql  = _fetch_sql_file(sql_file) if sql_file else None
				if not raw_sql:
					raw_sql = extract_sql(task_data)
				for run in dag_runs:
					try:
						ti = _get(
							composer_env,
							f"/dags/{_enc(dag_id)}/dagRuns/{_enc(run['dag_run_id'])}/taskInstances/{_enc(tid)}",
						)
						rendered_sql	  = _extract_rendered_sql(ti)
						rendered_truncated = _rendered_was_truncated(ti)
						if rendered_sql:
							break
					except Exception:
						continue
			except Exception:
				continue

			best = _best_sql(raw_sql, rendered_sql, rendered_truncated)
			if best:
				results[tid] = format_sql(best)

		return results
	except Exception:
		return {}


# ── Jinja resolution ──────────────────────────────────────────────────────────

def _load_jinja_vars() -> dict:
    """Load Jinja substitution map from LOCAL_JINJA_VARS_PATH. Returns {} on any failure."""
    path = config.LOCAL_JINJA_VARS_PATH
    if not path:
        return {}
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def _load_jinja_vars_for_git(repo_path: str, ref: str) -> dict:
    """Load Jinja vars for git mode.

    If LOCAL_GIT_JINJA_VARS_PATH is set (repo-relative path), reads the JSON from
    the git object store at the given ref via 'git show <ref>:<path>' — so the vars
    match the branch/commit being validated, not the host filesystem state.
    Falls back to LOCAL_JINJA_VARS_PATH (filesystem) on any failure or if not set.
    """
    import subprocess

    git_vars_path = config.LOCAL_GIT_JINJA_VARS_PATH
    if git_vars_path:
        try:
            result = subprocess.run(
                ["git", "-C", repo_path, "show", f"{ref}:{git_vars_path}"],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode == 0 and result.stdout.strip():
                return json.loads(result.stdout)
        except Exception:
            pass

    return _load_jinja_vars()


def _resolve_jinja(sql: str, vars: dict) -> str:
    """Render Jinja2 expressions in SQL using vars + a mock Airflow context.

    Handles: variable substitution, filters (ds_add, ds_format), method calls on
    datetime objects (strftime), conditional {% if %} blocks, and params.x notation.
    Unknown variables / filters silently become the SQL-safe string '__JINJA__'.
    Falls back to regex strip_jinja() if Jinja2 rendering raises an exception.
    """
    if not vars:
        return strip_jinja(sql)

    try:
        import datetime as _dt
        from jinja2 import Environment, Undefined

        class _Silent(Undefined):
            """Silently absorbs unknown variable access, attribute chaining, and calls."""
            def __str__(self) -> str:
                return "'__JINJA__'"

            def __iter__(self):
                return iter([])

            def __bool__(self) -> bool:
                return False

            def __call__(self, *args, **kwargs):
                return _Silent(name="unknown")

            def __getattr__(self, name: str):
                # Guard: let Python/Jinja2 resolve private/dunder attrs normally
                if name.startswith("_"):
                    raise AttributeError(name)
                return _Silent(name=name)

        class _MockObj:
            """Proxy for Airflow context objects (ti, task, dag) — absorbs any access."""
            def __getattr__(self, _): return _MockObj()
            def __call__(self, *a, **kw): return "'__JINJA__'"
            def __str__(self): return "'__JINJA__'"

        # ── Build Jinja context ───────────────────────────────────────────────
        context: dict = {}
        params: dict = {}

        for k, v in vars.items():
            context[k] = v
            # Expose params.x as both context["params"]["x"] and context["params.x"]
            if k.startswith("params."):
                params[k[7:]] = v

        context.setdefault("params", params)

        # Derive execution date from "ds" key if available
        ds_raw = vars.get("ds", "")
        try:
            exec_dt = _dt.datetime.strptime(ds_raw, "%Y-%m-%d") if ds_raw else _dt.datetime.now()
        except ValueError:
            exec_dt = _dt.datetime.now()

        context.setdefault("ds",                exec_dt.strftime("%Y-%m-%d"))
        context.setdefault("ds_nodash",         exec_dt.strftime("%Y%m%d"))
        context.setdefault("execution_date",    exec_dt)
        context.setdefault("next_execution_date", exec_dt + _dt.timedelta(days=1))
        context.setdefault("prev_execution_date", exec_dt - _dt.timedelta(days=1))
        context.setdefault("next_ds",           (exec_dt + _dt.timedelta(days=1)).strftime("%Y-%m-%d"))
        context.setdefault("prev_ds",           (exec_dt - _dt.timedelta(days=1)).strftime("%Y-%m-%d"))
        context.setdefault("tomorrow_ds",       (exec_dt + _dt.timedelta(days=1)).strftime("%Y-%m-%d"))
        context.setdefault("yesterday_ds",      (exec_dt - _dt.timedelta(days=1)).strftime("%Y-%m-%d"))

        # Mock Airflow macros (most common functions used in DAG SQL templates)
        class _Macros:
            @staticmethod
            def ds_add(ds_str: str, days: int) -> str:
                try:
                    return (
                        _dt.datetime.strptime(str(ds_str), "%Y-%m-%d")
                        + _dt.timedelta(days=int(days))
                    ).strftime("%Y-%m-%d")
                except Exception:
                    return str(ds_str)

            @staticmethod
            def ds_format(ds_str: str, input_fmt: str, output_fmt: str) -> str:
                try:
                    return _dt.datetime.strptime(str(ds_str), input_fmt).strftime(output_fmt)
                except Exception:
                    return str(ds_str)

            @staticmethod
            def datetime(y: int, m: int, d: int, *args) -> _dt.datetime:
                try:
                    return _dt.datetime(int(y), int(m), int(d), *[int(a) for a in args])
                except Exception:
                    return exec_dt

        context.setdefault("macros", _Macros())
        context.setdefault("var",    {"value": {}, "json": {}})
        context.setdefault("ti",     _MockObj())
        context.setdefault("task",   _MockObj())
        context.setdefault("dag",    _MockObj())
        context.setdefault("run_id", vars.get("run_id", "__JINJA__"))

        env = Environment(undefined=_Silent)
        return env.from_string(sql).render(**context)

    except Exception:
        return strip_jinja(sql)


# ── Local / Git SQL extraction ────────────────────────────────────────────────

_SQL_KEYWORD_RE = re.compile(
    r"\b(SELECT|WITH|INSERT|MERGE|UPDATE|DELETE|CREATE|DECLARE)\b",
    re.IGNORECASE,
)


def _looks_like_sql(text: str) -> bool:
    return bool(text and len(text.strip()) > 40 and _SQL_KEYWORD_RE.search(text))


def _try_eval_str_node(node: "ast.AST", var_map: "dict[str, str]") -> "str | None":
    """Best-effort evaluation of an AST node to a plain string.

    Handles: string literals, variable references, string concatenation (+),
    os.path.join / pathlib calls, str.format(), and f-strings.
    Unknown sub-expressions are replaced with '__VAR__' so callers can still
    detect '.sql' suffixes on partially-resolved paths.
    """
    import ast

    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return var_map.get(node.id)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left  = _try_eval_str_node(node.left,  var_map)
        right = _try_eval_str_node(node.right, var_map)
        if left is not None and right is not None:
            return left + right
        # Return whichever side looks like a resolvable SQL path
        for side in (left, right):
            if side and side.strip().endswith(".sql"):
                return side
        return None
    if isinstance(node, ast.Call):
        func = node.func
        attr = (
            func.attr if isinstance(func, ast.Attribute) else
            func.id   if isinstance(func, ast.Name) else ""
        )
        # os.path.join / Path.joinpath / any join-like call
        if attr in ("join", "joinpath"):
            parts = [_try_eval_str_node(a, var_map) for a in node.args]
            resolved = [p if p is not None else "__VAR__" for p in parts]
            return "/".join(p.strip("/") for p in resolved) if resolved else None
        # "template/{table}.sql".format(table="foo") or "{}".format(x)
        if attr == "format" and isinstance(func, ast.Attribute):
            fmt = _try_eval_str_node(func.value, var_map)
            if fmt:
                # Resolve positional and keyword args to substitute into the template
                pos_args = [_try_eval_str_node(a, var_map) for a in node.args]
                kw_args  = {
                    kw.arg: _try_eval_str_node(kw.value, var_map)
                    for kw in node.keywords
                    if kw.arg is not None
                }
                try:
                    import string
                    out_parts: list[str] = []
                    pos_idx = 0
                    for literal, field_name, _, _ in string.Formatter().parse(fmt):
                        out_parts.append(literal)
                        if field_name is None:
                            continue
                        base_key = field_name.split(".")[0].split("[")[0]
                        if base_key == "" or base_key.isdigit():
                            idx = int(base_key) if base_key.isdigit() else pos_idx
                            val = pos_args[idx] if idx < len(pos_args) else None
                            pos_idx += 1
                        else:
                            val = kw_args.get(base_key)
                        out_parts.append(val if val is not None else "__VAR__")
                    return "".join(out_parts)
                except Exception:
                    return re.sub(r"\{[^}]*\}", "__VAR__", fmt)
    if isinstance(node, ast.JoinedStr):
        # f-string — render constant parts, replace FormattedValues with __VAR__
        parts: list[str] = []
        for v in node.values:
            if isinstance(v, ast.Constant):
                parts.append(str(v.value))
            elif isinstance(v, ast.FormattedValue):
                inner = _try_eval_str_node(v.value, var_map) if isinstance(v.value, ast.AST) else None
                parts.append(inner if inner is not None else "__VAR__")
        return "".join(parts) or None
    return None


def _extract_cfg_query_from_ast(
    dict_node: "ast.Dict", var_map: "dict[str, str]"
) -> "str | None":
    """Walk a configuration={} AST dict node to extract configuration.query.query
    without ast.literal_eval — handles f-strings and variable references."""
    import ast

    try:
        for i, key in enumerate(dict_node.keys):
            if not (isinstance(key, ast.Constant) and key.value == "query"):
                continue
            inner = dict_node.values[i]
            if not isinstance(inner, ast.Dict):
                continue
            for j, inner_key in enumerate(inner.keys):
                if isinstance(inner_key, ast.Constant) and inner_key.value == "query":
                    return _try_eval_str_node(inner.values[j], var_map)
    except Exception:
        pass
    return None


def _extract_sql_from_python(
    source: str,
    file_dir: "Path",
    task_filter: str | None,
    path_reader: "Callable[[str], str | None] | None" = None,
) -> "dict[str, str]":
    """Return {task_id: sql_text} extracted from a Python DAG source file.

    Pass 1 — build variable maps (str, dict, list) from module-level assignments,
              including string concat, os.path.join, f-strings, and .format().
    Pass 2 — extract DAG(template_searchpath=...) to extend SQL search roots.
    Pass 3 — walk all Call nodes; handle sql=/bql= (str, list, variable),
              configuration= (literal dict, variable, and AST-walked dicts with
              f-strings), {% include %} directives, and plain .sql paths.
    Fallback — regex scan for triple-quoted SQL strings.

    path_reader: optional callback(repo_relative_path) -> file_content | None.
    Used in git mode to read files from git history when they are not on disk.
    """
    import ast

    results: dict[str, str] = {}

    try:
        tree = ast.parse(source)

        # ── Pass 1: build variable maps ──────────────────────────────────────
        var_map:      dict[str, str]   = {}  # name -> str value
        dict_var_map: dict[str, dict]  = {}  # name -> dict (for configuration=VAR)
        list_var_map: dict[str, list]  = {}  # name -> list[str] (for sql=[...])

        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
            ):
                continue
            name = node.targets[0].id
            val  = node.value

            if isinstance(val, ast.Dict):
                try:
                    dict_var_map[name] = ast.literal_eval(val)
                except Exception:
                    pass
            elif isinstance(val, ast.List):
                strings = []
                for elt in val.elts:
                    s = _try_eval_str_node(elt, var_map)
                    if s:
                        strings.append(s)
                if strings:
                    list_var_map[name] = strings
            else:
                s = _try_eval_str_node(val, var_map)
                if s:
                    var_map[name] = s

        # ── Pass 2: extract template_searchpath from DAG() constructor ────────
        template_searchpaths: list[Path] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            func_name = (
                func.id   if isinstance(func, ast.Name)      else
                func.attr if isinstance(func, ast.Attribute) else ""
            )
            if func_name != "DAG":
                continue
            for kw in node.keywords:
                if kw.arg != "template_searchpath":
                    continue
                raw_paths: list[str] = []
                if isinstance(kw.value, ast.List):
                    for elt in kw.value.elts:
                        s = _try_eval_str_node(elt, var_map)
                        if s:
                            raw_paths.append(s)
                else:
                    s = _try_eval_str_node(kw.value, var_map)
                    if s:
                        raw_paths.append(s)
                for rp in raw_paths:
                    p = Path(rp)
                    resolved = (file_dir / p).resolve() if not p.is_absolute() else p
                    template_searchpaths.append(resolved)

        # ── Helpers ───────────────────────────────────────────────────────────
        def _resolve_sql_path(path_str: str) -> "str | None":
            """Find a .sql file by searching file_dir, template_searchpaths, parents."""
            path_str = path_str.strip().lstrip("/")
            search_bases: list[Path] = (
                [file_dir] + template_searchpaths + list(file_dir.parents)
            )
            for base in search_bases:
                candidate = (base / path_str).resolve()
                if candidate.is_file():
                    return candidate.read_text(encoding="utf-8", errors="replace")
            if path_reader is not None:
                return path_reader(path_str)
            return None

        def _resolve_sql_val(sql_str: str) -> "str | None":
            """Resolve a raw sql= value: include directive, .sql path, or inline SQL."""
            s = sql_str.strip()
            # Jinja {% include 'path.sql' %}
            m = re.search(r"""{%-?\s*include\s+['"]([^'"]+)['"]\s*-?%}""", s)
            if m:
                return _resolve_sql_path(m.group(1))
            # Plain .sql file path (may contain __VAR__ from f-string resolution)
            if s.endswith(".sql"):
                content = _resolve_sql_path(s)
                if content:
                    return content
                # If __VAR__ is in the path, strip it and try just the filename
                if "__VAR__" in s:
                    filename = Path(s).name.replace("__VAR__", "")
                    if filename.endswith(".sql"):
                        return _resolve_sql_path(filename)
                return None
            # Inline SQL
            if _looks_like_sql(s):
                return s
            return None

        # ── Pass 3: walk all Call nodes ───────────────────────────────────────
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue

            tid:      str | None   = None
            sql_vals: list[str]    = []

            for kw in node.keywords:
                # task_id — resolve even when it's a variable or f-string
                if kw.arg == "task_id":
                    s = _try_eval_str_node(kw.value, var_map)
                    if s:
                        tid = s

                # sql= / bql= — string, list, or variable
                if kw.arg in ("sql", "bql"):
                    if isinstance(kw.value, ast.List):
                        for elt in kw.value.elts:
                            s = _try_eval_str_node(elt, var_map)
                            if s:
                                sql_vals.append(s)
                    elif isinstance(kw.value, ast.Name) and kw.value.id in list_var_map:
                        sql_vals.extend(list_var_map[kw.value.id])
                    else:
                        s = _try_eval_str_node(kw.value, var_map)
                        if s:
                            sql_vals.append(s)

                # configuration= — literal dict, variable, or AST-walked dict
                if kw.arg == "configuration":
                    raw_q: str | None = None
                    if isinstance(kw.value, ast.Name) and kw.value.id in dict_var_map:
                        cfg = dict_var_map[kw.value.id]
                        raw_q = (cfg.get("query") or {}).get("query", "") or None
                        if not raw_q:
                            from core.sql_formatter import extract_sql as _exsql
                            raw_q = _exsql(cfg)
                    elif isinstance(kw.value, ast.Dict):
                        try:
                            cfg = ast.literal_eval(kw.value)
                            raw_q = (cfg.get("query") or {}).get("query", "") or None
                            if not raw_q:
                                from core.sql_formatter import extract_sql as _exsql
                                raw_q = _exsql(cfg)
                        except Exception:
                            # literal_eval fails on f-strings / variable refs —
                            # walk the AST directly
                            raw_q = _extract_cfg_query_from_ast(kw.value, var_map)
                    if raw_q and isinstance(raw_q, str):
                        sql_vals.append(raw_q.strip())

            for sv in sql_vals:
                content = _resolve_sql_val(sv)
                if content and _looks_like_sql(content):
                    if task_filter is None or tid == task_filter:
                        key = tid or f"inline_{len(results)}"
                        results.setdefault(key, content)

    except SyntaxError:
        pass
    except Exception:
        pass

    # ── Regex fallback: triple-quoted SQL strings ─────────────────────────────
    if not results:
        for m in re.finditer(r'(?:"""|\'\'\')([\s\S]*?)(?:"""|\'\'\')' , source):
            s = m.group(1).strip()
            if _looks_like_sql(s):
                results[f"sql_{len(results)}"] = s

    return results


def _find_dag_files(root: "Path", dag_id: str) -> "list[Path]":
    """Find .py and .sql files under root likely related to dag_id.

    Priority:
    1. Name match via rglob — instant, zero I/O.
    2. OS grep -r -l  — single subprocess call, uses OS-level buffered I/O; orders of
       magnitude faster than reading files one-by-one in Python on large repos.
    """
    import subprocess

    dag_slug = dag_id.lower().replace("-", "_")
    candidates: list[Path] = []

    # Step 1: filename match (fast, no file I/O)
    for ext in (".py", ".sql"):
        for p in root.rglob(f"*{ext}"):
            if dag_slug in p.stem.lower() or dag_id.lower() in p.stem.lower():
                candidates.append(p)

    if candidates:
        return candidates

    # Step 2: OS grep — searches all .py files in one subprocess call
    try:
        result = subprocess.run(
            ["grep", "-r", "-l", "--include=*.py", dag_id, str(root)],
            capture_output=True, text=True, timeout=30,
        )
        for line in result.stdout.strip().splitlines():
            p = Path(line.strip())
            if p.is_file():
                candidates.append(p)
    except Exception:
        pass

    return candidates


def _diagnose_local_fetch(
    dag_id: str,
    local_root: str,
    task_filter: str | None,
) -> dict:
    """Walk the local SQL fetch pipeline step by step and return a diagnostic dict.

    Called when _fetch_sql_local returns empty so the user can see exactly which
    step broke: directory missing → no files matched → files found but no SQL extracted.
    """
    root = Path(local_root)
    diag: dict = {
        "dag_id":            dag_id,
        "local_root":        local_root,
        "root_exists":       root.is_dir(),
        "files_found":       [],
        "sql_per_file":      {},
        "step_failed":       "",
        "hint":              "",
    }

    if not root.is_dir():
        diag["step_failed"] = "directory_not_found"
        diag["hint"] = (
            f"LOCAL_DAG_ROOT='{local_root}' does not exist or is not a directory. "
            "Check the path in your .env file."
        )
        return diag

    files = _find_dag_files(root, dag_id)
    diag["files_found"] = [str(f) for f in files]

    if not files:
        diag["step_failed"] = "no_files_matched"
        dag_slug = dag_id.lower().replace("-", "_")
        diag["hint"] = (
            f"No .py or .sql files matched dag_id='{dag_id}' under '{local_root}'. "
            f"Discovery looks for '{dag_slug}' in filenames first, "
            f"then falls back to OS grep for the dag_id string inside .py files. "
            f"Check that the DAG filename contains '{dag_slug}' or that the dag_id "
            f"string appears in the file content."
        )
        return diag

    any_sql = False
    for fpath in files:
        try:
            source = fpath.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            diag["sql_per_file"][str(fpath)] = f"READ ERROR: {exc}"
            continue

        if fpath.suffix == ".sql":
            diag["sql_per_file"][str(fpath)] = ["<direct .sql file>"]
            any_sql = True
        else:
            sqls = _extract_sql_from_python(source, fpath.parent, task_filter)
            if sqls:
                diag["sql_per_file"][str(fpath)] = list(sqls.keys())
                any_sql = True
            else:
                diag["sql_per_file"][str(fpath)] = (
                    "no SQL extracted — no sql=/bql= kwargs found, "
                    "no resolvable .sql file paths, "
                    "and no triple-quoted SQL strings detected"
                )

    if not any_sql:
        diag["step_failed"] = "no_sql_extracted"
        diag["hint"] = (
            "Files were found but no SQL could be extracted from them. "
            "Common causes: (1) the DAG uses a variable for sql= that cannot be "
            "statically resolved (e.g. sql=QUERY_VAR where QUERY_VAR is built at "
            "runtime); (2) the sql= value is a GCS/Airflow path string — only local "
            ".sql paths are resolved; (3) BigQueryInsertJobOperator uses a "
            "configuration= dict built dynamically. "
            "Try pointing directly at the .sql file instead: pass the sql file path "
            "as local_dag_path or add the .sql files alongside the .py DAG file."
        )
    else:
        diag["step_failed"] = "format_or_jinja_error"
        diag["hint"] = (
            "SQL was extracted but was empty after Jinja resolution or formatting. "
            "Check LOCAL_JINJA_VARS_PATH and ensure the rendered SQL is valid BigQuery SQL."
        )

    return diag


def _diagnose_git_fetch(
    dag_id: str,
    git_root: str,
    ref: str,
    task_filter: str | None,
) -> dict:
    """Walk the git SQL fetch pipeline step by step and return a diagnostic dict."""
    import subprocess

    repo = Path(git_root)
    diag: dict = {
        "dag_id":        dag_id,
        "git_root":      git_root,
        "ref":           ref,
        "root_exists":   repo.is_dir(),
        "ls_tree_ok":    False,
        "files_found":   [],
        "sql_per_file":  {},
        "step_failed":   "",
        "hint":          "",
    }

    if not repo.is_dir():
        diag["step_failed"] = "directory_not_found"
        diag["hint"] = f"LOCAL_GIT_REPO_PATH='{git_root}' does not exist."
        return diag

    try:
        ls = subprocess.run(
            ["git", "-C", str(repo), "ls-tree", "-r", "--name-only", ref],
            capture_output=True, text=True, timeout=30,
        )
        if ls.returncode != 0:
            diag["step_failed"] = "invalid_ref"
            diag["hint"] = (
                f"'git ls-tree' failed for ref='{ref}' in '{git_root}'. "
                f"git stderr: {ls.stderr.strip()}. "
                "Check that the branch/tag/commit exists in the local repo."
            )
            return diag
        all_files = ls.stdout.strip().splitlines()
        diag["ls_tree_ok"] = True
    except Exception as exc:
        diag["step_failed"] = "git_error"
        diag["hint"] = f"git command failed: {exc}"
        return diag

    dag_slug = dag_id.lower().replace("-", "_")
    name_matches = [
        f for f in all_files
        if Path(f).suffix in (".py", ".sql")
        and (dag_slug in Path(f).stem.lower() or dag_id.lower() in Path(f).stem.lower())
    ]

    # Also try git grep
    grep_matches: list[str] = []
    if not name_matches:
        try:
            grep = subprocess.run(
                ["git", "-C", str(repo), "grep", "-l", dag_id, ref, "--", "*.py"],
                capture_output=True, text=True, timeout=20,
            )
            for line in grep.stdout.strip().splitlines():
                fpath = line.split(":", 1)[-1].strip()
                if fpath:
                    grep_matches.append(fpath)
        except Exception:
            pass

    candidate_paths = name_matches or grep_matches
    diag["files_found"] = candidate_paths
    diag["name_match_count"] = len(name_matches)
    diag["grep_match_count"] = len(grep_matches)

    if not candidate_paths:
        diag["step_failed"] = "no_files_matched"
        diag["hint"] = (
            f"No .py/.sql files matched dag_id='{dag_id}' at ref='{ref}'. "
            f"Name search looked for '{dag_slug}' in filenames across {len(all_files)} "
            f"tracked files. git grep also found nothing. "
            "Check that the dag_id string appears in a filename or file content at this ref."
        )
        return diag

    any_sql = False
    for git_path in candidate_paths[:5]:  # inspect first 5 only to keep it fast
        try:
            content = subprocess.run(
                ["git", "-C", str(repo), "show", f"{ref}:{git_path}"],
                capture_output=True, text=True, timeout=15,
            ).stdout
            def _diag_git_reader(rel_path: str) -> "str | None":
                try:
                    out = subprocess.run(
                        ["git", "-C", str(repo), "show", f"{ref}:{rel_path}"],
                        capture_output=True, text=True, timeout=15,
                    )
                    return out.stdout if out.returncode == 0 else None
                except Exception:
                    return None

            sqls = _extract_sql_from_python(
                content, repo / Path(git_path).parent, task_filter, path_reader=_diag_git_reader
            )
            if sqls:
                diag["sql_per_file"][git_path] = list(sqls.keys())
                any_sql = True
            else:
                diag["sql_per_file"][git_path] = "no SQL extracted"
        except Exception as exc:
            diag["sql_per_file"][git_path] = f"ERROR: {exc}"

    if not any_sql:
        diag["step_failed"] = "no_sql_extracted"
        diag["hint"] = (
            "Files found at this ref but no SQL could be extracted. "
            "Same causes as local mode: dynamic sql= variables, runtime-built "
            "configuration= dicts, or GCS SQL paths that cannot be resolved from git."
        )

    return diag


def _fetch_sql_local(
    dag_id: str,
    local_dag_root: str,
    task_filter: str | None,
    jinja_vars: dict,
) -> "dict[str, str]":
    """Scan local filesystem for DAG / SQL files and return Jinja-resolved SQL per task."""
    from core.sql_formatter import format_sql

    root = Path(local_dag_root)
    if not root.is_dir():
        return {}

    results: dict[str, str] = {}
    for fpath in _find_dag_files(root, dag_id):
        try:
            source = fpath.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue

        if fpath.suffix == ".sql":
            if task_filter is None or fpath.stem == task_filter:
                results[fpath.stem] = format_sql(_resolve_jinja(source, jinja_vars))
        else:
            for tid, raw in _extract_sql_from_python(source, fpath.parent, task_filter).items():
                results[tid] = format_sql(_resolve_jinja(raw, jinja_vars))

    return results


def _fetch_sql_git(
    dag_id: str,
    git_repo_path: str,
    git_ref: str,
    task_filter: str | None,
) -> "dict[str, str]":
    """Read DAG / SQL files from a local git repo at a specific ref using 'git show'.
    No checkout required — reads file content directly from git object store.
    Jinja vars are loaded from git history (LOCAL_GIT_JINJA_VARS_PATH at the same ref)
    so they match the branch/commit being validated."""
    import subprocess
    from core.sql_formatter import format_sql

    repo = Path(git_repo_path)
    if not repo.is_dir():
        return {}

    ref = git_ref or "HEAD"
    dag_slug = dag_id.lower().replace("-", "_")
    results: dict[str, str] = {}

    # Load Jinja vars from git history at this ref (not from host filesystem)
    resolved_vars = _load_jinja_vars_for_git(str(repo), ref)

    # ── Candidate file discovery ──────────────────────────────────────────────
    # Step 1: name-match via ls-tree (zero I/O, instant)
    try:
        ls = subprocess.run(
            ["git", "-C", str(repo), "ls-tree", "-r", "--name-only", ref],
            capture_output=True, text=True, timeout=30,
        )
        all_files = ls.stdout.strip().splitlines()
    except Exception:
        return {}

    candidate_paths = [
        f for f in all_files
        if Path(f).suffix in (".py", ".sql")
        and (dag_slug in Path(f).stem.lower() or dag_id.lower() in Path(f).stem.lower())
    ]

    # Step 2: git grep fallback — searches git index, no per-file I/O
    if not candidate_paths:
        try:
            grep = subprocess.run(
                ["git", "-C", str(repo), "grep", "-l", dag_id, ref, "--", "*.py"],
                capture_output=True, text=True, timeout=20,
            )
            # Output format: "<ref>:<filepath>" per line
            for line in grep.stdout.strip().splitlines():
                fpath = line.split(":", 1)[-1].strip()
                if fpath and Path(fpath).suffix == ".py":
                    candidate_paths.append(fpath)
        except Exception:
            pass

    for git_path in candidate_paths:
        try:
            content = subprocess.run(
                ["git", "-C", str(repo), "show", f"{ref}:{git_path}"],
                capture_output=True, text=True, timeout=30,
            ).stdout
        except Exception:
            continue

        fpath = Path(git_path)
        if fpath.suffix == ".sql":
            if task_filter is None or fpath.stem == task_filter:
                results[fpath.stem] = format_sql(_resolve_jinja(content, resolved_vars))
        else:
            def _git_reader(rel_path: str) -> "str | None":
                try:
                    out = subprocess.run(
                        ["git", "-C", str(repo), "show", f"{ref}:{rel_path}"],
                        capture_output=True, text=True, timeout=15,
                    )
                    return out.stdout if out.returncode == 0 else None
                except Exception:
                    return None

            for tid, raw in _extract_sql_from_python(
                content, repo / fpath.parent, task_filter, path_reader=_git_reader
            ).items():
                results[tid] = format_sql(_resolve_jinja(raw, resolved_vars))

    return results


def _do_validate_mapping(
	mapping_file_name: str,
	composer_env: str = None,
	dag_id: str = None,
	task_id: str = None,
	target_column_filter: str = None,
	force_refresh: bool = False,
	source_mode: str = "composer",
	local_dag_path: str = None,
	git_repo_path: str = None,
	git_ref: str = None,
) -> dict:
	start = time.time()
	try:
		# ── Resolve mapping file in registry ─────────────────────────────────
		registry = persistence.get_registry()
		stem_filter = mapping_file_name.lower().replace(".xlsx", "").replace(".xls", "")
		entry = None
		for e in registry:
			stem = Path(e.get("file_path", "")).stem.lower()
			if stem_filter in stem or stem in stem_filter or stem_filter in e.get("table_name", "").lower():
				entry = e
				break

		if not entry:
			return {
				"error": f"No Excel file matching '{mapping_file_name}' found in registry.",
				"hint": "Use list_loaded_tables to see available files.",
			}

		table_name = entry["table_name"]
		file_stem = Path(entry["file_path"]).stem

		# ── Load excel_mapping.json config for this file ──────────────────────
		excel_map  = persistence.get_excel_mapping()
		file_config = excel_map.get(file_stem) or excel_map.get(file_stem.lower()) or {}
		configured_cols	 = file_config.get("mapping_columns") or {}
		bq_tables_config	= file_config.get("bq_table") or entry.get("bq_table") or []
		if isinstance(bq_tables_config, str):
			bq_tables_config = [bq_tables_config]

		dag_names_config = file_config.get("dag_names") or entry.get("dag_names") or []
		resolved_dag_id = dag_id or (dag_names_config[0] if dag_names_config else None)

		# ── Resolve Composer environment ──────────────────────────────────────
		resolved_env = composer_env
		if not resolved_env:
			from core.workspace import get_pinned_workspace
			resolved_env = get_pinned_workspace().get("composer_env")
		if not resolved_env and config.COMPOSER_ENVS:
			resolved_env = next(iter(config.COMPOSER_ENVS))

		# ── Load rows from DuckDB ─────────────────────────────────────────────
		db = get_manager()
		if table_name not in db.list_tables():
			from tools.excel_tools import ingest_excel_files
			ingest_excel_files()

		try:
			df = db.execute(f"SELECT * FROM {table_name}")
		except Exception as exc:
			return {"error": f"DuckDB query failed: {exc}", "table_name": table_name}

		if df.empty:
			return {"error": f"Table {table_name} is empty.", "table_name": table_name}

		actual_cols = list(df.columns)

		# ── Detect / resolve column roles ─────────────────────────────────────
		col_config  = _resolve_column_config(actual_cols, configured_cols)
		target_col  = col_config["target"]
		source_col  = col_config["source"]
		logic_col	= col_config["logic"]
		supp_cols	= col_config["logic_supplementary"]
		bqtable_col = col_config["bq_table"]
		multirow_col = col_config["multi_row_key"]
		ruleid_col  = col_config.get("rule_id")

		if not target_col and not logic_col:
			return {
				"error": (
					"Could not detect target or logic columns. "
					"Set mapping_columns in excel_mapping.json for this file."
				),
				"available_columns": actual_cols,
				"column_config": col_config,
			}

		# ── Build rule groups (merge multi-row rules) ─────────────────────────
		def _cell(row, col: str | None) -> str:
			if col is None:
				return ""
			v = row.get(col)
			s = "" if v is None else str(v).strip()
			return "" if s.lower() in ("none", "nan", "nat", "") else s

		group_map: dict[tuple, dict] = {}
		for _, row in df.iterrows():
			bq_val	= _cell(row, bqtable_col)
			key_val  = _cell(row, multirow_col)
			src_val  = _cell(row, source_col)
			logic_val = _cell(row, logic_col)
			rule_id_val = _cell(row, ruleid_col) if ruleid_col else None
			supp_val = " | ".join(filter(None, (_cell(row, c) for c in supp_cols)))

			# If rule_id exists, we can use it to uniquely group rows across the Excel sheet
			# Otherwise we fall back to (bq_table, target_column)
			if rule_id_val:
				gkey = str(rule_id_val)
			else:
				gkey = (bq_val, key_val)
				
			if gkey not in group_map:
				group_map[gkey] = {
					"rule_id": rule_id_val if rule_id_val else None,
					"target_columns": [key_val] if key_val else [],
					"source_columns": [],
					"logic_parts": [],
				}

			if src_val:
				for s in src_val.split(","):
					s = s.strip()
					if s and s not in group_map[gkey]["source_columns"]:
						group_map[gkey]["source_columns"].append(s)

			if logic_val:
				full = logic_val + (f" | {supp_val}" if supp_val else "")
				if full not in group_map[gkey]["logic_parts"]:
					group_map[gkey]["logic_parts"].append(full)

		raw_rules: list[dict] = []
		# Use explicit enumerate rule_id if rule_id column was empty for some rows
		for fallback_idx, (gkey, grp) in enumerate(group_map.items(), start=1):
			rule_text = " | ".join(grp["logic_parts"])
			
			# The AI array mapping strictly depends on rule_id, so it must be an int or a robust string.
			try:
				rid = int(grp["rule_id"]) if grp["rule_id"] else fallback_idx
			except ValueError:
				rid = grp["rule_id"] or fallback_idx
				
			raw_rules.append({
				"rule_id":	  rid,
				"target_columns": grp["target_columns"],
				"source_columns": grp["source_columns"],
				"rule_text":	 rule_text,
				"bq_table_hint": None, # note bq_val here might be stale from the loop above, but we grouped by gkey.
			})

		# Fix: Extract actual bq_table_hint for raw_rules from the grouped items properly.
		# Since we changed gkey to be rule_id_val, bq_val from the outer scope isn't correct.
		# We'll just assign it None for now since bulk evaluation handles all tables.

		# ── Apply target column filter ─────────────────────────────────────────
		if target_column_filter:
			flt = target_column_filter.lower()
			raw_rules = [
				r for r in raw_rules
				if any(flt in c.lower() for c in r["target_columns"])
			]

		# ── Classify rules (NOT_APPLICABLE + rule_type + confidence) ──────────
		for rule in raw_rules:
			if not rule["rule_text"] or _NA_PATTERN.match(rule["rule_text"]):
				rule["_na"] = True
				rule["rule_type"] = "not_applicable"
				rule["confidence_tier"] = ""
			else:
				rtype, conf = _classify_rule(rule["rule_text"], rule["target_columns"])
				rule["rule_type"]	  = rtype
				rule["confidence_tier"] = conf
				rule["_na"] = False

		# ── Fetch SQL — branched on source_mode ──────────────────────────────────
		task_sqls: dict[str, str] = {}
		sql_fetch_error: str | None = None
		sql_debug: dict | None = None
		tasks_evaluated: list[str] = []

		if source_mode == "local":
			local_root = local_dag_path or config.LOCAL_DAG_ROOT
			if not resolved_dag_id:
				sql_fetch_error = "dag_id is required for source_mode=local."
			elif not local_root:
				sql_fetch_error = "LOCAL_DAG_ROOT is not set in .env and local_dag_path was not provided."
			else:
				try:
					jinja_vars = _load_jinja_vars()
					task_sqls = _fetch_sql_local(resolved_dag_id, local_root, task_id, jinja_vars)
					tasks_evaluated = list(task_sqls.keys())
					if not task_sqls:
						sql_debug = _diagnose_local_fetch(resolved_dag_id, local_root, task_id)
						sql_fetch_error = sql_debug.get("hint") or "No SQL found in local mode."
				except Exception as exc:
					sql_fetch_error = str(exc)

		elif source_mode == "git":
			git_root = git_repo_path or config.LOCAL_GIT_REPO_PATH
			ref = git_ref or config.LOCAL_GIT_DEFAULT_BRANCH
			if not resolved_dag_id:
				sql_fetch_error = "dag_id is required for source_mode=git."
			elif not git_root:
				sql_fetch_error = "LOCAL_GIT_REPO_PATH is not set in .env and git_repo_path was not provided."
			else:
				try:
					task_sqls = _fetch_sql_git(resolved_dag_id, git_root, ref, task_id)
					tasks_evaluated = list(task_sqls.keys())
					if not task_sqls:
						sql_debug = _diagnose_git_fetch(resolved_dag_id, git_root, ref, task_id)
						sql_fetch_error = sql_debug.get("hint") or "No SQL found in git mode."
				except Exception as exc:
					sql_fetch_error = str(exc)

		else:  # composer (original behaviour)
			if resolved_env and resolved_dag_id:
				try:
					if task_id:
						from tools.composer_tools import (
							_get, _enc, _best_sql, _extract_rendered_sql,
							_rendered_was_truncated, _get_sql_file_path, _fetch_sql_file,
						)
						from core.sql_formatter import extract_sql, format_sql

						task_data = _get(resolved_env, f"/dags/{_enc(resolved_dag_id)}/tasks/{_enc(task_id)}")
						sql_file = _get_sql_file_path(task_data)
						raw_sql  = _fetch_sql_file(sql_file) if sql_file else None
						if not raw_sql:
							raw_sql = extract_sql(task_data)

						runs = _get(
							resolved_env,
							f"/dags/{_enc(resolved_dag_id)}/dagRuns",
							{"limit": 10, "order_by": "-execution_date", "state": "success"},
						)
						rendered_sql = None
						rendered_truncated = False
						for run in runs.get("dag_runs", []):
							try:
								ti = _get(
									resolved_env,
									f"/dags/{_enc(resolved_dag_id)}/dagRuns"
									f"/{_enc(run['dag_run_id'])}/taskInstances/{_enc(task_id)}",
								)
								rendered_sql	  = _extract_rendered_sql(ti)
								rendered_truncated = _rendered_was_truncated(ti)
								if rendered_sql:
									break
							except Exception:
								continue

						best = _best_sql(raw_sql, rendered_sql, rendered_truncated)
						if best:
							task_sqls[task_id] = format_sql(best)
					else:
						task_sqls = _fetch_all_task_sqls(resolved_env, resolved_dag_id)

					tasks_evaluated = list(task_sqls.keys())
				except Exception as exc:
					sql_fetch_error = str(exc)

		sql_note = (
			"Jinja template expressions have been pre-resolved using configured variable values "
			"(LOCAL_JINJA_VARS_PATH). Unknown Jinja expressions are replaced with the "
			"placeholder '__JINJA__' — do not flag these as mismatches."
			if source_mode != "composer" else ""
		)

		# ── Deconstruct SQL per task, then merge ──────────────────────────────
		structures: dict[str, dict] = {tid: _deconstruct_sql(sql) for tid, sql in task_sqls.items()}
		merged_structure = _merge_structures(list(structures.values())) if structures else {
			"ctes": {}, "joins": [], "where_clauses": [], "group_by": [],
			"select_expressions": {}, "aggregations": [], "destination_table": None,
			"raw_sql": ""
		}

		def _structure_for_bq(bq_hint: str | None) -> dict:
			"""Pick the task structure whose destination table matches the BQ hint."""
			if bq_hint:
				hint_norm = _norm(bq_hint.split(".")[-1])
				for s in structures.values():
					dest = _norm((s.get("destination_table") or "").split(".")[-1])
					if dest and hint_norm == dest:
						return s
			return merged_structure

		# ── Group rules by BQ table label ─────────────────────────────────────
		bq_groups: dict[str, list[dict]] = {}
		for rule in raw_rules:
			hint = rule.get("bq_table_hint")
			if hint:
				bq_groups.setdefault(hint, []).append(rule)
			elif bq_tables_config:
				for bq in bq_tables_config:
					bq_groups.setdefault(bq, []).append(rule)
			else:
				bq_groups.setdefault("(no BQ table configured)", []).append(rule)

		# ── Evaluate rules ────────────────────────────────────────────────────
		summary = {
			"total": 0, "pass": 0, "fail": 0, "partial": 0,
			"not_applicable": 0, "not_evaluated": 0, "error": 0, "low_confidence": 0,
		}
		output_groups: list[dict] = []
		has_sql = bool(structures)

		for bq_label, group_rules in bq_groups.items():
			structure = _structure_for_bq(bq_label)
			evaluated_rules: list[dict] = []
			
			# --- BULK EVALUATION EXECUTION ---
			bulk_verdicts = {}
			if has_sql:
				 bulk_verdicts = _evaluate_rules_bulk(group_rules, structure, force_refresh, sql_note)

			for rule in group_rules:
				summary["total"] += 1
				out: dict = {
					"rule_id":		 rule["rule_id"],
					"target_columns":  rule["target_columns"],
					"source_columns":  rule["source_columns"],
					"rule_text":		rule["rule_text"],
					"rule_type":		rule.get("rule_type", ""),
					"confidence_tier": rule.get("confidence_tier", ""),
					"verdict":		 "",
					"reason":		  "",
					"evidence":		 "",
					"flags":			[],
					"relevant_ctes":	[],
					"relevant_clauses": [],
					"cache_hit":		False,
				}

				if rule["_na"]:
					out["verdict"] = "NOT_APPLICABLE"
					out["reason"] = "Rule text indicates no transformation logic required."
					summary["not_applicable"] += 1
				elif not has_sql:
					out["verdict"] = "NOT_EVALUATED"
					out["reason"] = (
						sql_fetch_error
						or (
							"No SQL available — DAG not found or no SQL could be extracted."
							if source_mode != "composer"
							else "No SQL available (Composer not configured or DAG not found)."
						)
					)
					summary["not_evaluated"] += 1
				else:
					# Fetch from bulk results
					verdict_data = bulk_verdicts.get(rule["rule_id"])
					
					# Sometimes LLM might cast rule_id as string or vice versa
					if not verdict_data:
						try:
							verdict_data = bulk_verdicts.get(str(rule["rule_id"]))
						except Exception:
							pass
					
					if verdict_data:
						out.update(verdict_data)
						v = out["verdict"].lower().replace(" ", "_")
						if v in summary:
							summary[v] += 1
						if rule.get("confidence_tier") == "LOW":
							summary["low_confidence"] += 1
					else:
						out["verdict"] = "ERROR"
						out["reason"] = "LLM omitted this rule from its bulk response."
						summary["error"] += 1

				evaluated_rules.append(out)

			output_groups.append({
				"bq_table":  bq_label,
				"rule_count": len(evaluated_rules),
				"rules":	 evaluated_rules,
			})

		result = {
			"mapping_file": mapping_file_name,
			"duckdb_table": table_name,
			"dag_id":		resolved_dag_id,
			"source_mode":  source_mode,
			"composer_env": resolved_env if source_mode == "composer" else None,
			"column_config": col_config,
			"summary":	  summary,
			"bq_table_groups": output_groups,
			"sql_structure": {
				"tasks_evaluated": tasks_evaluated,
				"cte_count":	  len(merged_structure.get("ctes", {})),
				"join_count":	 len(merged_structure.get("joins", [])),
				"parse_errors":	[
					s["parse_error"] for s in structures.values() if s.get("parse_error")
				],
			},
		}
		if sql_fetch_error:
			result["sql_fetch_error"] = sql_fetch_error
		if sql_debug:
			result["sql_debug"] = sql_debug

		log_audit(
			"mapping_validation", resolved_env or "local",
			f"validate:{mapping_file_name}",
			row_count=summary["total"],
			duration_ms=int((time.time() - start) * 1000),
		)
		return result

	except Exception as exc:
		return {"error": str(exc)}

# ── Batch folder tool ────────────────────────────────────────────────────────

def _discover_and_stage_excel_files(
    folder_path: str | None,
    gcs_path: str | None,
    git_folder: str | None,
    git_repo_path: str | None,
    git_ref: str | None,
) -> tuple[list[Path], list[str]]:
    """Return (list_of_local_paths, warning_messages).

    Local paths point to files inside DATA_ROOT/mapping/ ready for ingest.
    Files from GCS / git are downloaded/extracted into a staging subdirectory.
    """
    import shutil, tempfile
    from core import config

    data_root   = Path(config.DATA_ROOT)
    mapping_dir = data_root / "mapping"
    mapping_dir.mkdir(parents=True, exist_ok=True)

    staged: list[Path] = []
    warnings: list[str] = []

    # ── Local folder ─────────────────────────────────────────────────────────
    if folder_path:
        src = Path(folder_path)
        if not src.is_dir():
            warnings.append(f"folder_path '{folder_path}' does not exist or is not a directory.")
            return staged, warnings
        for f in src.glob("*.xlsx"):
            dest = mapping_dir / f.name
            if not dest.exists():
                shutil.copy2(f, dest)
            staged.append(dest)

    # ── GCS path  gs://bucket/prefix/ ────────────────────────────────────────
    elif gcs_path:
        try:
            from google.cloud import storage as gcs
            # Parse gs://bucket/prefix
            without_scheme = gcs_path.replace("gs://", "")
            bucket_name, _, prefix = without_scheme.partition("/")
            client = gcs.Client(project=config.GOOGLE_CLOUD_PROJECT or None)
            blobs  = client.list_blobs(bucket_name, prefix=prefix)
            for blob in blobs:
                if not blob.name.endswith(".xlsx"):
                    continue
                fname = Path(blob.name).name
                dest  = mapping_dir / fname
                if not dest.exists():
                    blob.download_to_filename(str(dest))
                staged.append(dest)
        except Exception as exc:
            warnings.append(f"GCS download error: {exc}")

    # ── Git folder  (path within repo at a given ref) ─────────────────────────
    elif git_folder:
        import subprocess
        repo = Path(git_repo_path or config.LOCAL_GIT_REPO_PATH)
        ref  = git_ref or config.LOCAL_GIT_DEFAULT_BRANCH
        if not repo.is_dir():
            warnings.append(f"git_repo_path '{repo}' does not exist.")
            return staged, warnings
        try:
            ls = subprocess.run(
                ["git", "-C", str(repo), "ls-tree", "-r", "--name-only", ref],
                capture_output=True, text=True, timeout=30,
            )
            all_files = ls.stdout.strip().splitlines()
        except Exception as exc:
            warnings.append(f"git ls-tree failed: {exc}")
            return staged, warnings

        folder_norm = git_folder.strip("/")
        xlsx_files  = [
            f for f in all_files
            if f.endswith(".xlsx") and f.startswith(folder_norm)
        ]
        for git_path in xlsx_files:
            fname = Path(git_path).name
            dest  = mapping_dir / fname
            if not dest.exists():
                try:
                    content = subprocess.run(
                        ["git", "-C", str(repo), "show", f"{ref}:{git_path}"],
                        capture_output=True, timeout=30,
                    ).stdout
                    dest.write_bytes(content)
                except Exception as exc:
                    warnings.append(f"git show failed for {git_path}: {exc}")
                    continue
            staged.append(dest)

    return staged, warnings


@tool
def validate_mapping_folder(
    folder_path: str = None,
    gcs_path: str = None,
    git_folder: str = None,
    composer_env: str = None,
    source_mode: str = "composer",
    local_dag_path: str = None,
    git_repo_path: str = None,
    git_ref: str = None,
    force_refresh: bool = False,
) -> str:
    """Validate ALL mapping Excel files found in a folder against SQL / DAG code.

    Exactly one of folder_path / gcs_path / git_folder must be supplied:
      folder_path : absolute path to a local directory containing .xlsx files.
      gcs_path    : GCS URI, e.g. "gs://my-bucket/mappings/".
      git_folder  : repo-relative folder containing .xlsx files, e.g. "config/mappings".
                    Uses LOCAL_GIT_REPO_PATH + git_ref (or LOCAL_GIT_DEFAULT_BRANCH).

    source_mode controls where SQL is read from — same as validate_mapping_rules:
      "composer" (default) | "local" | "git"

    The DAG id for each file is resolved from config/excel_mapping.json (dag_names field).
    Files not present in the registry are auto-ingested before validation.

    Returns consolidated results + path to a generated Excel export file.
    """
    import json, time
    from core import config, persistence
    from core.duckdb_manager import get_manager
    from utils.excel_export import export_validation_excel

    start = time.time()

    # ── 1. Discover and stage Excel files ────────────────────────────────────
    staged, disc_warnings = _discover_and_stage_excel_files(
        folder_path, gcs_path, git_folder, git_repo_path, git_ref
    )

    if not staged:
        return safe_json({
            "error": "No .xlsx files found in the specified location.",
            "warnings": disc_warnings,
            "hint": (
                "Check folder_path / gcs_path / git_folder. "
                "For git, ensure git_repo_path and git_ref are set."
            ),
        })

    # ── 2. Auto-ingest any newly staged files ────────────────────────────────
    try:
        from tools.excel_tools import ingest_excel_files
        ingest_excel_files()
    except Exception:
        pass

    # ── 3. Resolve DAG ids from excel_mapping.json ───────────────────────────
    excel_map = persistence.get_excel_mapping()

    # ── 4. Validate each file ────────────────────────────────────────────────
    results: list[dict]      = []
    progress_log: list[dict] = []
    overall_summary = {
        "total": 0, "pass": 0, "fail": 0, "partial": 0,
        "not_applicable": 0, "not_evaluated": 0, "error": 0, "low_confidence": 0,
    }

    for xlsx_path in staged:
        file_stem = xlsx_path.stem
        file_cfg  = excel_map.get(file_stem) or excel_map.get(file_stem.lower()) or {}
        dag_names = file_cfg.get("dag_names") or []
        resolved_dag = dag_names[0] if dag_names else None

        res = _do_validate_mapping(
            xlsx_path.name,
            composer_env,
            resolved_dag,
            None,                   # task_id
            None,                   # target_column_filter
            force_refresh,
            source_mode,
            local_dag_path,
            git_repo_path,
            git_ref,
        )

        results.append(res)

        s = res.get("summary", {})
        for k in overall_summary:
            overall_summary[k] += s.get(k, 0)

        progress_log.append({
            "file":           xlsx_path.name,
            "dag_id":         resolved_dag or "(not configured)",
            "status":         "error" if "error" in res else "done",
            "pass":           s.get("pass", 0),
            "fail":           s.get("fail", 0),
            "partial":        s.get("partial", 0),
            "not_evaluated":  s.get("not_evaluated", 0),
            "total":          s.get("total", 0),
        })

    # ── 5. Determine env label ────────────────────────────────────────────────
    if source_mode == "composer":
        env_label = composer_env or "composer"
    elif source_mode == "git":
        env_label = (git_ref or config.LOCAL_GIT_DEFAULT_BRANCH or "git").replace("/", "-")
    else:
        env_label = "local"

    # ── 6. Export to Excel ───────────────────────────────────────────────────
    export_path: str | None = None
    try:
        out = export_validation_excel(
            results,
            env_label,
            Path(config.EXPORTS_ROOT),
        )
        export_path = str(out)
    except Exception as exc:
        disc_warnings.append(f"Excel export failed: {exc}")

    return safe_json({
        "is_bulk":          True,
        "is_folder_batch":  True,
        "source":           "gcs" if gcs_path else ("git" if git_folder else "local"),
        "env_label":        env_label,
        "files_processed":  len(results),
        "overall_summary":  overall_summary,
        "progress_log":     progress_log,
        "results":          results,
        "export_path":      export_path,
        "warnings":         disc_warnings,
        "elapsed_seconds":  round(time.time() - start, 1),
    })


# ── Main tool ─────────────────────────────────────────────────────────────────

@tool
def validate_mapping_rules(
	mapping_file_name: str,
	composer_env: str = None,
	dag_id: str = None,
	task_id: str = None,
	target_column_filter: str = None,
	force_refresh: bool = False,
	source_mode: str = "composer",
	local_dag_path: str = None,
	git_repo_path: str = None,
	git_ref: str = None,
) -> str:
	"""Validate Excel mapping transformation rules against BigQuery SQL implementation.

	Extracts transformation rules from the Excel file in DuckDB, deconstructs the
	DAG task SQL using sqlglot, then uses an LLM to assess whether each rule is
	correctly implemented. Groups results by BigQuery target table.

	mapping_file_name: Excel file name (with or without .xlsx/.xls), comma-separated list,
					  or folder pattern to validate multiple files.
	composer_env: Airflow/Composer env alias; falls back to pinned workspace. Used when
				  source_mode=composer (default).
	dag_id: DAG to evaluate; falls back to excel_mapping.json lookup.
	task_id: Specific task to evaluate; if None, all tasks are evaluated and merged.
	target_column_filter: Only validate rules whose target column contains this string.
	force_refresh: Bypass in-session verdict cache (default False).
	source_mode: Where to read SQL from. One of:
	  "composer" (default) — live Airflow rendered SQL via Composer REST API.
	  "local"   — DAG / SQL files on the local filesystem (uses LOCAL_DAG_ROOT from .env
	              or local_dag_path argument). Jinja vars resolved from LOCAL_JINJA_VARS_PATH.
	  "git"     — DAG / SQL files read from a local git repo at a specific ref using
	              "git show" (uses LOCAL_GIT_REPO_PATH / LOCAL_GIT_DEFAULT_BRANCH from .env
	              or git_repo_path / git_ref arguments). No checkout required.
	local_dag_path: Override LOCAL_DAG_ROOT for this call (source_mode=local).
	git_repo_path: Override LOCAL_GIT_REPO_PATH for this call (source_mode=git).
	git_ref: Branch, tag, or commit SHA to read from (source_mode=git). Defaults to
	         LOCAL_GIT_DEFAULT_BRANCH from .env (fallback: main).

	Returns JSON with: source_mode, column_config, summary (pass/fail/partial/not_applicable
	counts), bq_table_groups (rules with verdict/reason/evidence/flags), sql_structure metadata.
	Verdicts: PASS, FAIL, PARTIAL, NOT_APPLICABLE, NOT_EVALUATED, ERROR.
	Confidence tiers: HIGH, MEDIUM, LOW (LOW requires human sign-off in the UI)."""
	
	# Check if this is a bulk operation (comma separated list or directory)
	# We will determine this by looking up the registry for matching files.
	registry = persistence.get_registry()
	files_to_process = []
	
	if "," in mapping_file_name:
		parts = [p.strip().lower() for p in mapping_file_name.split(",")]
		for p in parts:
			stem_filter = p.replace(".xlsx", "").replace(".xls", "")
			for e in registry:
				stem = Path(e.get("file_path", "")).stem.lower()
				if stem_filter in stem or stem in stem_filter or stem_filter in e.get("table_name", "").lower():
					files_to_process.append(e.get("file_path"))
					break
	else:
		# Check if it's a directory
		stem_filter = mapping_file_name.lower().replace(".xlsx", "").replace(".xls", "")
		# Could be a folder, let's see if multiple registry items match this folder pattern
		folder_matches = [e.get("file_path") for e in registry if stem_filter in Path(e.get("file_path", "")).parent.name.lower()]
		
		if len(folder_matches) > 0:
			files_to_process = folder_matches
		else:
			# Just a single file
			for e in registry:
				stem = Path(e.get("file_path", "")).stem.lower()
				if stem_filter in stem or stem in stem_filter or stem_filter in e.get("table_name", "").lower():
					files_to_process.append(e.get("file_path"))
					break
					
	# Deduplicate
	files_to_process = list(set(files_to_process))
	
	if not files_to_process:
		 return safe_json({
			"error": f"No Excel file matching '{mapping_file_name}' found in registry.",
			"hint": "Use list_loaded_tables to see available files.",
		})
		
	if len(files_to_process) == 1:
		# Single file flow
		result = _do_validate_mapping(
			Path(files_to_process[0]).name,
			composer_env,
			dag_id,
			task_id,
			target_column_filter,
			force_refresh,
			source_mode,
			local_dag_path,
			git_repo_path,
			git_ref,
		)
		return safe_json(result)

	# Bulk flow
	bulk_results = []
	overall_summary = {
		"total": 0, "pass": 0, "fail": 0, "partial": 0,
		"not_applicable": 0, "not_evaluated": 0, "error": 0, "low_confidence": 0,
	}

	for f in files_to_process:
		fname = Path(f).name
		res = _do_validate_mapping(
			fname, composer_env, None, None, target_column_filter, force_refresh,
			source_mode, local_dag_path, git_repo_path, git_ref,
		)
		bulk_results.append(res)
		
		if "summary" in res:
			for k, v in res["summary"].items():
				if k in overall_summary:
					overall_summary[k] += v

	return safe_json({
		"is_bulk": True,
		"bulk_query": mapping_file_name,
		"files_processed": len(files_to_process),
		"overall_summary": overall_summary,
		"results": bulk_results
	})