"""sqlglot-based SQL formatter and Airflow rendered-field SQL extractor."""

import re

try:
	import sqlglot
	_HAS_SQLGLOT = True
except ImportError:
	_HAS_SQLGLOT = False

# Scoping constants
MAX_FORMAT_SIZE = 300000  # Characters
_SQL_RE = re.compile(r'\b(SELECT|WITH|INSERT|MERGE|UPDATE|DELETE|CREATE|DECLARE|SET|BEGIN|EXCEPTION)\b', re.IGNORECASE)


def strip_jinja(sql: str) -> str:
	"""Replace Jinja2 templates with SQL-safe placeholders for AST parsing.
	Does NOT modify the original string returned to the UI — call this only
	before passing SQL to sqlglot for structural analysis."""
	sql = re.sub(r"\{\{.*?\}\}", "'__JINJA__'", sql, flags=re.DOTALL)
	sql = re.sub(r"\{%-?.*?-?%\}", " ", sql, flags=re.DOTALL)
	return sql


def extract_sql(obj, _depth: int = 0) -> str | None:
	"""Recursively extract SQL from an Airflow renderedFields / task-definition dict.

	Priority:
	  1. Top-level keys: sql, query, bql
	  2. BigQueryInsertJobOperator nesting: configuration.query.query
	  3. Any string value containing a SQL keyword (len > 20)
	"""
	if _depth > 6:
		return None
	if isinstance(obj, str):
		s = obj.strip()
		return s if len(s) > 20 and _SQL_RE.search(s) else None
	if isinstance(obj, dict):
		for field in ("sql", "query", "bql"):
			result = extract_sql(obj.get(field), _depth + 1)
			if result:
				return result
		cfg = obj.get("configuration")
		if isinstance(cfg, dict):
			q_block = cfg.get("query")
			if isinstance(q_block, dict):
				result = extract_sql(q_block.get("query"), _depth + 1)
				if result:
					return result
		for key, val in obj.items():
			if key in ("sql", "query", "bql", "configuration"):
				continue
			result = extract_sql(val, _depth + 1)
			if result:
				return result
	if isinstance(obj, list):
		for item in obj:
			result = extract_sql(item, _depth + 1)
			if result:
				return result
	return None


def format_sql(sql: str, dialect: str = "bigquery") -> str:
	"""
	Standardizes, sanitizes, and beautifies SQL with a strict 'Data-Loss' protection.
	"""
	if not sql or not sql.strip():
		return sql

	# Phase 1: Deep Sanitization & Standardization
	# Convert escaped chars, standardize tabs to 4 spaces, remove carriage returns
	raw_sql = (sql.replace("\\n", "\n")
				  .replace("\r", "")
				  .replace("\t", "	")
				  .replace("\xa0", " ")
				  .replace("\\xa0", " "))

	# Logic Gate: Skip formatting if script is massive or library missing
	if not _HAS_SQLGLOT or len(raw_sql) > MAX_FORMAT_SIZE:
		return raw_sql

	formatted = ""
	try:
		# Phase 2: Script-Level Transpilation
		statements = sqlglot.transpile(raw_sql, read=dialect, write=dialect, pretty=True)
		if not statements:
			return raw_sql

		formatted = ";\n\n".join(s for s in statements if s)

		if formatted and (len(statements) > 1 or raw_sql.strip().endswith(";")):
			formatted += ";"

	except Exception:
		try:
			# Generic Fallback
			statements = sqlglot.transpile(raw_sql, pretty=True)
			formatted = ";\n\n".join(s for s in statements if s)
			if formatted and (len(statements) > 1 or raw_sql.strip().endswith(";")):
				formatted += ";"
		except Exception:
			return raw_sql

	# Phase 3: Post-Format Polish
	formatted = formatted.replace("\xa0", " ").replace("\\xa0", " ")

	# Phase 4: Data-Loss Safety Guardrail
	# Revert if formatting drops > 15% of content or results in empty string
	if (not formatted.strip() and raw_sql.strip()) or (len(formatted) < len(raw_sql) * 0.85):
		return raw_sql

	return formatted


def is_ddl_dml(sql: str) -> bool:
	"""Identify state-changing commands while ignoring comments."""
	if not sql:
		return False

	# Strip comments (Single line and Block)
	clean = re.sub(r'(--.*)|(/\*[\s\S]*?\*/)', '', sql).strip().upper()
	if not clean:
		return False

	forbidden = ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "MERGE",
				 "TRUNCATE", "ALTER", "GRANT", "REVOKE", "CALL", "EXPORT")

	# Split by semicolon to inspect every statement in the script
	statements = [s.strip() for s in clean.split(';') if s.strip()]
	for stmt in statements:
		if any(stmt.startswith(kw) for kw in forbidden):
			return True
	return False