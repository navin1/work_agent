"""Mapping validation panel — traceability matrix of business rules vs SQL implementation."""

import json

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from core import monaco

_VERDICT = {
	"PASS":		 ("🟢", "#1B8A3E", "#F0FFF4"),
	"FAIL":		 ("🔴", "#C41230", "#FFF0F0"),
	"PARTIAL":		("🟡", "#B38600", "#FFFBEB"),
	"NOT_APPLICABLE": ("⚪", "#6B7280", "#F9FAFB"),
	"NOT_EVALUATED": ("🔵", "#1D4ED8", "#EFF6FF"),
	"ERROR":		 ("⚠️", "#9333EA", "#FAF5FF"),
}
_CONFIDENCE = {
	"HIGH": ("HIGH",	"#1B8A3E"),
	"MEDIUM": ("MEDIUM", "#B38600"),
	"LOW":	("LOW ⚠️", "#C41230"),
}
_render_count = 0


def render_mapping_validation(raw_json: str) -> None:
	global _render_count
	_render_count += 1
	uid = str(_render_count)

	try:
		data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
	except Exception:
		st.error("Could not parse mapping validation result.")
		return

	if "error" in data:
		st.error(f"Mapping validation error: {data['error']}")
		if "available_columns" in data:
			st.caption("Columns found in the Excel file:")
			st.code(", ".join(data["available_columns"]))
			st.info(
				"Set **mapping_columns** in `config/excel_mapping.json` for this file "
				"to specify which columns hold target, source, and logic."
			)
		if "column_config" in data:
			with st.expander("Column detection details"):
				st.json(data["column_config"])
		return

	# Handle bulk validation output
	if data.get("is_bulk"):
		st.markdown(f"### Bulk Validation Results: {data.get('files_processed')} file(s)")
		
		ovr = data.get("overall_summary", {})
		c1, c2, c3, c4, c5, c6 = st.columns(6)
		c1.metric("🟢 PASS",	ovr.get("pass", 0))
		c2.metric("🔴 FAIL",	ovr.get("fail", 0))
		c3.metric("🟡 PARTIAL", ovr.get("partial", 0))
		c4.metric("⚪ N/A",	 ovr.get("not_applicable", 0))
		c5.metric("🔵 No SQL", ovr.get("not_evaluated", 0))
		c6.metric("Total",	 ovr.get("total", 0))
		
		for res in data.get("results", []):
			st.divider()
			st.markdown(f"#### {res.get('mapping_file', 'Unknown file')}")
			_render_single_file_validation(res, uid)
	else:
		_render_single_file_validation(data, uid)


def _render_single_file_validation(data: dict, uid: str) -> None:
	summary	= data.get("summary", {})
	groups	 = data.get("bq_table_groups", [])
	col_config = data.get("column_config", {})
	sql_info = data.get("sql_structure", {})
	file_label = data.get("mapping_file", "")
	composer_env = data.get("composer_env", "local")

	# ── Session state for human-reviewed tracking (session-scoped) ────────────
	reviewed_key = f"mv_reviewed_{data.get('duckdb_table', uid)}_{file_label}"
	if reviewed_key not in st.session_state:
		st.session_state[reviewed_key] = set()

	# ── Summary cards ─────────────────────────────────────────────────────────
	c1, c2, c3, c4, c5, c6 = st.columns(6)
	c1.metric("🟢 PASS",	summary.get("pass",		 0))
	c2.metric("🔴 FAIL",	summary.get("fail",		 0))
	c3.metric("🟡 PARTIAL", summary.get("partial",		0))
	c4.metric("⚪ N/A",	 summary.get("not_applicable", 0))
	c5.metric("🔵 No SQL", summary.get("not_evaluated", 0))
	c6.metric("Total",	 summary.get("total",		 0))

	low_conf = summary.get("low_confidence", 0)
	if low_conf:
		st.warning(
			f"⚠️ **{low_conf} rule(s) have LOW confidence** — "
			"human review is required before these can be considered validated."
		)
	if data.get("sql_fetch_error"):
		st.warning(f"SQL fetch issue: {data['sql_fetch_error']}")

	# ── Context strip ─────────────────────────────────────────────────────────
	ctx_parts = []
	if file_label:
		ctx_parts.append(f"File: **{file_label}**")
	if data.get("dag_id"):
		ctx_parts.append(f"DAG: **{data['dag_id']}**")
	if data.get("composer_env"):
		ctx_parts.append(f"Env: **{data['composer_env']}**")
	if sql_info.get("tasks_evaluated"):
		ctx_parts.append(f"Tasks evaluated: **{len(sql_info['tasks_evaluated'])}**")
	if sql_info.get("cte_count"):
		ctx_parts.append(f"CTEs: **{sql_info['cte_count']}**")
	if sql_info.get("join_count"):
		ctx_parts.append(f"JOINs: **{sql_info['join_count']}**")
	if ctx_parts:
		st.caption(" · ".join(ctx_parts))

	if sql_info.get("parse_errors"):
		with st.expander(f"⚠️ {len(sql_info['parse_errors'])} SQL parse error(s)"):
			for err in sql_info["parse_errors"]:
				st.code(err or "(unknown)")

	# ── Column mapping config disclosure ──────────────────────────────────────
	with st.expander("📋 Column role mapping", expanded=False):
		det = col_config.get("detected_by", {})
		rows = []
		for role in ("target", "source", "logic", "bq_table"):
			col_name = col_config.get(role) or "(not found)"
			how	 = det.get(role, "auto")
			rows.append({"Role": role, "Mapped to column": col_name, "Detected by": how})
		supp = col_config.get("logic_supplementary") or []
		if supp:
			rows.append({
				"Role": "logic_supplementary",
				"Mapped to column": ", ".join(supp),
				"Detected by": "auto",
			})
		st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
		st.caption(
			"Override any role by setting `mapping_columns` in `config/excel_mapping.json`."
		)

	# ── Mapping Summary Section ───────────────────────────────────────────────
	
	# We will compute the table data first
	summary_rows = []
	for group in groups:
		for rule in group.get("rules", []):
			target_str = ", ".join(rule.get("target_columns") or [])
			summary_rows.append({
				"column": target_str,
				"verdict": rule.get("verdict", ""),
				"confidence": rule.get("confidence_tier", ""),
				"reason": rule.get("reason", "")
			})
	
	summary_df = pd.DataFrame(summary_rows)
	
	with st.expander("📊 Mapping Summary", expanded=False):
		st.dataframe(summary_df, hide_index=True, use_container_width=True)

	# ── BQ table groups ───────────────────────────────────────────────────────
	for group in groups:
		bq_label = group.get("bq_table", "Unknown")
		rules	= group.get("rules", [])
		if not rules:
			continue

		pass_n = sum(1 for r in rules if r["verdict"] == "PASS")
		fail_n = sum(1 for r in rules if r["verdict"] == "FAIL")
		part_n = sum(1 for r in rules if r["verdict"] == "PARTIAL")
		na_n = sum(1 for r in rules if r["verdict"] == "NOT_APPLICABLE")
		ne_n = sum(1 for r in rules if r["verdict"] == "NOT_EVALUATED")
		low_n = sum(
			1 for r in rules
			if r.get("confidence_tier") == "LOW"
			and r["verdict"] not in ("NOT_APPLICABLE", "NOT_EVALUATED")
		)

		low_badge = f" · ⚠️ {low_n} low-conf" if low_n else ""
		ne_badge = f" · 🔵 {ne_n} no-sql" if ne_n else ""
		st.markdown(
			f'<div style="background:#F3F4F6;border-left:4px solid #374151;'
			f'padding:8px 14px;border-radius:4px;margin:16px 0 6px;">'
			f'<span style="font-weight:700;font-size:14px;">📦 {bq_label}</span>'
			f'&nbsp;&nbsp;<span style="color:#6B7280;font-size:12px;">'
			f'🟢 {pass_n} &nbsp;🔴 {fail_n} &nbsp;🟡 {part_n} &nbsp;⚪ {na_n}'
			f"{low_badge}{ne_badge}</span></div>",
			unsafe_allow_html=True,
		)

		filter_opts = ["All", "FAIL", "PARTIAL", "PASS", "NOT_APPLICABLE", "NOT_EVALUATED", "ERROR"]
		chosen = st.selectbox(
			"Filter",
			filter_opts,
			key=f"mvf_{uid}_{file_label}_{bq_label[:24]}",
			label_visibility="collapsed",
		)
		visible = rules if chosen == "All" else [r for r in rules if r["verdict"] == chosen]

		for rule in visible:
			rule_id = rule.get("rule_id", 0)
			verdict = rule.get("verdict", "ERROR")
			icon, color, bg = _VERDICT.get(verdict, ("❓", "#6B7280", "#F9FAFB"))
			conf	 = rule.get("confidence_tier", "")
			conf_label, conf_color = _CONFIDENCE.get(conf, (conf, "#6B7280"))
			is_low = conf == "LOW" and verdict not in ("NOT_APPLICABLE", "NOT_EVALUATED")
			reviewed = rule_id in st.session_state[reviewed_key]

			target_str = ", ".join(rule.get("target_columns") or []) or "(unknown)"
			rule_preview = (rule.get("rule_text") or "")
			rule_preview = rule_preview[:80] + "…" if len(rule_preview) > 80 else rule_preview
			cache_badge = " · 💾" if rule.get("cache_hit") else ""
			rev_badge	= " · ✅ reviewed" if reviewed else ""

			header = (
				f'`#{rule_id}` **{target_str}** '
				f'<span style="color:{color};font-weight:700;">{icon} {verdict}</span>'
			)
			if conf:
				header += f' <span style="color:{conf_color};font-size:11px;">{conf_label}</span>'
			if cache_badge or rev_badge:
				header += (
					f' <span style="color:#9CA3AF;font-size:11px;">{cache_badge}{rev_badge}</span>'
				)

			with st.expander(header):
				if is_low and not reviewed:
					st.warning(
						"⚠️ **LOW confidence** — this verdict requires human review "
						"before being considered validated."
					)

				tab_rule, tab_sql, tab_reason, tab_raw = st.tabs(
					["Rule", "SQL Evidence", "AI Reasoning", "Raw"]
				)

				with tab_rule:
					col_a, col_b = st.columns(2)
					with col_a:
						st.markdown("**Target column(s)**")
						st.code(", ".join(rule.get("target_columns") or []) or "(none)")
						st.markdown("**Source column(s)**")
						st.code(", ".join(rule.get("source_columns") or []) or "(not specified)")
					with col_b:
						st.markdown("**Rule type**")
						st.code(rule.get("rule_type") or "(unknown)")
						st.markdown("**Confidence tier**")
						st.markdown(
							f'<span style="color:{conf_color};font-weight:700;font-size:14px;">'
							f"{conf_label}</span>",
							unsafe_allow_html=True,
						)
					st.markdown("**Full rule text**")
					st.info(rule.get("rule_text") or "(empty — marked as N/A)")

				with tab_sql:
					evidence = (rule.get("evidence") or "").strip()
					if evidence:
						h = min(max(120, evidence.count("\n") * 20 + 80), 500)
						components.html(
							monaco.editor(evidence, language="sql", height=h), height=h + 20
						)
					else:
						st.info("No specific SQL evidence was extracted for this rule.")
					rel_ctes = rule.get("relevant_ctes") or []
					rel_cls = rule.get("relevant_clauses") or []
					if rel_ctes:
						st.caption(f"Relevant CTEs evaluated: {', '.join(rel_ctes)}")
					if rel_cls:
						st.caption(f"SQL clauses checked: {', '.join(rel_cls)}")

				with tab_reason:
					reason = (rule.get("reason") or "").strip()
					if reason:
						st.markdown(
							f'<div style="background:{bg};border:1px solid {color};'
							f'border-radius:6px;padding:10px 14px;margin-bottom:10px;">'
							f'<span style="color:{color};font-weight:700;">{icon} {verdict}</span><br>'
							f'<span style="font-size:13px;">{reason}</span></div>',
							unsafe_allow_html=True,
						)
					flags = rule.get("flags") or []
					if flags:
						st.markdown("**Specific issues found:**")
						for flag in flags:
							st.markdown(f"- {flag}")

					if is_low:
						if reviewed:
							st.success("✅ Marked as human-reviewed this session.")
						else:
							if st.button(
								"✅ Mark as Human-Reviewed",
								key=f"rev_{uid}_{file_label}_{rule_id}",
							):
								st.session_state[reviewed_key].add(rule_id)
								st.rerun()

				with tab_raw:
					raw_str = json.dumps(rule, indent=2, default=str)
					h = min(max(200, raw_str.count("\n") * 18), 500)
					components.html(
						monaco.editor(raw_str, language="json", height=h), height=h + 20
					)