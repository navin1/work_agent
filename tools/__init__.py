"""Tool registry — all LangChain tools exposed to the agent."""
from tools.excel_tools import (
    query_excel_data,
    list_loaded_tables,
    get_table_schema,
    get_bq_table_for_mapping_file,
    get_dags_for_mapping_file,
    reingest_excel_files,
    trace_from_excel,
)
from tools.bigquery_tools import (
    query_bigquery,
    list_bq_datasets,
    list_bq_tables,
    get_bq_job_stats,
)
from tools.composer_tools import (
    list_composers,
    list_dags,
    get_dag_details,
    get_dag_rendered_files,
    get_dag_run_history,
    get_task_sql,
    get_task_performance,
    get_error_logs,
    get_execution_log,
    list_airflow_jobs,
    get_dag_task_graph,
    get_dag_snapshot_diff,
)
from tools.optimizer_tools import (
    get_sql_flags,
    optimise_sql,
    optimise_dag,
    optimise_all_dag_sqls,
    optimise_sql_file,
)
from tools.testing_tools import (
    compare_query_outputs,
    validate_optimisation,
)
from tools.schema_tools import (
    introspect_bq_schema,
    run_schema_audit,
)
from tools.reconciliation_tools import (
    run_reconciliation,
    get_reconciliation_detail,
    acknowledge_reconciliation_finding,
)
from tools.user_tools import (
    save_query,
    get_saved_queries,
    update_glossary,
    get_glossary,
    pin_workspace,
    save_favorite,
    get_favorites,
)
from tools.code_tools import (
    read_file,
    compare_git_gcs,
    optimise_file,
    optimise_folder,
)
from tools.browse_tools import (
    browse_gcs,
    browse_git,
)
from tools.mapping_validation_tools import (
    validate_mapping_rules,
    validate_mapping_folder,
)

ALL_TOOLS = [
    # Excel / mapping
    query_excel_data,
    list_loaded_tables,
    get_table_schema,
    get_bq_table_for_mapping_file,
    get_dags_for_mapping_file,
    reingest_excel_files,
    trace_from_excel,
    # BigQuery
    query_bigquery,
    list_bq_datasets,
    list_bq_tables,
    get_bq_job_stats,
    # Composer / Airflow
    list_composers,
    list_dags,
    get_dag_details,
    get_dag_rendered_files,
    get_dag_run_history,
    get_task_sql,
    get_task_performance,
    get_error_logs,
    get_execution_log,
    list_airflow_jobs,
    get_dag_task_graph,
    get_dag_snapshot_diff,
    # Optimisation
    get_sql_flags,
    optimise_sql,
    optimise_dag,
    optimise_all_dag_sqls,
    optimise_sql_file,
    # Testing / validation
    compare_query_outputs,
    validate_optimisation,
    # Schema
    introspect_bq_schema,
    run_schema_audit,
    # Reconciliation
    run_reconciliation,
    get_reconciliation_detail,
    acknowledge_reconciliation_finding,
    # Code: read, Git vs GCS comparison, file/folder optimisation
    read_file,
    compare_git_gcs,
    optimise_file,
    optimise_folder,
    # File browser
    browse_gcs,
    browse_git,
    # Mapping validation
    validate_mapping_rules,
    validate_mapping_folder,
    # User
    save_query,
    get_saved_queries,
    update_glossary,
    get_glossary,
    pin_workspace,
    save_favorite,
    get_favorites,
]
