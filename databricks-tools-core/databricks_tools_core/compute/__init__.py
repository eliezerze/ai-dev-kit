"""
Compute - Code Execution and Compute Management Operations

Functions for executing code on Databricks clusters and serverless compute,
and for creating, modifying, and deleting compute resources.

Low-level functions are in execution.py, serverless.py, manage.py.
High-level API with business logic are in compute_api.py.
"""

from .execution import (
    ExecutionResult,
    NoRunningClusterError,
    list_clusters,
    get_best_cluster,
    start_cluster,
    get_cluster_status,
    create_context,
    destroy_context,
    execute_databricks_command,
    run_file_on_databricks,
)

from .serverless import (
    ServerlessRunResult,
    run_code_on_serverless,
)

from .manage import (
    create_cluster,
    modify_cluster,
    terminate_cluster,
    delete_cluster,
    list_node_types,
    list_spark_versions,
    create_sql_warehouse,
    modify_sql_warehouse,
    delete_sql_warehouse,
)

# High-level API (used by MCP and CLI)
from .compute_api import (
    execute_code as api_execute_code,
    manage_cluster as api_manage_cluster,
    manage_sql_warehouse as api_manage_sql_warehouse,
    list_compute as api_list_compute,
)

__all__ = [
    # Low-level execution
    "ExecutionResult",
    "NoRunningClusterError",
    "list_clusters",
    "get_best_cluster",
    "start_cluster",
    "get_cluster_status",
    "create_context",
    "destroy_context",
    "execute_databricks_command",
    "run_file_on_databricks",
    # Serverless
    "ServerlessRunResult",
    "run_code_on_serverless",
    # Low-level cluster management
    "create_cluster",
    "modify_cluster",
    "terminate_cluster",
    "delete_cluster",
    "list_node_types",
    "list_spark_versions",
    "create_sql_warehouse",
    "modify_sql_warehouse",
    "delete_sql_warehouse",
    # High-level API
    "api_execute_code",
    "api_manage_cluster",
    "api_manage_sql_warehouse",
    "api_list_compute",
]
