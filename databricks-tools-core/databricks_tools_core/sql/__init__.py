"""
SQL - SQL Warehouse Operations

Functions for executing SQL queries, managing SQL warehouses, and getting table statistics.

Low-level functions are in sql.py, warehouse.py, table_stats.py.
High-level workflows with business logic are in workflows.py.
"""

from .sql import execute_sql, execute_sql_multi
from .warehouse import list_warehouses, get_best_warehouse
from .table_stats import get_table_stats_and_schema, get_volume_folder_details
from .sql_utils import (
    SQLExecutionError,
    TableStatLevel,
    TableSchemaResult,
    DataSourceInfo,
    TableInfo,  # Alias for DataSourceInfo (backwards compatibility)
    ColumnDetail,
    VolumeFileInfo,
    VolumeFolderResult,  # Alias for DataSourceInfo (backwards compatibility)
)

# High-level workflows (used by MCP and CLI)
from .workflows import (
    execute_sql as workflow_execute_sql,
    execute_sql_multi as workflow_execute_sql_multi,
    manage_warehouse as workflow_manage_warehouse,
    get_table_stats_and_schema as workflow_get_table_stats_and_schema,
    get_volume_folder_details as workflow_get_volume_folder_details,
    format_results_markdown,
)

__all__ = [
    # SQL execution (low-level)
    "execute_sql",
    "execute_sql_multi",
    # Warehouse management (low-level)
    "list_warehouses",
    "get_best_warehouse",
    # Table statistics (low-level)
    "get_table_stats_and_schema",
    "get_volume_folder_details",
    "TableStatLevel",
    "TableSchemaResult",
    "DataSourceInfo",
    "TableInfo",  # Alias for DataSourceInfo
    "ColumnDetail",
    # Volume folder statistics
    "VolumeFileInfo",
    "VolumeFolderResult",  # Alias for DataSourceInfo
    # Errors
    "SQLExecutionError",
    # High-level workflows
    "workflow_execute_sql",
    "workflow_execute_sql_multi",
    "workflow_manage_warehouse",
    "workflow_get_table_stats_and_schema",
    "workflow_get_volume_folder_details",
    "format_results_markdown",
]
