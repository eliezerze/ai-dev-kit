"""
SQL - SQL Warehouse Operations

Functions for executing SQL queries, managing SQL warehouses, and getting table statistics.

Low-level functions are in sql.py, warehouse.py, table_stats.py.
High-level API with business logic are in sql_api.py.
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

# High-level API (used by MCP and CLI)
from .sql_api import (
    execute_sql as api_execute_sql,
    execute_sql_multi as api_execute_sql_multi,
    manage_warehouse as api_manage_warehouse,
    get_table_stats_and_schema as api_get_table_stats_and_schema,
    get_volume_folder_details as api_get_volume_folder_details,
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
    "api_execute_sql",
    "api_execute_sql_multi",
    "api_manage_warehouse",
    "api_get_table_stats_and_schema",
    "api_get_volume_folder_details",
    "format_results_markdown",
]
