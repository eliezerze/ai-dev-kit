"""SQL tools - Execute SQL queries and get table information.

Tools:
- execute_sql: Single SQL query
- execute_sql_multi: Multiple SQL statements with parallel execution
- manage_warehouse: list, get_best
- get_table_stats_and_schema: Schema and stats for tables
- get_volume_folder_details: Schema for volume files

This module is a thin wrapper around databricks_tools_core.sql.workflows.
All business logic lives in the workflows module.
"""

from typing import Any, Dict, List, Optional, Union

from databricks_tools_core.sql.workflows import (
    execute_sql as _execute_sql,
    execute_sql_multi as _execute_sql_multi,
    manage_warehouse as _manage_warehouse,
    get_table_stats_and_schema as _get_table_stats_and_schema,
    get_volume_folder_details as _get_volume_folder_details,
)

from ..server import mcp


# CLI_MAPPING for skill transformation
CLI_MAPPING = {
    "execute_sql": "aidevkit sql execute",
    "execute_sql_multi": "aidevkit sql execute-multi",
    "manage_warehouse": {
        "list": "aidevkit sql warehouse list",
        "get_best": "aidevkit sql warehouse get-best",
    },
    "get_table_stats_and_schema": "aidevkit sql table-stats",
    "get_volume_folder_details": "aidevkit sql volume-details",
}


@mcp.tool(timeout=60)
def execute_sql(
    sql_query: str,
    warehouse_id: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    timeout: int = 180,
    query_tags: Optional[str] = None,
    output_format: str = "markdown",
) -> Union[str, List[Dict[str, Any]]]:
    """Execute SQL query on Databricks warehouse. Auto-selects warehouse if not provided.

    Use for SELECT/INSERT/UPDATE/table DDL. For catalog/schema/volume DDL, use manage_uc_objects.
    output_format: "markdown" (default, 50% smaller) or "json"."""
    return _execute_sql(
        sql_query=sql_query,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        timeout=timeout,
        query_tags=query_tags,
        output_format=output_format,
    )


@mcp.tool(timeout=120)
def execute_sql_multi(
    sql_content: str,
    warehouse_id: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    timeout: int = 180,
    max_workers: int = 4,
    query_tags: Optional[str] = None,
    output_format: str = "markdown",
) -> Dict[str, Any]:
    """Execute multiple SQL statements with dependency-aware parallelism. Independent queries run in parallel.

    For catalog/schema/volume DDL, use manage_uc_objects instead."""
    return _execute_sql_multi(
        sql_content=sql_content,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        timeout=timeout,
        max_workers=max_workers,
        query_tags=query_tags,
        output_format=output_format,
    )


@mcp.tool(timeout=30)
def manage_warehouse(
    action: str = "get_best",
) -> Dict[str, Any]:
    """Manage SQL warehouses: list, get_best.

    Actions:
    - list: List all SQL warehouses.
      Returns: {warehouses: [{id, name, state, size, ...}]}.
    - get_best: Get best available warehouse ID. Prefers running, then starting, smaller sizes.
      Returns: {warehouse_id} or {warehouse_id: null, error}."""
    return _manage_warehouse(action=action)


@mcp.tool(timeout=60)
def get_table_stats_and_schema(
    catalog: str,
    schema: str,
    table_names: Optional[List[str]] = None,
    table_stat_level: str = "SIMPLE",
    warehouse_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Get schema and stats for tables. table_stat_level: NONE (schema only), SIMPLE (default, +row count), DETAILED (+cardinality/min/max/histograms).

    table_names: list or glob patterns, None=all tables."""
    return _get_table_stats_and_schema(
        catalog=catalog,
        schema=schema,
        table_names=table_names,
        table_stat_level=table_stat_level,
        warehouse_id=warehouse_id,
    )


@mcp.tool(timeout=60)
def get_volume_folder_details(
    volume_path: str,
    format: str = "parquet",
    table_stat_level: str = "SIMPLE",
    warehouse_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Get schema/stats for data files in Volume folder. format: parquet/csv/json/delta/file."""
    return _get_volume_folder_details(
        volume_path=volume_path,
        format=format,
        table_stat_level=table_stat_level,
        warehouse_id=warehouse_id,
    )
