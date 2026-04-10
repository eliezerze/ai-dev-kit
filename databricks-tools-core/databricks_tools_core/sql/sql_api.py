"""SQL workflows - High-level SQL operations with business logic.

This module contains the business logic for SQL operations, used by both
the MCP server and CLI. It wraps the low-level functions from the sql module
with additional features like result formatting and action dispatch.

Tools:
- execute_sql: Execute SQL query with output formatting
- execute_sql_multi: Execute multiple SQL statements with parallelism
- manage_warehouse: List warehouses or get best available
- get_table_stats_and_schema: Get schema and stats for tables
- get_volume_folder_details: Get schema for volume files
"""

from typing import Any, Dict, List, Optional, Union

from .sql import execute_sql as _execute_sql
from .sql import execute_sql_multi as _execute_sql_multi
from .warehouse import list_warehouses as _list_warehouses
from .warehouse import get_best_warehouse as _get_best_warehouse
from .table_stats import get_table_stats_and_schema as _get_table_stats_and_schema
from .table_stats import get_volume_folder_details as _get_volume_folder_details
from .sql_utils import TableStatLevel


def format_results_markdown(rows: List[Dict[str, Any]]) -> str:
    """Format SQL results as a markdown table.

    Markdown tables state column names once in the header instead of repeating
    them on every row (as JSON does), reducing token usage by ~50%.

    Args:
        rows: List of row dicts from the SQL executor.

    Returns:
        Markdown table string, or "(no results)" if empty.
    """
    if not rows:
        return "(no results)"

    columns = list(rows[0].keys())

    # Build header
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"

    # Build rows — convert None to empty string, stringify everything
    data_lines = []
    for row in rows:
        cells = []
        for col in columns:
            val = row.get(col)
            cell = "" if val is None else str(val)
            # Escape pipe characters inside cell values
            cell = cell.replace("|", "\\|")
            cells.append(cell)
        data_lines.append("| " + " | ".join(cells) + " |")

    parts = [header, separator] + data_lines
    # Append row count for awareness
    parts.append(f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})")
    return "\n".join(parts)


def execute_sql(
    sql_query: str,
    warehouse_id: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    timeout: int = 180,
    query_tags: Optional[str] = None,
    output_format: str = "markdown",
) -> Union[str, List[Dict[str, Any]]]:
    """Execute SQL query on Databricks warehouse.

    Auto-selects warehouse if not provided. Use for SELECT/INSERT/UPDATE/table DDL.
    For catalog/schema/volume DDL, use manage_uc_objects.

    Args:
        sql_query: The SQL query to execute.
        warehouse_id: SQL warehouse ID. Auto-selected if not provided.
        catalog: Default catalog for unqualified table names.
        schema: Default schema for unqualified table names.
        timeout: Query timeout in seconds.
        query_tags: Optional tags for the query.
        output_format: "markdown" (default, 50% smaller) or "json".

    Returns:
        Query results as markdown table or list of dicts.
    """
    rows = _execute_sql(
        sql_query=sql_query,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        timeout=timeout,
        query_tags=query_tags,
    )
    if output_format == "json":
        return rows
    return format_results_markdown(rows)


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
    """Execute multiple SQL statements with dependency-aware parallelism.

    Independent queries run in parallel. For catalog/schema/volume DDL,
    use manage_uc_objects instead.

    Args:
        sql_content: SQL content with multiple statements (semicolon-separated).
        warehouse_id: SQL warehouse ID. Auto-selected if not provided.
        catalog: Default catalog for unqualified table names.
        schema: Default schema for unqualified table names.
        timeout: Query timeout in seconds per statement.
        max_workers: Maximum parallel workers.
        query_tags: Optional tags for the queries.
        output_format: "markdown" (default) or "json" for sample_results.

    Returns:
        Dict with execution results including sample_results per query.
    """
    result = _execute_sql_multi(
        sql_content=sql_content,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        timeout=timeout,
        max_workers=max_workers,
        query_tags=query_tags,
    )
    # Format sample_results in each query result if markdown requested
    if output_format != "json" and "results" in result:
        for query_result in result["results"].values():
            sample = query_result.get("sample_results")
            if sample and isinstance(sample, list) and len(sample) > 0:
                query_result["sample_results"] = format_results_markdown(sample)
    return result


def manage_warehouse(
    action: str = "get_best",
) -> Dict[str, Any]:
    """Manage SQL warehouses: list, get_best.

    Actions:
    - list: List all SQL warehouses.
      Returns: {warehouses: [{id, name, state, size, ...}]}.
    - get_best: Get best available warehouse ID. Prefers running, then starting, smaller sizes.
      Returns: {warehouse_id} or {warehouse_id: null, error}.

    Args:
        action: The action to perform (list, get_best).

    Returns:
        Dict with warehouse information or error.
    """
    act = action.lower()

    if act == "list":
        return {"warehouses": _list_warehouses()}

    elif act == "get_best":
        warehouse_id = _get_best_warehouse()
        if warehouse_id:
            return {"warehouse_id": warehouse_id}
        raise ValueError("No available warehouses found. Create a SQL warehouse or start an existing one.")

    else:
        return {"error": f"Invalid action '{action}'. Valid actions: list, get_best"}


def get_table_stats_and_schema(
    catalog: str,
    schema: str,
    table_names: Optional[List[str]] = None,
    table_stat_level: str = "SIMPLE",
    warehouse_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Get schema and stats for tables.

    Args:
        catalog: The catalog name.
        schema: The schema name.
        table_names: List of table names or glob patterns. None for all tables.
        table_stat_level: NONE (schema only), SIMPLE (default, +row count),
                         DETAILED (+cardinality/min/max/histograms).
        warehouse_id: SQL warehouse ID. Auto-selected if not provided.

    Returns:
        Dict with table schema and statistics.
    """
    # Convert string to enum
    level = TableStatLevel[table_stat_level.upper()]
    result = _get_table_stats_and_schema(
        catalog=catalog,
        schema=schema,
        table_names=table_names,
        table_stat_level=level,
        warehouse_id=warehouse_id,
    )
    # Convert to dict for JSON serialization
    return result.model_dump(exclude_none=True) if hasattr(result, "model_dump") else result


def get_volume_folder_details(
    volume_path: str,
    format: str = "parquet",
    table_stat_level: str = "SIMPLE",
    warehouse_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Get schema/stats for data files in Volume folder.

    Args:
        volume_path: Path to the volume folder.
        format: File format - parquet/csv/json/delta/file.
        table_stat_level: NONE, SIMPLE (default), or DETAILED.
        warehouse_id: SQL warehouse ID. Auto-selected if not provided.

    Returns:
        Dict with volume folder schema and statistics.
    """
    level = TableStatLevel[table_stat_level.upper()]
    result = _get_volume_folder_details(
        volume_path=volume_path,
        format=format,
        table_stat_level=level,
        warehouse_id=warehouse_id,
    )
    return result.model_dump(exclude_none=True) if hasattr(result, "model_dump") else result
