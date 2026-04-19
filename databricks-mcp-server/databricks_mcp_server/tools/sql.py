"""SQL tools - Execute SQL queries and get table information.

Tools:
- execute_sql: Single SQL query
- execute_sql_multi: Multiple SQL statements with parallel execution
- manage_warehouse: list, get_best
- get_table_stats_and_schema: Schema and stats for tables
- get_volume_folder_details: Schema for volume files
"""

from typing import Any, Dict, List, Optional, Union

from databricks_tools_core.sql import (
    execute_sql as _execute_sql,
    execute_sql_multi as _execute_sql_multi,
    list_warehouses as _list_warehouses,
    get_best_warehouse as _get_best_warehouse,
    get_table_stats_and_schema as _get_table_stats_and_schema,
    get_volume_folder_details as _get_volume_folder_details,
    TableStatLevel,
)

from ..server import mcp
from ..table_filter import get_table_filter


def _format_results_markdown(rows: List[Dict[str, Any]]) -> str:
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


@mcp.tool(timeout=60)
def execute_sql(
    sql_query: str,
    warehouse_id: str = None,
    catalog: str = None,
    schema: str = None,
    timeout: int = 180,
    query_tags: str = None,
    output_format: str = "markdown",
) -> Union[str, List[Dict[str, Any]]]:
    """Execute SQL query on Databricks warehouse. Auto-selects warehouse if not provided.

    Use for SELECT/INSERT/UPDATE/table DDL. For catalog/schema/volume DDL, use manage_uc_objects.
    output_format: "markdown" (default, 50% smaller) or "json"."""
    tag_filter = get_table_filter()
    if tag_filter.is_enabled:
        tag_filter.validate_sql(
            sql_query,
            catalog_context=catalog,
            schema_context=schema,
        )

    rows = _execute_sql(
        sql_query=sql_query,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        timeout=timeout,
        query_tags=query_tags,
    )

    if tag_filter.is_enabled:
        rows = tag_filter.filter_show_results(
            sql_query, rows,
            catalog_context=catalog,
            schema_context=schema,
        )

    if output_format == "json":
        return rows
    return _format_results_markdown(rows)


@mcp.tool(timeout=120)
def execute_sql_multi(
    sql_content: str,
    warehouse_id: str = None,
    catalog: str = None,
    schema: str = None,
    timeout: int = 180,
    max_workers: int = 4,
    query_tags: str = None,
    output_format: str = "markdown",
) -> Dict[str, Any]:
    """Execute multiple SQL statements with dependency-aware parallelism. Independent queries run in parallel.

    For catalog/schema/volume DDL, use manage_uc_objects instead."""
    tag_filter = get_table_filter()
    if tag_filter.is_enabled:
        tag_filter.validate_sql(
            sql_content,
            catalog_context=catalog,
            schema_context=schema,
        )

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
                query_result["sample_results"] = _format_results_markdown(sample)
    return result


@mcp.tool(timeout=30)
def manage_warehouse(
    action: str = "get_best",
) -> Union[str, List[Dict[str, Any]], Dict[str, Any]]:
    """Manage SQL warehouses: list, get_best.

    Actions:
    - list: List all SQL warehouses.
      Returns: {warehouses: [{id, name, state, size, ...}]}.
    - get_best: Get best available warehouse ID. Prefers running, then starting, smaller sizes.
      Returns: {warehouse_id} or {warehouse_id: null, error}."""
    act = action.lower()

    if act == "list":
        return {"warehouses": _list_warehouses()}

    elif act == "get_best":
        warehouse_id = _get_best_warehouse()
        if warehouse_id:
            return {"warehouse_id": warehouse_id}
        return {"warehouse_id": None, "error": "No available warehouses found"}

    else:
        return {"error": f"Invalid action '{action}'. Valid actions: list, get_best"}


@mcp.tool(timeout=60)
def get_table_stats_and_schema(
    catalog: str,
    schema: str,
    table_names: List[str] = None,
    table_stat_level: str = "SIMPLE",
    warehouse_id: str = None,
) -> Dict[str, Any]:
    """Get schema and stats for tables. table_stat_level: NONE (schema only), SIMPLE (default, +row count), DETAILED (+cardinality/min/max/histograms).

    table_names: list or glob patterns, None=all tables."""
    tag_filter = get_table_filter()
    if tag_filter.is_enabled and table_names:
        blocked = [
            t for t in table_names
            if not any(c in t for c in ["*", "?", "[", "]"])
            and not tag_filter.is_table_allowed(catalog, schema, t)
        ]
        if blocked:
            tag_repr = (
                f"{tag_filter.tag_name}={tag_filter.tag_value}"
                if tag_filter.tag_value else tag_filter.tag_name
            )
            return {
                "error": True,
                "message": (
                    f"Access denied. Tables not tagged with "
                    f"'{tag_repr}': {', '.join(blocked)}"
                ),
            }

    level = TableStatLevel[table_stat_level.upper()]
    result = _get_table_stats_and_schema(
        catalog=catalog,
        schema=schema,
        table_names=table_names,
        table_stat_level=level,
        warehouse_id=warehouse_id,
    )

    result_dict = result.model_dump(exclude_none=True) if hasattr(result, "model_dump") else result

    if tag_filter.is_enabled and "tables" in result_dict:
        allowed = tag_filter.get_allowed_tables()
        result_dict["tables"] = [
            t for t in result_dict["tables"]
            if (catalog.lower(), schema.lower(), t.get("name", "").split(".")[-1].lower()) in allowed
        ]

    return result_dict


@mcp.tool(timeout=60)
def get_volume_folder_details(
    volume_path: str,
    format: str = "parquet",
    table_stat_level: str = "SIMPLE",
    warehouse_id: str = None,
) -> Dict[str, Any]:
    """Get schema/stats for data files in Volume folder. format: parquet/csv/json/delta/file."""
    level = TableStatLevel[table_stat_level.upper()]
    result = _get_volume_folder_details(
        volume_path=volume_path,
        format=format,
        table_stat_level=level,
        warehouse_id=warehouse_id,
    )
    return result.model_dump(exclude_none=True) if hasattr(result, "model_dump") else result
