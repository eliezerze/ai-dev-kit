"""SQL CLI commands - Execute SQL queries and manage warehouses.

Commands:
    aidevkit sql execute --query "SELECT 1"
    aidevkit sql execute-multi --content "SELECT 1; SELECT 2"
    aidevkit sql warehouse list
    aidevkit sql warehouse get-best
    aidevkit sql table-stats --catalog my_catalog --schema my_schema
    aidevkit sql volume-details --path /Volumes/catalog/schema/volume/folder
"""

import json
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from databricks_tools_core.sql.sql_api import (
    execute_sql as _execute_sql,
    execute_sql_multi as _execute_sql_multi,
    manage_warehouse as _manage_warehouse,
    get_table_stats_and_schema as _get_table_stats_and_schema,
    get_volume_folder_details as _get_volume_folder_details,
)

app = typer.Typer(help="Execute SQL queries and manage warehouses")
warehouse_app = typer.Typer(help="Manage SQL warehouses")
app.add_typer(warehouse_app, name="warehouse")

console = Console()


def _output_result(result, output_format: str = "auto"):
    """Output result in the specified format."""
    if output_format == "json":
        print(json.dumps(result, indent=2, default=str))
    elif isinstance(result, str):
        # Already formatted (e.g., markdown table)
        print(result)
    elif isinstance(result, dict):
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


@app.command("execute")
def execute(
    query: str = typer.Option(..., "--query", "-q", help="SQL query to execute"),
    warehouse_id: Optional[str] = typer.Option(None, "--warehouse-id", "-w", help="SQL warehouse ID"),
    catalog: Optional[str] = typer.Option(None, "--catalog", "-c", help="Default catalog"),
    schema: Optional[str] = typer.Option(None, "--schema", "-s", help="Default schema"),
    timeout: int = typer.Option(180, "--timeout", "-t", help="Query timeout in seconds"),
    query_tags: Optional[str] = typer.Option(None, "--tags", help="Query tags"),
    output_format: str = typer.Option("markdown", "--format", "-f", help="Output format: markdown or json"),
):
    """Execute a SQL query on Databricks warehouse.

    Auto-selects warehouse if not provided.

    Example:
        aidevkit sql execute --query "SELECT * FROM my_table LIMIT 10"
        aidevkit sql execute -q "SELECT 1" -f json
    """
    result = _execute_sql(
        sql_query=query,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        timeout=timeout,
        query_tags=query_tags,
        output_format=output_format,
    )
    _output_result(result, output_format)


@app.command("execute-multi")
def execute_multi(
    content: str = typer.Option(..., "--content", "-c", help="SQL content with multiple statements"),
    warehouse_id: Optional[str] = typer.Option(None, "--warehouse-id", "-w", help="SQL warehouse ID"),
    catalog: Optional[str] = typer.Option(None, "--catalog", help="Default catalog"),
    schema: Optional[str] = typer.Option(None, "--schema", "-s", help="Default schema"),
    timeout: int = typer.Option(180, "--timeout", "-t", help="Query timeout per statement"),
    max_workers: int = typer.Option(4, "--workers", help="Max parallel workers"),
    query_tags: Optional[str] = typer.Option(None, "--tags", help="Query tags"),
    output_format: str = typer.Option("markdown", "--format", "-f", help="Output format: markdown or json"),
):
    """Execute multiple SQL statements with dependency-aware parallelism.

    Independent queries run in parallel.

    Example:
        aidevkit sql execute-multi --content "SELECT 1; SELECT 2; SELECT 3"
    """
    result = _execute_sql_multi(
        sql_content=content,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        timeout=timeout,
        max_workers=max_workers,
        query_tags=query_tags,
        output_format=output_format,
    )
    _output_result(result, "json")


@warehouse_app.command("list")
def warehouse_list():
    """List all SQL warehouses.

    Example:
        aidevkit sql warehouse list
    """
    result = _manage_warehouse(action="list")

    if "error" in result:
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    warehouses = result.get("warehouses", [])
    if not warehouses:
        rprint("[yellow]No warehouses found[/yellow]")
        return

    table = Table(title="SQL Warehouses")
    table.add_column("ID", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("State", style="yellow")
    table.add_column("Size")

    for wh in warehouses:
        table.add_row(
            wh.get("id", ""),
            wh.get("name", ""),
            wh.get("state", ""),
            wh.get("size", wh.get("cluster_size", "")),
        )

    console.print(table)


@warehouse_app.command("get-best")
def warehouse_get_best():
    """Get the best available warehouse ID.

    Prefers running warehouses, then starting ones, with smaller sizes preferred.

    Example:
        aidevkit sql warehouse get-best
    """
    result = _manage_warehouse(action="get_best")

    if result.get("error"):
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    warehouse_id = result.get("warehouse_id")
    if warehouse_id:
        rprint(f"[green]Best warehouse ID: {warehouse_id}[/green]")
    else:
        rprint("[yellow]No available warehouse found[/yellow]")


@app.command("table-stats")
def table_stats(
    catalog: str = typer.Option(..., "--catalog", "-c", help="Catalog name"),
    schema: str = typer.Option(..., "--schema", "-s", help="Schema name"),
    tables: Optional[str] = typer.Option(None, "--tables", "-t", help="Table names (comma-separated or glob patterns)"),
    stat_level: str = typer.Option("SIMPLE", "--level", "-l", help="Stat level: NONE, SIMPLE, DETAILED"),
    warehouse_id: Optional[str] = typer.Option(None, "--warehouse-id", "-w", help="SQL warehouse ID"),
):
    """Get schema and statistics for tables.

    stat_level options:
    - NONE: Schema only
    - SIMPLE: Schema + row count (default)
    - DETAILED: Schema + cardinality/min/max/histograms

    Example:
        aidevkit sql table-stats --catalog my_catalog --schema my_schema
        aidevkit sql table-stats -c catalog -s schema -t "table1,table2" -l DETAILED
    """
    table_names = tables.split(",") if tables else None

    result = _get_table_stats_and_schema(
        catalog=catalog,
        schema=schema,
        table_names=table_names,
        table_stat_level=stat_level,
        warehouse_id=warehouse_id,
    )
    _output_result(result, "json")


@app.command("volume-details")
def volume_details(
    path: str = typer.Option(..., "--path", "-p", help="Volume folder path"),
    format: str = typer.Option("parquet", "--format", "-f", help="File format: parquet/csv/json/delta/file"),
    stat_level: str = typer.Option("SIMPLE", "--level", "-l", help="Stat level: NONE, SIMPLE, DETAILED"),
    warehouse_id: Optional[str] = typer.Option(None, "--warehouse-id", "-w", help="SQL warehouse ID"),
):
    """Get schema/stats for data files in a Volume folder.

    Example:
        aidevkit sql volume-details --path /Volumes/catalog/schema/volume/data
        aidevkit sql volume-details -p /Volumes/cat/sch/vol/data -f csv -l DETAILED
    """
    result = _get_volume_folder_details(
        volume_path=path,
        format=format,
        table_stat_level=stat_level,
        warehouse_id=warehouse_id,
    )
    _output_result(result, "json")


if __name__ == "__main__":
    app()
