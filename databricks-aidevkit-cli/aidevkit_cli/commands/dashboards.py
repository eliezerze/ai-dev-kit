"""Dashboards CLI commands - Manage AI/BI dashboards.

Commands:
    aidevkit dashboards create-or-update --name "My Dashboard" --parent-path /Workspace/... --definition '{...}'
    aidevkit dashboards get --dashboard-id abc123
    aidevkit dashboards list
    aidevkit dashboards delete --dashboard-id abc123
    aidevkit dashboards publish --dashboard-id abc123 --warehouse-id xyz
    aidevkit dashboards unpublish --dashboard-id abc123
"""

import json
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from databricks_tools_core.aibi_dashboards import (
    create_or_update_dashboard as _create_or_update_dashboard,
    get_dashboard as _get_dashboard,
    list_dashboards as _list_dashboards,
    trash_dashboard as _trash_dashboard,
    publish_dashboard as _publish_dashboard,
    unpublish_dashboard as _unpublish_dashboard,
)

app = typer.Typer(help="Manage AI/BI dashboards")
console = Console()


def _output_result(result, output_format: str = "json"):
    """Output result in the specified format."""
    if output_format == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


@app.command("create-or-update")
def create_or_update(
    name: str = typer.Option(..., "--name", "-n", help="Dashboard display name"),
    parent_path: str = typer.Option(..., "--parent-path", "-p", help="Workspace folder path"),
    definition: str = typer.Option(..., "--definition", "-d", help="Dashboard JSON definition"),
    warehouse_id: str = typer.Option(..., "--warehouse-id", "-w", help="SQL warehouse ID"),
    publish: bool = typer.Option(True, "--publish/--no-publish", help="Auto-publish after create"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create or update a dashboard.

    IMPORTANT: Test all queries with 'aidevkit sql execute' before creating dashboard!

    Example:
        aidevkit dashboards create-or-update --name "Sales Report" --parent-path /Workspace/Users/me --definition '{"pages": [...]}' --warehouse-id abc123
    """
    result = _create_or_update_dashboard(
        display_name=name,
        parent_path=parent_path,
        serialized_dashboard=definition,
        warehouse_id=warehouse_id,
        publish=publish,
    )
    _output_result(result, output_format)


@app.command("get")
def get(
    dashboard_id: str = typer.Option(..., "--dashboard-id", "-d", help="Dashboard ID"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get dashboard details.

    Example:
        aidevkit dashboards get --dashboard-id abc123
    """
    result = _get_dashboard(dashboard_id=dashboard_id)
    _output_result(result, output_format)


@app.command("list")
def list_dashboards():
    """List all dashboards.

    Example:
        aidevkit dashboards list
    """
    result = _list_dashboards(page_size=200)

    dashboards = result.get("dashboards", []) if isinstance(result, dict) else result
    if not dashboards:
        rprint("[yellow]No dashboards found[/yellow]")
        return

    table = Table(title="AI/BI Dashboards")
    table.add_column("Dashboard ID", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Path", style="yellow")

    for d in dashboards[:50]:
        table.add_row(
            d.get("dashboard_id", ""),
            d.get("display_name", ""),
            d.get("path", ""),
        )

    console.print(table)
    if len(dashboards) > 50:
        rprint(f"[dim]Showing 50 of {len(dashboards)} dashboards[/dim]")


@app.command("delete")
def delete(
    dashboard_id: str = typer.Option(..., "--dashboard-id", "-d", help="Dashboard ID to delete"),
):
    """Delete a dashboard (moves to trash).

    Example:
        aidevkit dashboards delete --dashboard-id abc123
    """
    result = _trash_dashboard(dashboard_id=dashboard_id)

    if isinstance(result, dict) and result.get("error"):
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    rprint(f"[green]Dashboard {dashboard_id} moved to trash[/green]")


@app.command("publish")
def publish(
    dashboard_id: str = typer.Option(..., "--dashboard-id", "-d", help="Dashboard ID"),
    warehouse_id: str = typer.Option(..., "--warehouse-id", "-w", help="SQL warehouse ID"),
    embed_credentials: bool = typer.Option(True, "--embed-credentials/--no-embed-credentials", help="Allow users without data access to view"),
):
    """Publish a dashboard.

    Example:
        aidevkit dashboards publish --dashboard-id abc123 --warehouse-id xyz
    """
    result = _publish_dashboard(
        dashboard_id=dashboard_id,
        warehouse_id=warehouse_id,
        embed_credentials=embed_credentials,
    )

    if isinstance(result, dict) and result.get("error"):
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    rprint(f"[green]Dashboard {dashboard_id} published[/green]")


@app.command("unpublish")
def unpublish(
    dashboard_id: str = typer.Option(..., "--dashboard-id", "-d", help="Dashboard ID"),
):
    """Unpublish a dashboard.

    Example:
        aidevkit dashboards unpublish --dashboard-id abc123
    """
    result = _unpublish_dashboard(dashboard_id=dashboard_id)

    if isinstance(result, dict) and result.get("error"):
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    rprint(f"[green]Dashboard {dashboard_id} unpublished[/green]")


if __name__ == "__main__":
    app()
