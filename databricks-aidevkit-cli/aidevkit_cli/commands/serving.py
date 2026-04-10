"""Serving CLI commands - Manage model serving endpoints.

Commands:
    aidevkit serving create-or-update --name my-endpoint --served-entities '[...]'
    aidevkit serving get --name my-endpoint
    aidevkit serving list
    aidevkit serving delete --name my-endpoint
    aidevkit serving query --name my-endpoint --inputs '[...]'
"""

import json
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from databricks_tools_core.serving.serving_api import manage_serving_endpoint as _manage_serving_endpoint

app = typer.Typer(help="Manage model serving endpoints")
console = Console()


def _output_result(result, output_format: str = "json"):
    """Output result in the specified format."""
    if output_format == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


@app.command("create-or-update")
def create_or_update(
    name: str = typer.Option(..., "--name", "-n", help="Endpoint name"),
    served_entities: str = typer.Option(..., "--served-entities", "-e", help="JSON array of served entities config"),
    route_optimized: bool = typer.Option(False, "--route-optimized", help="Enable route optimization"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create or update a serving endpoint.

    Idempotent - updates if exists.

    Example:
        aidevkit serving create-or-update --name my-endpoint --served-entities '[{"name": "model", "entity_name": "catalog.schema.model", "entity_version": "1"}]'
    """
    entities = json.loads(served_entities)
    result = _manage_serving_endpoint(
        action="create_or_update",
        name=name,
        served_entities=entities,
        route_optimized=route_optimized,
    )
    _output_result(result, output_format)


@app.command("get")
def get(
    name: str = typer.Option(..., "--name", "-n", help="Endpoint name"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get endpoint details.

    Example:
        aidevkit serving get --name my-endpoint
    """
    result = _manage_serving_endpoint(action="get", name=name)
    _output_result(result, output_format)


@app.command("list")
def list_endpoints():
    """List all serving endpoints.

    Example:
        aidevkit serving list
    """
    result = _manage_serving_endpoint(action="list")

    if "error" in result:
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    endpoints = result.get("endpoints", [])
    if not endpoints:
        rprint("[yellow]No endpoints found[/yellow]")
        return

    table = Table(title="Serving Endpoints")
    table.add_column("Name", style="cyan")
    table.add_column("State", style="yellow")
    table.add_column("Creator", style="green")

    for ep in endpoints:
        table.add_row(
            ep.get("name", ""),
            ep.get("state", {}).get("ready", "UNKNOWN") if isinstance(ep.get("state"), dict) else str(ep.get("state", "")),
            ep.get("creator", ""),
        )

    console.print(table)


@app.command("delete")
def delete(
    name: str = typer.Option(..., "--name", "-n", help="Endpoint name to delete"),
):
    """Delete a serving endpoint.

    Example:
        aidevkit serving delete --name my-endpoint
    """
    result = _manage_serving_endpoint(action="delete", name=name)

    if "error" in result:
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    rprint(f"[green]Endpoint '{name}' deleted successfully[/green]")


@app.command("query")
def query(
    name: str = typer.Option(..., "--name", "-n", help="Endpoint name"),
    inputs: str = typer.Option(..., "--inputs", "-i", help="JSON input data"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Query a serving endpoint.

    Example:
        aidevkit serving query --name my-endpoint --inputs '[{"prompt": "Hello"}]'
    """
    input_data = json.loads(inputs)
    result = _manage_serving_endpoint(action="query", name=name, inputs=input_data)
    _output_result(result, output_format)


if __name__ == "__main__":
    app()
