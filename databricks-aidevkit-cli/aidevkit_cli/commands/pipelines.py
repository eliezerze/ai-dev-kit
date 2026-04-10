"""Pipelines CLI commands - Manage Delta Live Tables (DLT) pipelines.

Commands:
    aidevkit pipelines create --name my-pipeline --libraries '[...]'
    aidevkit pipelines create-or-update --name my-pipeline --libraries '[...]'
    aidevkit pipelines get --pipeline-id abc123
    aidevkit pipelines find-by-name --name my-pipeline
    aidevkit pipelines update --pipeline-id abc123 --libraries '[...]'
    aidevkit pipelines delete --pipeline-id abc123
    aidevkit pipelines run-start --pipeline-id abc123
    aidevkit pipelines run-get --pipeline-id abc123
    aidevkit pipelines run-stop --pipeline-id abc123
    aidevkit pipelines run-events --pipeline-id abc123
"""

import json
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from databricks_tools_core.spark_declarative_pipelines.pipelines import (
    create_pipeline as _create_pipeline,
    get_pipeline as _get_pipeline,
    update_pipeline as _update_pipeline,
    delete_pipeline as _delete_pipeline,
    start_update as _start_pipeline_update,
    get_update as _get_pipeline_update,
    stop_pipeline as _stop_pipeline,
    get_pipeline_events as _get_pipeline_events,
    create_or_update_pipeline as _create_or_update_pipeline,
    find_pipeline_by_name as _find_pipeline_by_name,
)
from databricks_tools_core.auth import get_workspace_client

app = typer.Typer(help="Manage Delta Live Tables pipelines")
console = Console()


def _output_result(result, output_format: str = "json"):
    """Output result in the specified format."""
    if output_format == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


@app.command("create")
def create(
    name: str = typer.Option(..., "--name", "-n", help="Pipeline name"),
    libraries: str = typer.Option(..., "--libraries", "-l", help="JSON array of library paths"),
    target: Optional[str] = typer.Option(None, "--target", "-t", help="Target schema"),
    catalog: Optional[str] = typer.Option(None, "--catalog", "-c", help="Target catalog"),
    channel: str = typer.Option("CURRENT", "--channel", help="Release channel: CURRENT or PREVIEW"),
    continuous: bool = typer.Option(False, "--continuous", help="Run continuously"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create a new DLT pipeline.

    Example:
        aidevkit pipelines create --name my-pipeline --libraries '[{"notebook": {"path": "/path/to/notebook"}}]' --target my_schema --catalog my_catalog
    """
    libs = json.loads(libraries)
    result = _create_pipeline(
        name=name,
        libraries=libs,
        target=target,
        catalog=catalog,
        channel=channel,
        continuous=continuous,
    )
    _output_result(result, output_format)


@app.command("create-or-update")
def create_or_update(
    name: str = typer.Option(..., "--name", "-n", help="Pipeline name"),
    libraries: str = typer.Option(..., "--libraries", "-l", help="JSON array of library paths"),
    target: Optional[str] = typer.Option(None, "--target", "-t", help="Target schema"),
    catalog: Optional[str] = typer.Option(None, "--catalog", "-c", help="Target catalog"),
    channel: str = typer.Option("CURRENT", "--channel", help="Release channel: CURRENT or PREVIEW"),
    continuous: bool = typer.Option(False, "--continuous", help="Run continuously"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create or update a DLT pipeline (idempotent).

    Example:
        aidevkit pipelines create-or-update --name my-pipeline --libraries '[{"notebook": {"path": "/path/to/notebook"}}]'
    """
    libs = json.loads(libraries)
    result = _create_or_update_pipeline(
        name=name,
        libraries=libs,
        target=target,
        catalog=catalog,
        channel=channel,
        continuous=continuous,
    )
    _output_result(result, output_format)


@app.command("get")
def get(
    pipeline_id: str = typer.Option(..., "--pipeline-id", "-p", help="Pipeline ID"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get pipeline details.

    Example:
        aidevkit pipelines get --pipeline-id abc123
    """
    result = _get_pipeline(pipeline_id=pipeline_id)
    _output_result(result, output_format)


@app.command("find-by-name")
def find_by_name(
    name: str = typer.Option(..., "--name", "-n", help="Pipeline name"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Find pipeline by name.

    Example:
        aidevkit pipelines find-by-name --name my-pipeline
    """
    result = _find_pipeline_by_name(name=name)
    _output_result(result, output_format)


@app.command("update")
def update(
    pipeline_id: str = typer.Option(..., "--pipeline-id", "-p", help="Pipeline ID"),
    name: Optional[str] = typer.Option(None, "--name", "-n", help="New name"),
    libraries: Optional[str] = typer.Option(None, "--libraries", "-l", help="New libraries JSON"),
    target: Optional[str] = typer.Option(None, "--target", "-t", help="New target schema"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Update an existing pipeline.

    Example:
        aidevkit pipelines update --pipeline-id abc123 --name new-name
    """
    libs = json.loads(libraries) if libraries else None
    result = _update_pipeline(
        pipeline_id=pipeline_id,
        name=name,
        libraries=libs,
        target=target,
    )
    _output_result(result, output_format)


@app.command("delete")
def delete(
    pipeline_id: str = typer.Option(..., "--pipeline-id", "-p", help="Pipeline ID to delete"),
):
    """Delete a pipeline.

    Example:
        aidevkit pipelines delete --pipeline-id abc123
    """
    _delete_pipeline(pipeline_id=pipeline_id)
    rprint(f"[green]Pipeline {pipeline_id} deleted[/green]")


@app.command("list")
def list_pipelines_cmd(
    filter_str: Optional[str] = typer.Option(None, "--filter", help="Filter string"),
    max_results: int = typer.Option(100, "--max", "-m", help="Max results"),
):
    """List all pipelines.

    Example:
        aidevkit pipelines list
        aidevkit pipelines list --filter "name LIKE 'etl%'"
    """
    client = get_workspace_client()
    result = client.pipelines.list_pipelines(filter=filter_str, max_results=max_results)

    pipelines = list(result) if result else []
    if not pipelines:
        rprint("[yellow]No pipelines found[/yellow]")
        return

    table = Table(title="DLT Pipelines")
    table.add_column("Pipeline ID", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("State", style="yellow")

    for p in pipelines[:50]:
        # Handle both dict and SDK object formats
        if hasattr(p, 'pipeline_id'):
            table.add_row(p.pipeline_id or "", p.name or "", p.state.value if p.state else "")
        else:
            table.add_row(p.get("pipeline_id", ""), p.get("name", ""), p.get("state", ""))

    console.print(table)


@app.command("run-start")
def run_start(
    pipeline_id: str = typer.Option(..., "--pipeline-id", "-p", help="Pipeline ID"),
    full_refresh: bool = typer.Option(False, "--full-refresh", help="Full refresh all tables"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Start a pipeline update/run.

    Example:
        aidevkit pipelines run-start --pipeline-id abc123
        aidevkit pipelines run-start --pipeline-id abc123 --full-refresh
    """
    result = _start_pipeline_update(pipeline_id=pipeline_id, full_refresh=full_refresh)
    _output_result(result, output_format)


@app.command("run-get")
def run_get(
    pipeline_id: str = typer.Option(..., "--pipeline-id", "-p", help="Pipeline ID"),
    update_id: Optional[str] = typer.Option(None, "--update-id", "-u", help="Specific update ID"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get pipeline update status.

    Example:
        aidevkit pipelines run-get --pipeline-id abc123
    """
    result = _get_pipeline_update(pipeline_id=pipeline_id, update_id=update_id)
    _output_result(result, output_format)


@app.command("run-stop")
def run_stop(
    pipeline_id: str = typer.Option(..., "--pipeline-id", "-p", help="Pipeline ID to stop"),
):
    """Stop a running pipeline.

    Example:
        aidevkit pipelines run-stop --pipeline-id abc123
    """
    _stop_pipeline(pipeline_id=pipeline_id)
    rprint(f"[green]Pipeline {pipeline_id} stopped[/green]")


@app.command("run-events")
def run_events(
    pipeline_id: str = typer.Option(..., "--pipeline-id", "-p", help="Pipeline ID"),
    max_results: int = typer.Option(100, "--max", "-m", help="Max events to return"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get pipeline events/logs.

    Example:
        aidevkit pipelines run-events --pipeline-id abc123
    """
    result = _get_pipeline_events(pipeline_id=pipeline_id, max_results=max_results)
    _output_result(result, output_format)


if __name__ == "__main__":
    app()
