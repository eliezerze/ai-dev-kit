"""Apps CLI commands - Manage Databricks Apps.

Commands:
    aidevkit apps create-or-update --name my-app --source-code-path /Volumes/...
    aidevkit apps get --name my-app
    aidevkit apps list
    aidevkit apps delete --name my-app
"""

import json
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from databricks_tools_core.apps.apps_api import manage_app as _manage_app

app = typer.Typer(help="Manage Databricks Apps")
console = Console()


def _output_result(result, output_format: str = "json"):
    """Output result in the specified format."""
    if output_format == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


@app.command("create-or-update")
def create_or_update(
    name: str = typer.Option(..., "--name", "-n", help="App name"),
    source_code_path: Optional[str] = typer.Option(None, "--source-code-path", "-s", help="Volume or workspace path to deploy from"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="App description"),
    mode: Optional[str] = typer.Option(None, "--mode", "-m", help="Deployment mode"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create or update a Databricks App.

    Idempotent - returns existing app if name matches.
    Deploys if source_code_path is provided.

    Example:
        aidevkit apps create-or-update --name my-app --source-code-path /Volumes/cat/sch/vol/app
    """
    result = _manage_app(
        action="create_or_update",
        name=name,
        source_code_path=source_code_path,
        description=description,
        mode=mode,
    )
    _output_result(result, output_format)


@app.command("get")
def get(
    name: str = typer.Option(..., "--name", "-n", help="App name"),
    include_logs: bool = typer.Option(False, "--logs", "-l", help="Include deployment logs"),
    deployment_id: Optional[str] = typer.Option(None, "--deployment-id", help="Specific deployment ID"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get app details.

    Example:
        aidevkit apps get --name my-app
        aidevkit apps get --name my-app --logs
    """
    result = _manage_app(
        action="get",
        name=name,
        include_logs=include_logs,
        deployment_id=deployment_id,
    )
    _output_result(result, output_format)


@app.command("list")
def list_apps(
    name_contains: Optional[str] = typer.Option(None, "--filter", help="Filter by name contains"),
):
    """List all apps.

    Example:
        aidevkit apps list
        aidevkit apps list --filter demo
    """
    result = _manage_app(action="list", name_contains=name_contains)

    if "error" in result:
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    apps = result.get("apps", [])
    if not apps:
        rprint("[yellow]No apps found[/yellow]")
        return

    table = Table(title="Databricks Apps")
    table.add_column("Name", style="cyan")
    table.add_column("URL", style="green")
    table.add_column("Status", style="yellow")

    for a in apps:
        table.add_row(
            a.get("name", ""),
            a.get("url", ""),
            a.get("status", ""),
        )

    console.print(table)


@app.command("delete")
def delete(
    name: str = typer.Option(..., "--name", "-n", help="App name to delete"),
):
    """Delete an app.

    Example:
        aidevkit apps delete --name my-app
    """
    result = _manage_app(action="delete", name=name)

    if "error" in result:
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    rprint(f"[green]App '{name}' deleted successfully[/green]")


if __name__ == "__main__":
    app()
