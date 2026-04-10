"""Workspace CLI commands - Manage Databricks workspace connections.

Commands:
    aidevkit workspace status
    aidevkit workspace list
    aidevkit workspace switch --profile prod
    aidevkit workspace login --host https://adb-xxx.azuredatabricks.net
"""

from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from databricks_tools_core.workspace_workflows import manage_workspace as _manage_workspace

app = typer.Typer(help="Manage Databricks workspace connections")
console = Console()


@app.command("status")
def status():
    """Show current workspace connection status.

    Example:
        aidevkit workspace status
    """
    result = _manage_workspace(action="status")

    if "error" in result:
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    rprint(f"[green]Host:[/green] {result.get('host', 'N/A')}")
    rprint(f"[green]Profile:[/green] {result.get('profile', 'N/A')}")
    rprint(f"[green]Username:[/green] {result.get('username', 'N/A')}")


@app.command("list")
def list_profiles():
    """List all configured workspace profiles from ~/.databrickscfg.

    Example:
        aidevkit workspace list
    """
    result = _manage_workspace(action="list")

    profiles = result.get("profiles", [])
    if not profiles:
        rprint("[yellow]No profiles found in ~/.databrickscfg[/yellow]")
        if "message" in result:
            rprint(f"[dim]{result['message']}[/dim]")
        return

    table = Table(title="Workspace Profiles")
    table.add_column("Profile", style="cyan")
    table.add_column("Host", style="green")
    table.add_column("Active", style="yellow")

    for p in profiles:
        active = "✓" if p.get("active") else ""
        table.add_row(p.get("profile", ""), p.get("host", ""), active)

    console.print(table)


@app.command("switch")
def switch(
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Profile name from ~/.databrickscfg"),
    host: Optional[str] = typer.Option(None, "--host", "-h", help="Workspace URL"),
):
    """Switch to a different workspace.

    Provide either --profile (from ~/.databrickscfg) or --host (workspace URL).

    Example:
        aidevkit workspace switch --profile prod
        aidevkit workspace switch --host https://adb-xxx.azuredatabricks.net
    """
    if not profile and not host:
        rprint("[red]Error: Provide either --profile or --host[/red]")
        raise typer.Exit(1)

    result = _manage_workspace(action="switch", profile=profile, host=host)

    if "error" in result:
        rprint(f"[red]Error: {result['error']}[/red]")
        if result.get("token_expired"):
            rprint(f"[yellow]Token expired. Run: aidevkit workspace login --host {result.get('host')}[/yellow]")
        raise typer.Exit(1)

    rprint(f"[green]{result.get('message', 'Switched successfully')}[/green]")
    rprint(f"[dim]Host: {result.get('host')}[/dim]")
    rprint(f"[dim]Username: {result.get('username')}[/dim]")


@app.command("login")
def login(
    host: str = typer.Option(..., "--host", "-h", help="Workspace URL to authenticate"),
):
    """Authenticate to a workspace using OAuth (opens browser).

    Example:
        aidevkit workspace login --host https://adb-xxx.azuredatabricks.net
    """
    result = _manage_workspace(action="login", host=host)

    if "error" in result:
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    rprint(f"[green]{result.get('message', 'Login successful')}[/green]")
    rprint(f"[dim]Host: {result.get('host')}[/dim]")
    rprint(f"[dim]Username: {result.get('username')}[/dim]")


if __name__ == "__main__":
    app()
