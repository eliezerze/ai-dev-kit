"""Auth CLI commands - Get current user information.

Commands:
    aidevkit auth whoami
"""

import json

import typer
from rich import print as rprint

from databricks_tools_core.auth_workflows import get_current_user as _get_current_user

app = typer.Typer(help="Authentication and user information")


@app.command("whoami")
def whoami(
    output_format: str = typer.Option("text", "--format", "-f", help="Output format: text or json"),
):
    """Get current authenticated user information.

    Example:
        aidevkit auth whoami
        aidevkit auth whoami --format json
    """
    result = _get_current_user()

    if "error" in result:
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    if output_format == "json":
        print(json.dumps(result, indent=2))
    else:
        rprint(f"[green]Username:[/green] {result.get('user_name', 'N/A')}")
        rprint(f"[green]Display Name:[/green] {result.get('display_name', 'N/A')}")
        if result.get("emails"):
            rprint(f"[green]Email:[/green] {result['emails'][0].get('value', 'N/A')}")


if __name__ == "__main__":
    app()
