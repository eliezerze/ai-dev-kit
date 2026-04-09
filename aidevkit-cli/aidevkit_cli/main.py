"""AI Dev Kit CLI - Main entry point.

Usage:
    aidevkit <command-group> <command> [options]

Examples:
    aidevkit sql execute --query "SELECT 1"
    aidevkit jobs create-or-update --name "my-job" --tasks '[...]'
    aidevkit lakebase get --name "my-db" --type autoscale
"""

import typer

from aidevkit_cli import __version__

# Create main app
app = typer.Typer(
    name="aidevkit",
    help="CLI interface for Databricks AI Dev Kit",
    no_args_is_help=True,
)


def version_callback(value: bool):
    if value:
        typer.echo(f"aidevkit version {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        None,
        "--version",
        "-v",
        help="Show version and exit",
        callback=version_callback,
        is_eager=True,
    ),
):
    """AI Dev Kit CLI - Command line interface for Databricks tools."""
    pass


# Import and register command groups
# These will be added as we implement each domain
from aidevkit_cli.commands import sql

app.add_typer(sql.app, name="sql", help="Execute SQL queries and manage warehouses")


if __name__ == "__main__":
    app()
