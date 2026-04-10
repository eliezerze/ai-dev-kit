"""AI Dev Kit CLI - Main entry point.

Usage:
    aidevkit <command-group> <command> [options]

Examples:
    aidevkit sql execute --query "SELECT 1"
    aidevkit workspace status
    aidevkit jobs list
    aidevkit uc catalog list
"""

import sys
import typer
from rich import print as rprint

from aidevkit_cli import __version__


def _format_error(error: Exception) -> str:
    """Format an exception into a user-friendly error message."""
    error_type = type(error).__name__
    error_msg = str(error)

    # Common Databricks SDK errors with helpful hints
    hints = {
        "NotFound": "Check that the resource exists and you have access to it.",
        "PermissionDenied": "You don't have permission. Check your grants or contact an admin.",
        "InvalidParameterValue": "Check your input parameters.",
        "ResourceAlreadyExists": "A resource with this name already exists.",
        "ResourceDoesNotExist": "The specified resource was not found.",
        "Unauthenticated": "Run 'aidevkit auth login' or check your credentials.",
        "DeadlineExceeded": "Request timed out. Try again later.",
        "BadRequest": "Check your parameters.",
    }

    hint = hints.get(error_type, "")
    return f"{error_type}: {error_msg}" + (f"\n💡 {hint}" if hint else "")

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
from aidevkit_cli.commands import (
    sql,
    workspace,
    auth,
    apps,
    serving,
    pdf,
    volume_files,
    workspace_files,
    jobs,
    compute,
    dashboards,
    pipelines,
    vector_search,
    genie,
    agent_bricks,
    lakebase,
    uc,
)

# Core commands
app.add_typer(sql.app, name="sql", help="Execute SQL queries and manage warehouses")
app.add_typer(workspace.app, name="workspace", help="Manage workspace connections")
app.add_typer(auth.app, name="auth", help="Authentication and user info")

# Resource management
app.add_typer(apps.app, name="apps", help="Manage Databricks Apps")
app.add_typer(serving.app, name="serving", help="Manage model serving endpoints")
app.add_typer(jobs.app, name="jobs", help="Manage Databricks Jobs")
app.add_typer(compute.app, name="compute", help="Execute code and manage clusters")
app.add_typer(dashboards.app, name="dashboards", help="Manage AI/BI dashboards")
app.add_typer(pipelines.app, name="pipelines", help="Manage DLT pipelines")

# AI/ML features
app.add_typer(vector_search.app, name="vector-search", help="Manage vector search")
app.add_typer(genie.app, name="genie", help="Manage Genie Spaces")
app.add_typer(agent_bricks.app, name="agent-bricks", help="Manage KA and MAS agents")

# Data management
app.add_typer(uc.app, name="uc", help="Unity Catalog management")
app.add_typer(lakebase.app, name="lakebase", help="Manage Lakebase databases")
app.add_typer(volume_files.app, name="volume-files", help="Manage Volume files")
app.add_typer(workspace_files.app, name="workspace-files", help="Manage workspace files")

# Utilities
app.add_typer(pdf.app, name="pdf", help="Generate and upload PDFs")


def main():
    """Main entry point with error handling."""
    try:
        app()
    except (typer.Exit, typer.Abort, SystemExit, KeyboardInterrupt):
        raise
    except Exception as e:
        rprint(f"[red]Error:[/red] {_format_error(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
