"""Workspace Files CLI commands - Manage files in Databricks workspace.

Commands:
    aidevkit workspace-files upload --local-path ./code --workspace-path /Workspace/Users/user@example.com/project
    aidevkit workspace-files delete --path /Workspace/Users/user@example.com/project/old_file.py
"""

import json

import typer
from rich import print as rprint

from databricks_tools_core.file.file_api import manage_workspace_files as _manage_workspace_files

app = typer.Typer(help="Manage files in Databricks workspace")


def _output_result(result, output_format: str = "json"):
    """Output result in the specified format."""
    if output_format == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


@app.command("upload")
def upload(
    local_path: str = typer.Option(..., "--local-path", "-l", help="Local file/folder/glob to upload"),
    workspace_path: str = typer.Option(..., "--workspace-path", "-w", help="Destination workspace path"),
    max_workers: int = typer.Option(10, "--workers", help="Parallel upload threads"),
    overwrite: bool = typer.Option(True, "--overwrite/--no-overwrite", help="Overwrite existing files"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Upload files/folders to workspace.

    Supports files, folders, globs, and tilde expansion.

    Example:
        aidevkit workspace-files upload --local-path ./src --workspace-path /Workspace/Users/user@example.com/project
    """
    result = _manage_workspace_files(
        action="upload",
        workspace_path=workspace_path,
        local_path=local_path,
        max_workers=max_workers,
        overwrite=overwrite,
    )
    _output_result(result, output_format)


@app.command("delete")
def delete(
    path: str = typer.Option(..., "--path", "-p", help="Workspace path to delete"),
    recursive: bool = typer.Option(False, "--recursive", "-r", help="Delete directories recursively"),
):
    """Delete file or folder from workspace.

    Has safety checks for protected paths.

    Example:
        aidevkit workspace-files delete --path /Workspace/Users/user@example.com/old_file.py
        aidevkit workspace-files delete --path /Workspace/Users/user@example.com/old_folder --recursive
    """
    result = _manage_workspace_files(
        action="delete",
        workspace_path=path,
        recursive=recursive,
    )

    if "error" in result:
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    rprint(f"[green]Deleted: {path}[/green]")


if __name__ == "__main__":
    app()
