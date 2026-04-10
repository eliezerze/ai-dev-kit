"""Volume Files CLI commands - Manage files in Unity Catalog Volumes.

Commands:
    aidevkit volume-files list --path /Volumes/catalog/schema/volume/
    aidevkit volume-files upload --local-path ./data --volume-path /Volumes/cat/sch/vol/data
    aidevkit volume-files download --volume-path /Volumes/cat/sch/vol/file.csv --local-path ./file.csv
    aidevkit volume-files delete --path /Volumes/catalog/schema/volume/file.csv
    aidevkit volume-files mkdir --path /Volumes/catalog/schema/volume/new_folder
    aidevkit volume-files info --path /Volumes/catalog/schema/volume/file.csv
"""

import json
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from databricks_tools_core.unity_catalog.unity_catalog_api import manage_volume_files as _manage_volume_files

app = typer.Typer(help="Manage files in Unity Catalog Volumes")
console = Console()


def _output_result(result, output_format: str = "json"):
    """Output result in the specified format."""
    if output_format == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


@app.command("list")
def list_files(
    path: str = typer.Option(..., "--path", "-p", help="Volume path to list"),
    max_results: int = typer.Option(500, "--max", "-m", help="Maximum results (default 500, max 1000)"),
):
    """List files in a volume path.

    Example:
        aidevkit volume-files list --path /Volumes/catalog/schema/volume/
    """
    result = _manage_volume_files(action="list", volume_path=path, max_results=max_results)

    if "error" in result:
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    files = result.get("files", [])
    if not files:
        rprint("[yellow]No files found[/yellow]")
        return

    table = Table(title=f"Files in {path}")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="yellow")
    table.add_column("Size", style="green")

    for f in files:
        file_type = "DIR" if f.get("is_directory") else "FILE"
        size = str(f.get("file_size", "")) if not f.get("is_directory") else ""
        table.add_row(f.get("name", ""), file_type, size)

    console.print(table)
    if result.get("truncated"):
        rprint(f"[yellow]Results truncated. Use --max to increase limit.[/yellow]")


@app.command("upload")
def upload(
    local_path: str = typer.Option(..., "--local-path", "-l", help="Local file/folder/glob to upload"),
    volume_path: str = typer.Option(..., "--volume-path", "-v", help="Destination volume path"),
    max_workers: int = typer.Option(4, "--workers", "-w", help="Parallel upload threads"),
    overwrite: bool = typer.Option(True, "--overwrite/--no-overwrite", help="Overwrite existing files"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Upload local file/folder to volume.

    Supports files, folders, globs, and tilde expansion.

    Example:
        aidevkit volume-files upload --local-path ./data --volume-path /Volumes/cat/sch/vol/data
        aidevkit volume-files upload -l "*.csv" -v /Volumes/cat/sch/vol/csvs
    """
    result = _manage_volume_files(
        action="upload",
        volume_path=volume_path,
        local_path=local_path,
        max_workers=max_workers,
        overwrite=overwrite,
    )
    _output_result(result, output_format)


@app.command("download")
def download(
    volume_path: str = typer.Option(..., "--volume-path", "-v", help="Volume file path to download"),
    local_path: str = typer.Option(..., "--local-path", "-l", help="Local destination path"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Download file from volume to local path.

    Example:
        aidevkit volume-files download --volume-path /Volumes/cat/sch/vol/file.csv --local-path ./file.csv
    """
    result = _manage_volume_files(
        action="download",
        volume_path=volume_path,
        local_destination=local_path,
    )
    _output_result(result, output_format)


@app.command("delete")
def delete(
    path: str = typer.Option(..., "--path", "-p", help="Volume path to delete"),
    recursive: bool = typer.Option(False, "--recursive", "-r", help="Delete directories recursively"),
):
    """Delete file or directory from volume.

    Example:
        aidevkit volume-files delete --path /Volumes/cat/sch/vol/file.csv
        aidevkit volume-files delete --path /Volumes/cat/sch/vol/folder --recursive
    """
    result = _manage_volume_files(action="delete", volume_path=path, recursive=recursive)

    if "error" in result:
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    rprint(f"[green]Deleted successfully[/green]")
    rprint(f"[dim]Files: {result.get('files_deleted', 0)}, Directories: {result.get('directories_deleted', 0)}[/dim]")


@app.command("mkdir")
def mkdir(
    path: str = typer.Option(..., "--path", "-p", help="Volume directory path to create"),
):
    """Create directory in volume (like mkdir -p).

    Idempotent - succeeds if directory already exists.

    Example:
        aidevkit volume-files mkdir --path /Volumes/cat/sch/vol/new_folder
    """
    result = _manage_volume_files(action="mkdir", volume_path=path)

    if "error" in result:
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    rprint(f"[green]Directory created: {path}[/green]")


@app.command("info")
def info(
    path: str = typer.Option(..., "--path", "-p", help="Volume path to get info for"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get file/directory metadata.

    Example:
        aidevkit volume-files info --path /Volumes/cat/sch/vol/file.csv
    """
    result = _manage_volume_files(action="get_info", volume_path=path)
    _output_result(result, output_format)


if __name__ == "__main__":
    app()
