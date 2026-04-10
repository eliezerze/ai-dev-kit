"""PDF CLI commands - Generate and upload PDFs.

Commands:
    aidevkit pdf generate --html "<html>...</html>" --filename report.pdf --catalog cat --schema sch
"""

import json
from typing import Optional

import typer
from rich import print as rprint

from databricks_tools_core.pdf.pdf_api import generate_and_upload_pdf as _generate_and_upload_pdf

app = typer.Typer(help="Generate and upload PDFs to Unity Catalog volumes")


@app.command("generate")
def generate(
    html: str = typer.Option(..., "--html", "-h", help="HTML content to convert to PDF"),
    filename: str = typer.Option(..., "--filename", "-f", help="Output filename (e.g., report.pdf)"),
    catalog: str = typer.Option(..., "--catalog", "-c", help="Target catalog"),
    schema: str = typer.Option(..., "--schema", "-s", help="Target schema"),
    volume: str = typer.Option("raw_data", "--volume", "-v", help="Target volume name"),
    folder: Optional[str] = typer.Option(None, "--folder", help="Subfolder within volume"),
    output_format: str = typer.Option("json", "--format", help="Output format"),
):
    """Convert HTML to PDF and upload to Unity Catalog volume.

    Example:
        aidevkit pdf generate --html "<html><body><h1>Report</h1></body></html>" --filename report.pdf --catalog my_catalog --schema my_schema
    """
    result = _generate_and_upload_pdf(
        html_content=html,
        filename=filename,
        catalog=catalog,
        schema=schema,
        volume=volume,
        folder=folder,
    )

    if result.get("error"):
        rprint(f"[red]Error: {result['error']}[/red]")
        raise typer.Exit(1)

    if output_format == "json":
        print(json.dumps(result, indent=2))
    else:
        rprint(f"[green]PDF generated successfully[/green]")
        rprint(f"[dim]Volume path: {result.get('volume_path')}[/dim]")


if __name__ == "__main__":
    app()
