"""Genie CLI commands - Manage Genie Spaces and ask questions.

Commands:
    aidevkit genie create-or-update --name "My Space" --tables '[...]'
    aidevkit genie get --space-id abc123
    aidevkit genie list
    aidevkit genie delete --space-id abc123
    aidevkit genie export --space-id abc123
    aidevkit genie import --warehouse-id <id> --serialized-space '{...}'
    aidevkit genie ask --space-id abc123 --question "What is the total revenue?"
"""

import json
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from databricks_tools_core.agent_bricks import manage_genie, ask_genie

app = typer.Typer(help="Manage Genie Spaces and ask questions")
console = Console()


def _output_result(result, output_format: str = "json"):
    """Output result in the specified format."""
    if output_format == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


@app.command("create-or-update")
def create_or_update(
    name: str = typer.Option(..., "--name", "-n", help="Space name"),
    tables: str = typer.Option(..., "--tables", "-t", help="JSON array of table identifiers"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="Space description"),
    sample_questions: Optional[str] = typer.Option(None, "--sample-questions", help="JSON array of sample questions"),
    warehouse_id: Optional[str] = typer.Option(None, "--warehouse-id", "-w", help="SQL warehouse ID (auto-detected if omitted)"),
    space_id: Optional[str] = typer.Option(None, "--space-id", help="Space ID (for update)"),
    serialized_space: Optional[str] = typer.Option(None, "--serialized-space", help="Full config from export"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create or update a Genie Space.

    Example:
        aidevkit genie create-or-update --name "Sales Analytics" --tables '["catalog.schema.sales", "catalog.schema.customers"]'
    """
    table_list = json.loads(tables)
    questions_list = json.loads(sample_questions) if sample_questions else None

    result = manage_genie(
        action="create_or_update",
        display_name=name,
        table_identifiers=table_list,
        description=description,
        sample_questions=questions_list,
        warehouse_id=warehouse_id,
        space_id=space_id,
        serialized_space=serialized_space,
    )
    _output_result(result, output_format)


@app.command("get")
def get(
    space_id: str = typer.Option(..., "--space-id", "-s", help="Space ID"),
    include_serialized_space: bool = typer.Option(False, "--include-serialized", help="Include full config"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get Genie Space details.

    Example:
        aidevkit genie get --space-id abc123
    """
    result = manage_genie(
        action="get",
        space_id=space_id,
        include_serialized_space=include_serialized_space,
    )
    _output_result(result, output_format)


@app.command("list")
def list_spaces(
    output_format: str = typer.Option("table", "--format", "-f", help="Output format (table or json)"),
):
    """List all Genie Spaces.

    Example:
        aidevkit genie list
    """
    result = manage_genie(action="list")

    if "error" in result:
        rprint(f"[red]Error: {result['error']}[/red]")
        return

    spaces = result.get("spaces", [])
    if not spaces:
        rprint("[yellow]No Genie Spaces found[/yellow]")
        return

    if output_format == "json":
        _output_result(result, "json")
        return

    table = Table(title="Genie Spaces")
    table.add_column("Space ID", style="cyan")
    table.add_column("Title", style="green")
    table.add_column("Description", style="yellow")

    for s in spaces:
        table.add_row(
            s.get("space_id", ""),
            s.get("title", ""),
            s.get("description", "")[:50] + "..." if len(s.get("description", "")) > 50 else s.get("description", ""),
        )

    console.print(table)


@app.command("delete")
def delete(
    space_id: str = typer.Option(..., "--space-id", "-s", help="Space ID to delete"),
):
    """Delete a Genie Space.

    Example:
        aidevkit genie delete --space-id abc123
    """
    result = manage_genie(action="delete", space_id=space_id)

    if result.get("success"):
        rprint(f"[green]Genie Space {space_id} deleted[/green]")
    else:
        rprint(f"[red]Failed to delete: {result.get('error', 'Unknown error')}[/red]")


@app.command("export")
def export(
    space_id: str = typer.Option(..., "--space-id", "-s", help="Space ID to export"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Export a Genie Space configuration for migration/backup.

    Example:
        aidevkit genie export --space-id abc123 > space_config.json
    """
    result = manage_genie(action="export", space_id=space_id)
    _output_result(result, output_format)


@app.command("import")
def import_space(
    warehouse_id: str = typer.Option(..., "--warehouse-id", "-w", help="SQL warehouse ID"),
    serialized_space: str = typer.Option(..., "--serialized-space", "-c", help="Serialized space config from export"),
    title: Optional[str] = typer.Option(None, "--title", "-t", help="Override title"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="Override description"),
    parent_path: Optional[str] = typer.Option(None, "--parent-path", help="Parent path for the space"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Import a Genie Space from serialized configuration.

    Example:
        aidevkit genie import --warehouse-id abc123 --serialized-space '$(cat space_config.json | jq -r .serialized_space)'
    """
    result = manage_genie(
        action="import",
        warehouse_id=warehouse_id,
        serialized_space=serialized_space,
        title=title,
        description=description,
        parent_path=parent_path,
    )
    _output_result(result, output_format)


@app.command("ask")
def ask(
    space_id: str = typer.Option(..., "--space-id", "-s", help="Space ID to ask"),
    question: str = typer.Option(..., "--question", "-q", help="Question to ask"),
    conversation_id: Optional[str] = typer.Option(None, "--conversation-id", help="Conversation ID for follow-ups"),
    timeout: int = typer.Option(120, "--timeout", help="Timeout in seconds"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Ask a question to a Genie Space.

    Example:
        aidevkit genie ask --space-id abc123 --question "What is total revenue by region?"
    """
    result = ask_genie(
        space_id=space_id,
        question=question,
        conversation_id=conversation_id,
        timeout_seconds=timeout,
    )
    _output_result(result, output_format)


if __name__ == "__main__":
    app()
