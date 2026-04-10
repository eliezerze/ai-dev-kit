"""Lakebase CLI commands - Manage Lakebase (PostgreSQL-compatible) databases.

Commands:
    aidevkit lakebase database create-or-update --name my-db --catalog cat --schema sch
    aidevkit lakebase database get --name my-db
    aidevkit lakebase database list
    aidevkit lakebase database delete --name my-db
    aidevkit lakebase credential --name my-db
"""

import json
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from databricks_tools_core.lakebase import (
    create_lakebase_instance as _create_instance,
    get_lakebase_instance as _get_instance,
    list_lakebase_instances as _list_instances,
    delete_lakebase_instance as _delete_instance,
    create_synced_table as _create_sync,
    delete_synced_table as _delete_sync,
    generate_lakebase_credential as _generate_credential,
)

from databricks_tools_core.lakebase_autoscale import (
    create_project as _create_autoscale,
    get_project as _get_autoscale,
    list_projects as _list_autoscale,
    delete_project as _delete_autoscale,
    create_branch as _create_branch_autoscale,
    delete_branch as _delete_branch_autoscale,
    generate_credential as _generate_credential_autoscale,
)

app = typer.Typer(help="Manage Lakebase (PostgreSQL-compatible) databases")
database_app = typer.Typer(help="Manage Lakebase databases")
app.add_typer(database_app, name="database")
console = Console()


def _output_result(result, output_format: str = "json"):
    """Output result in the specified format."""
    if hasattr(result, 'as_dict'):
        result = result.as_dict()
    if output_format == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


# Database commands
@database_app.command("create-or-update")
def database_create_or_update(
    name: str = typer.Option(..., "--name", "-n", help="Database name"),
    catalog: str = typer.Option(..., "--catalog", "-c", help="Catalog name"),
    schema: str = typer.Option(..., "--schema", "-s", help="Schema name"),
    db_type: str = typer.Option("provisioned", "--type", "-t", help="Type: autoscale or provisioned"),
    capacity: Optional[int] = typer.Option(None, "--capacity", help="Capacity (provisioned only)"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create or update a Lakebase database.

    Example:
        aidevkit lakebase database create-or-update --name my-db --catalog my_catalog --schema my_schema
        aidevkit lakebase database create-or-update --name my-db --catalog cat --schema sch --type autoscale
    """
    if db_type == "autoscale":
        result = _create_autoscale(
            name=name,
            catalog_name=catalog,
            schema_name=schema,
        )
    else:
        result = _create_instance(
            name=name,
            catalog_name=catalog,
            schema_name=schema,
            capacity=capacity or 1,
        )
    _output_result(result, output_format)


@database_app.command("get")
def database_get(
    name: str = typer.Option(..., "--name", "-n", help="Database name"),
    db_type: str = typer.Option("provisioned", "--type", "-t", help="Type: autoscale or provisioned"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get database details.

    Example:
        aidevkit lakebase database get --name my-db
    """
    if db_type == "autoscale":
        result = _get_autoscale(name=name)
    else:
        result = _get_instance(name=name)
    _output_result(result, output_format)


@database_app.command("list")
def database_list(
    db_type: str = typer.Option("provisioned", "--type", "-t", help="Type: autoscale or provisioned"),
):
    """List all Lakebase databases.

    Example:
        aidevkit lakebase database list
    """
    if db_type == "autoscale":
        result = _list_autoscale()
    else:
        result = _list_instances()

    instances = list(result) if result else []
    if not instances:
        rprint("[yellow]No databases found[/yellow]")
        return

    table = Table(title=f"Lakebase Databases ({db_type})")
    table.add_column("Name", style="cyan")
    table.add_column("State", style="yellow")

    for db in instances:
        if hasattr(db, 'name'):
            table.add_row(db.name or "", db.state.value if hasattr(db, 'state') and db.state else "")
        else:
            table.add_row(db.get("name", ""), db.get("state", ""))

    console.print(table)


@database_app.command("delete")
def database_delete(
    name: str = typer.Option(..., "--name", "-n", help="Database name to delete"),
    db_type: str = typer.Option("provisioned", "--type", "-t", help="Type: autoscale or provisioned"),
):
    """Delete a Lakebase database.

    Example:
        aidevkit lakebase database delete --name my-db
    """
    if db_type == "autoscale":
        _delete_autoscale(name=name)
    else:
        _delete_instance(name=name)
    rprint(f"[green]Database {name} deleted[/green]")


# Credential command
@app.command("credential")
def credential(
    name: str = typer.Option(..., "--name", "-n", help="Database name"),
    db_type: str = typer.Option("provisioned", "--type", "-t", help="Type: autoscale or provisioned"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Generate PostgreSQL connection credentials.

    Example:
        aidevkit lakebase credential --name my-db
    """
    if db_type == "autoscale":
        result = _generate_credential_autoscale(name=name)
    else:
        result = _generate_credential(name=name)
    _output_result(result, output_format)


if __name__ == "__main__":
    app()
