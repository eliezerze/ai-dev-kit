"""Vector Search CLI commands - Manage vector search endpoints and indexes.

Commands:
    aidevkit vector-search endpoint create-or-update --name my-endpoint
    aidevkit vector-search endpoint get --name my-endpoint
    aidevkit vector-search endpoint list
    aidevkit vector-search endpoint delete --name my-endpoint
    aidevkit vector-search index create-or-update --name catalog.schema.index --endpoint my-endpoint
    aidevkit vector-search index get --name catalog.schema.index
    aidevkit vector-search index list --endpoint my-endpoint
    aidevkit vector-search index delete --name catalog.schema.index
    aidevkit vector-search query --index catalog.schema.index --query-text "search term"
    aidevkit vector-search data upsert --index catalog.schema.index --data '[...]'
"""

import json
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from databricks_tools_core.vector_search import (
    create_vs_endpoint as _create_vs_endpoint,
    get_vs_endpoint as _get_vs_endpoint,
    list_vs_endpoints as _list_vs_endpoints,
    delete_vs_endpoint as _delete_vs_endpoint,
    create_vs_index as _create_vs_index,
    get_vs_index as _get_vs_index,
    list_vs_indexes as _list_vs_indexes,
    delete_vs_index as _delete_vs_index,
    query_vs_index as _query_vs_index,
    upsert_vs_data as _upsert_vs_data,
    delete_vs_data as _delete_vs_data,
    scan_vs_index as _scan_vs_index,
    sync_vs_index as _sync_vs_index,
)

app = typer.Typer(help="Manage vector search endpoints and indexes")
endpoint_app = typer.Typer(help="Manage vector search endpoints")
index_app = typer.Typer(help="Manage vector search indexes")
data_app = typer.Typer(help="Manage vector search data")
app.add_typer(endpoint_app, name="endpoint")
app.add_typer(index_app, name="index")
app.add_typer(data_app, name="data")
console = Console()


def _output_result(result, output_format: str = "json"):
    """Output result in the specified format."""
    if output_format == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


# Endpoint commands
@endpoint_app.command("create-or-update")
def endpoint_create_or_update(
    name: str = typer.Option(..., "--name", "-n", help="Endpoint name"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create or update a vector search endpoint.

    Example:
        aidevkit vector-search endpoint create-or-update --name my-vs-endpoint
    """
    result = _create_vs_endpoint(name=name)
    _output_result(result, output_format)


@endpoint_app.command("get")
def endpoint_get(
    name: str = typer.Option(..., "--name", "-n", help="Endpoint name"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get endpoint details.

    Example:
        aidevkit vector-search endpoint get --name my-vs-endpoint
    """
    result = _get_vs_endpoint(name=name)
    _output_result(result, output_format)


@endpoint_app.command("list")
def endpoint_list():
    """List all vector search endpoints.

    Example:
        aidevkit vector-search endpoint list
    """
    result = _list_vs_endpoints()

    endpoints = result.get("endpoints", []) if isinstance(result, dict) else result
    if not endpoints:
        rprint("[yellow]No endpoints found[/yellow]")
        return

    table = Table(title="Vector Search Endpoints")
    table.add_column("Name", style="cyan")
    table.add_column("State", style="yellow")
    table.add_column("Creator", style="green")

    for ep in endpoints:
        table.add_row(
            ep.get("name", ""),
            ep.get("endpoint_status", {}).get("state", "") if isinstance(ep.get("endpoint_status"), dict) else "",
            ep.get("creator", ""),
        )

    console.print(table)


@endpoint_app.command("delete")
def endpoint_delete(
    name: str = typer.Option(..., "--name", "-n", help="Endpoint name to delete"),
):
    """Delete a vector search endpoint.

    Example:
        aidevkit vector-search endpoint delete --name my-vs-endpoint
    """
    _delete_vs_endpoint(name=name)
    rprint(f"[green]Endpoint {name} deleted[/green]")


# Index commands
@index_app.command("create-or-update")
def index_create_or_update(
    name: str = typer.Option(..., "--name", "-n", help="Full index name (catalog.schema.index)"),
    endpoint: str = typer.Option(..., "--endpoint", "-e", help="Endpoint name"),
    source_table: Optional[str] = typer.Option(None, "--source-table", "-s", help="Source table for delta sync index"),
    primary_key: Optional[str] = typer.Option(None, "--primary-key", "-p", help="Primary key column"),
    embedding_dimension: Optional[int] = typer.Option(None, "--embedding-dim", help="Embedding dimension for direct access index"),
    embedding_source_column: Optional[str] = typer.Option(None, "--embedding-source", help="Column with embeddings (delta sync)"),
    embedding_model_endpoint: Optional[str] = typer.Option(None, "--embedding-model", help="Model endpoint for computing embeddings"),
    columns_to_sync: Optional[str] = typer.Option(None, "--columns", help="Comma-separated columns to sync"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create or update a vector search index.

    For delta sync index, provide --source-table, --primary-key, and either --embedding-source or --embedding-model.
    For direct access index, provide --primary-key and --embedding-dim.

    Example:
        aidevkit vector-search index create-or-update --name cat.sch.my_index --endpoint my-endpoint --source-table cat.sch.docs --primary-key id --embedding-model databricks-gte-large-en
    """
    cols = columns_to_sync.split(",") if columns_to_sync else None

    result = _create_vs_index(
        index_name=name,
        endpoint_name=endpoint,
        source_table_name=source_table,
        primary_key=primary_key,
        embedding_dimension=embedding_dimension,
        embedding_source_column=embedding_source_column,
        embedding_model_endpoint_name=embedding_model_endpoint,
        columns_to_sync=cols,
    )
    _output_result(result, output_format)


@index_app.command("get")
def index_get(
    name: str = typer.Option(..., "--name", "-n", help="Full index name"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get index details.

    Example:
        aidevkit vector-search index get --name catalog.schema.my_index
    """
    result = _get_vs_index(index_name=name)
    _output_result(result, output_format)


@index_app.command("list")
def index_list(
    endpoint: str = typer.Option(..., "--endpoint", "-e", help="Endpoint name"),
):
    """List indexes on an endpoint.

    Example:
        aidevkit vector-search index list --endpoint my-endpoint
    """
    result = _list_vs_indexes(endpoint_name=endpoint)

    indexes = result.get("vector_indexes", []) if isinstance(result, dict) else result
    if not indexes:
        rprint("[yellow]No indexes found[/yellow]")
        return

    table = Table(title=f"Indexes on {endpoint}")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="yellow")
    table.add_column("Status", style="green")

    for idx in indexes:
        table.add_row(
            idx.get("name", ""),
            idx.get("index_type", ""),
            idx.get("status", {}).get("ready", "") if isinstance(idx.get("status"), dict) else "",
        )

    console.print(table)


@index_app.command("delete")
def index_delete(
    name: str = typer.Option(..., "--name", "-n", help="Full index name to delete"),
):
    """Delete a vector search index.

    Example:
        aidevkit vector-search index delete --name catalog.schema.my_index
    """
    _delete_vs_index(index_name=name)
    rprint(f"[green]Index {name} deleted[/green]")


# Query command
@app.command("query")
def query(
    index: str = typer.Option(..., "--index", "-i", help="Full index name"),
    query_text: Optional[str] = typer.Option(None, "--query-text", "-q", help="Text to search for"),
    query_vector: Optional[str] = typer.Option(None, "--query-vector", help="JSON array of floats"),
    columns: Optional[str] = typer.Option(None, "--columns", "-c", help="Columns to return (comma-separated)"),
    filters: Optional[str] = typer.Option(None, "--filters", help="Filter JSON object"),
    num_results: int = typer.Option(10, "--num-results", "-n", help="Number of results"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Query a vector search index.

    Example:
        aidevkit vector-search query --index cat.sch.my_index --query-text "find similar documents"
        aidevkit vector-search query --index cat.sch.my_index --query-vector "[0.1, 0.2, ...]" --num-results 5
    """
    if not query_text and not query_vector:
        rprint("[red]Error: Provide either --query-text or --query-vector[/red]")
        raise typer.Exit(1)

    cols = columns.split(",") if columns else None
    filters_obj = json.loads(filters) if filters else None
    vector = json.loads(query_vector) if query_vector else None

    result = _query_vs_index(
        index_name=index,
        query_text=query_text,
        query_vector=vector,
        columns=cols,
        filters=filters_obj,
        num_results=num_results,
    )
    _output_result(result, output_format)


# Data commands
@data_app.command("upsert")
def data_upsert(
    index: str = typer.Option(..., "--index", "-i", help="Full index name"),
    data: str = typer.Option(..., "--data", "-d", help="JSON array of records to upsert"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Upsert data into a direct access index.

    Example:
        aidevkit vector-search data upsert --index cat.sch.my_index --data '[{"id": "1", "text": "hello", "embedding": [0.1, ...]}]'
    """
    records = json.loads(data)
    result = _upsert_vs_data(index_name=index, inputs_json=records)
    _output_result(result, output_format)


@data_app.command("delete")
def data_delete(
    index: str = typer.Option(..., "--index", "-i", help="Full index name"),
    primary_keys: str = typer.Option(..., "--keys", "-k", help="JSON array of primary keys to delete"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Delete data from a direct access index.

    Example:
        aidevkit vector-search data delete --index cat.sch.my_index --keys '["id1", "id2"]'
    """
    keys = json.loads(primary_keys)
    result = _delete_vs_data(index_name=index, primary_keys=keys)
    _output_result(result, output_format)


@data_app.command("scan")
def data_scan(
    index: str = typer.Option(..., "--index", "-i", help="Full index name"),
    num_results: int = typer.Option(100, "--num-results", "-n", help="Number of results"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Scan data in an index.

    Example:
        aidevkit vector-search data scan --index cat.sch.my_index --num-results 50
    """
    result = _scan_vs_index(index_name=index, num_results=num_results)
    _output_result(result, output_format)


@data_app.command("sync")
def data_sync(
    index: str = typer.Option(..., "--index", "-i", help="Full index name"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Trigger a sync for a delta sync index.

    Example:
        aidevkit vector-search data sync --index cat.sch.my_index
    """
    result = _sync_vs_index(index_name=index)
    _output_result(result, output_format)


if __name__ == "__main__":
    app()
