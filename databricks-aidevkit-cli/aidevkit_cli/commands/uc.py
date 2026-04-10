"""Unity Catalog CLI commands - Manage UC objects, grants, storage, and more.

Commands:
    aidevkit uc catalog create --name my_catalog
    aidevkit uc catalog list
    aidevkit uc schema create --catalog my_catalog --name my_schema
    aidevkit uc volume create --catalog my_catalog --schema my_schema --name my_volume
    aidevkit uc grants grant --securable-type table --full-name cat.sch.tbl --principal user@example.com --privileges SELECT,MODIFY
    aidevkit uc tags set --object-type table --full-name cat.sch.tbl --tags '{"env": "prod"}'
"""

import json
from typing import List, Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from databricks_tools_core.unity_catalog import (
    # Catalogs
    list_catalogs as _list_catalogs,
    get_catalog as _get_catalog,
    create_catalog as _create_catalog,
    update_catalog as _update_catalog,
    delete_catalog as _delete_catalog,
    # Schemas
    list_schemas as _list_schemas,
    get_schema as _get_schema,
    create_schema as _create_schema,
    update_schema as _update_schema,
    delete_schema as _delete_schema,
    # Volumes
    list_volumes as _list_volumes,
    get_volume as _get_volume,
    create_volume as _create_volume,
    update_volume as _update_volume,
    delete_volume as _delete_volume,
    # Functions
    list_functions as _list_functions,
    get_function as _get_function,
    delete_function as _delete_function,
    # Grants
    grant_privileges as _grant_privileges,
    revoke_privileges as _revoke_privileges,
    get_grants as _get_grants,
    get_effective_grants as _get_effective_grants,
    # Tags
    set_tags as _set_tags,
    unset_tags as _unset_tags,
    set_comment as _set_comment,
    # Connections
    list_connections as _list_connections,
    get_connection as _get_connection,
    create_connection as _create_connection,
    delete_connection as _delete_connection,
    # Storage
    list_storage_credentials as _list_storage_credentials,
    get_storage_credential as _get_storage_credential,
    list_external_locations as _list_external_locations,
    get_external_location as _get_external_location,
    # Monitors
    create_monitor as _create_monitor,
    get_monitor as _get_monitor,
    run_monitor_refresh as _run_monitor_refresh,
    delete_monitor as _delete_monitor,
    # Metric Views
    create_metric_view as _create_metric_view,
    alter_metric_view as _alter_metric_view,
    describe_metric_view as _describe_metric_view,
    drop_metric_view as _drop_metric_view,
    query_metric_view as _query_metric_view,
    grant_metric_view as _grant_metric_view,
)

app = typer.Typer(help="Manage Unity Catalog resources")
catalog_app = typer.Typer(help="Manage catalogs")
schema_app = typer.Typer(help="Manage schemas")
volume_app = typer.Typer(help="Manage volumes")
function_app = typer.Typer(help="Manage functions")
grants_app = typer.Typer(help="Manage grants/permissions")
tags_app = typer.Typer(help="Manage tags and comments")
connections_app = typer.Typer(help="Manage connections")
storage_app = typer.Typer(help="Manage storage credentials and locations")
monitors_app = typer.Typer(help="Manage Lakehouse monitors")
metric_views_app = typer.Typer(help="Manage metric views (reusable business metrics)")

app.add_typer(catalog_app, name="catalog")
app.add_typer(schema_app, name="schema")
app.add_typer(volume_app, name="volume")
app.add_typer(function_app, name="function")
app.add_typer(grants_app, name="grants")
app.add_typer(tags_app, name="tags")
app.add_typer(connections_app, name="connections")
app.add_typer(storage_app, name="storage")
app.add_typer(monitors_app, name="monitors")
app.add_typer(metric_views_app, name="metric-views")
console = Console()


def _output_result(result, output_format: str = "json"):
    """Output result in the specified format."""
    if hasattr(result, "as_dict"):
        result = result.as_dict()
    if output_format == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


def _to_dict(obj):
    """Convert SDK object to dict."""
    if hasattr(obj, "as_dict"):
        return obj.as_dict()
    if hasattr(obj, "model_dump"):
        return obj.model_dump(exclude_none=True)
    return obj


# ============================================================================
# Catalog commands
# ============================================================================

@catalog_app.command("create")
def catalog_create(
    name: str = typer.Option(..., "--name", "-n", help="Catalog name"),
    comment: Optional[str] = typer.Option(None, "--comment", "-c", help="Catalog comment"),
    storage_root: Optional[str] = typer.Option(None, "--storage-root", help="Storage root location"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create a catalog.

    Example:
        aidevkit uc catalog create --name my_catalog
    """
    result = _to_dict(_create_catalog(name=name, comment=comment, storage_root=storage_root))
    _output_result(result, output_format)


@catalog_app.command("get")
def catalog_get(
    name: str = typer.Option(..., "--name", "-n", help="Catalog name"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get catalog details.

    Example:
        aidevkit uc catalog get --name my_catalog
    """
    result = _to_dict(_get_catalog(catalog_name=name))
    _output_result(result, output_format)


@catalog_app.command("list")
def catalog_list():
    """List all catalogs.

    Example:
        aidevkit uc catalog list
    """
    catalogs = _list_catalogs()

    table = Table(title="Catalogs")
    table.add_column("Name", style="cyan")
    table.add_column("Owner", style="green")
    table.add_column("Comment", style="yellow")

    for c in catalogs:
        cd = _to_dict(c)
        table.add_row(cd.get("name", ""), cd.get("owner", ""), cd.get("comment", "")[:50] if cd.get("comment") else "")

    console.print(table)


@catalog_app.command("update")
def catalog_update(
    name: str = typer.Option(..., "--name", "-n", help="Catalog name"),
    new_name: Optional[str] = typer.Option(None, "--new-name", help="New catalog name"),
    comment: Optional[str] = typer.Option(None, "--comment", "-c", help="New comment"),
    owner: Optional[str] = typer.Option(None, "--owner", help="New owner"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Update a catalog.

    Example:
        aidevkit uc catalog update --name my_catalog --comment "Updated description"
    """
    result = _to_dict(_update_catalog(catalog_name=name, new_name=new_name, comment=comment, owner=owner))
    _output_result(result, output_format)


@catalog_app.command("delete")
def catalog_delete(
    name: str = typer.Option(..., "--name", "-n", help="Catalog name to delete"),
    force: bool = typer.Option(False, "--force", help="Force delete with contents"),
):
    """Delete a catalog.

    Example:
        aidevkit uc catalog delete --name my_catalog
    """
    _delete_catalog(catalog_name=name, force=force)
    rprint(f"[green]Catalog {name} deleted[/green]")


# ============================================================================
# Schema commands
# ============================================================================

@schema_app.command("create")
def schema_create(
    catalog: str = typer.Option(..., "--catalog", "-c", help="Catalog name"),
    name: str = typer.Option(..., "--name", "-n", help="Schema name"),
    comment: Optional[str] = typer.Option(None, "--comment", help="Schema comment"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create a schema.

    Example:
        aidevkit uc schema create --catalog my_catalog --name my_schema
    """
    result = _to_dict(_create_schema(catalog_name=catalog, schema_name=name, comment=comment))
    _output_result(result, output_format)


@schema_app.command("get")
def schema_get(
    full_name: str = typer.Option(..., "--full-name", "-n", help="Full schema name (catalog.schema)"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get schema details.

    Example:
        aidevkit uc schema get --full-name my_catalog.my_schema
    """
    result = _to_dict(_get_schema(full_schema_name=full_name))
    _output_result(result, output_format)


@schema_app.command("list")
def schema_list(
    catalog: str = typer.Option(..., "--catalog", "-c", help="Catalog name"),
):
    """List schemas in a catalog.

    Example:
        aidevkit uc schema list --catalog my_catalog
    """
    schemas = _list_schemas(catalog_name=catalog)

    table = Table(title=f"Schemas in {catalog}")
    table.add_column("Name", style="cyan")
    table.add_column("Owner", style="green")
    table.add_column("Comment", style="yellow")

    for s in schemas:
        sd = _to_dict(s)
        table.add_row(sd.get("name", ""), sd.get("owner", ""), sd.get("comment", "")[:50] if sd.get("comment") else "")

    console.print(table)


@schema_app.command("delete")
def schema_delete(
    full_name: str = typer.Option(..., "--full-name", "-n", help="Full schema name to delete"),
):
    """Delete a schema.

    Example:
        aidevkit uc schema delete --full-name my_catalog.my_schema
    """
    _delete_schema(full_schema_name=full_name)
    rprint(f"[green]Schema {full_name} deleted[/green]")


# ============================================================================
# Volume commands
# ============================================================================

@volume_app.command("create")
def volume_create(
    catalog: str = typer.Option(..., "--catalog", "-c", help="Catalog name"),
    schema: str = typer.Option(..., "--schema", "-s", help="Schema name"),
    name: str = typer.Option(..., "--name", "-n", help="Volume name"),
    volume_type: str = typer.Option("MANAGED", "--type", "-t", help="Volume type: MANAGED or EXTERNAL"),
    storage_location: Optional[str] = typer.Option(None, "--storage-location", help="Storage location (EXTERNAL only)"),
    comment: Optional[str] = typer.Option(None, "--comment", help="Volume comment"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create a volume.

    Example:
        aidevkit uc volume create --catalog cat --schema sch --name my_volume
    """
    result = _to_dict(_create_volume(
        catalog_name=catalog,
        schema_name=schema,
        name=name,
        volume_type=volume_type,
        comment=comment,
        storage_location=storage_location,
    ))
    _output_result(result, output_format)


@volume_app.command("get")
def volume_get(
    full_name: str = typer.Option(..., "--full-name", "-n", help="Full volume name (catalog.schema.volume)"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get volume details.

    Example:
        aidevkit uc volume get --full-name my_catalog.my_schema.my_volume
    """
    result = _to_dict(_get_volume(full_volume_name=full_name))
    _output_result(result, output_format)


@volume_app.command("list")
def volume_list(
    catalog: str = typer.Option(..., "--catalog", "-c", help="Catalog name"),
    schema: str = typer.Option(..., "--schema", "-s", help="Schema name"),
):
    """List volumes in a schema.

    Example:
        aidevkit uc volume list --catalog my_catalog --schema my_schema
    """
    volumes = _list_volumes(catalog_name=catalog, schema_name=schema)

    table = Table(title=f"Volumes in {catalog}.{schema}")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="yellow")
    table.add_column("Owner", style="green")

    for v in volumes:
        vd = _to_dict(v)
        table.add_row(vd.get("name", ""), vd.get("volume_type", ""), vd.get("owner", ""))

    console.print(table)


@volume_app.command("delete")
def volume_delete(
    full_name: str = typer.Option(..., "--full-name", "-n", help="Full volume name to delete"),
):
    """Delete a volume.

    Example:
        aidevkit uc volume delete --full-name my_catalog.my_schema.my_volume
    """
    _delete_volume(full_volume_name=full_name)
    rprint(f"[green]Volume {full_name} deleted[/green]")


# ============================================================================
# Function commands
# ============================================================================

@function_app.command("list")
def function_list(
    catalog: str = typer.Option(..., "--catalog", "-c", help="Catalog name"),
    schema: str = typer.Option(..., "--schema", "-s", help="Schema name"),
):
    """List all functions in a schema.

    Example:
        aidevkit uc function list --catalog my_catalog --schema my_schema
    """
    functions = _list_functions(catalog_name=catalog, schema_name=schema)

    table = Table(title=f"Functions in {catalog}.{schema}")
    table.add_column("Name", style="cyan")
    table.add_column("Full Name", style="green")
    table.add_column("Type", style="yellow")

    for f in functions:
        fd = _to_dict(f)
        table.add_row(
            fd.get("name", ""),
            fd.get("full_name", ""),
            fd.get("routine_type", "FUNCTION"),
        )

    console.print(table)


@function_app.command("get")
def function_get(
    full_name: str = typer.Option(..., "--full-name", "-n", help="Full function name (catalog.schema.function)"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get function details.

    Example:
        aidevkit uc function get --full-name my_catalog.my_schema.my_function
    """
    result = _to_dict(_get_function(full_function_name=full_name))
    _output_result(result, output_format)


@function_app.command("delete")
def function_delete(
    full_name: str = typer.Option(..., "--full-name", "-n", help="Full function name to delete"),
    force: bool = typer.Option(False, "--force", help="Force delete"),
):
    """Delete a function.

    Example:
        aidevkit uc function delete --full-name my_catalog.my_schema.my_function
    """
    _delete_function(full_function_name=full_name, force=force)
    rprint(f"[green]Function {full_name} deleted[/green]")


# ============================================================================
# Grants commands
# ============================================================================

@grants_app.command("grant")
def grants_grant(
    securable_type: str = typer.Option(..., "--securable-type", "-s", help="Type: catalog, schema, table, volume, function"),
    full_name: str = typer.Option(..., "--full-name", "-n", help="Full name of the securable"),
    principal: str = typer.Option(..., "--principal", "-p", help="User, group, or service principal"),
    privileges: str = typer.Option(..., "--privileges", help="Comma-separated privileges"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Grant privileges.

    Example:
        aidevkit uc grants grant --securable-type table --full-name cat.sch.tbl --principal user@example.com --privileges SELECT,MODIFY
    """
    privs = [p.strip() for p in privileges.split(",")]
    result = _grant_privileges(
        securable_type=securable_type,
        full_name=full_name,
        principal=principal,
        privileges=privs,
    )
    _output_result(result, output_format)


@grants_app.command("revoke")
def grants_revoke(
    securable_type: str = typer.Option(..., "--securable-type", "-s", help="Type: catalog, schema, table, volume, function"),
    full_name: str = typer.Option(..., "--full-name", "-n", help="Full name of the securable"),
    principal: str = typer.Option(..., "--principal", "-p", help="User, group, or service principal"),
    privileges: str = typer.Option(..., "--privileges", help="Comma-separated privileges"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Revoke privileges.

    Example:
        aidevkit uc grants revoke --securable-type table --full-name cat.sch.tbl --principal user@example.com --privileges MODIFY
    """
    privs = [p.strip() for p in privileges.split(",")]
    result = _revoke_privileges(
        securable_type=securable_type,
        full_name=full_name,
        principal=principal,
        privileges=privs,
    )
    _output_result(result, output_format)


@grants_app.command("get")
def grants_get(
    securable_type: str = typer.Option(..., "--securable-type", "-s", help="Type: catalog, schema, table, volume, function"),
    full_name: str = typer.Option(..., "--full-name", "-n", help="Full name of the securable"),
    principal: Optional[str] = typer.Option(None, "--principal", "-p", help="Filter by principal"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get grants on a securable.

    Example:
        aidevkit uc grants get --securable-type table --full-name cat.sch.tbl
    """
    result = _get_grants(securable_type=securable_type, full_name=full_name, principal=principal)
    _output_result(result, output_format)


@grants_app.command("get-effective")
def grants_get_effective(
    securable_type: str = typer.Option(..., "--securable-type", "-s", help="Type: catalog, schema, table, volume, function"),
    full_name: str = typer.Option(..., "--full-name", "-n", help="Full name of the securable"),
    principal: Optional[str] = typer.Option(None, "--principal", "-p", help="Filter by principal"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get effective grants (including inherited).

    Example:
        aidevkit uc grants get-effective --securable-type table --full-name cat.sch.tbl
    """
    result = _get_effective_grants(securable_type=securable_type, full_name=full_name, principal=principal)
    _output_result(result, output_format)


# ============================================================================
# Tags commands
# ============================================================================

@tags_app.command("set")
def tags_set(
    object_type: str = typer.Option(..., "--object-type", "-o", help="Object type: catalog, schema, table, column"),
    full_name: str = typer.Option(..., "--full-name", "-n", help="Full object name"),
    tags: str = typer.Option(..., "--tags", "-t", help="Tags as JSON object"),
    column_name: Optional[str] = typer.Option(None, "--column", "-c", help="Column name (for column tags)"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Set tags on an object.

    Example:
        aidevkit uc tags set --object-type table --full-name cat.sch.tbl --tags '{"env": "prod", "team": "data"}'
    """
    tags_dict = json.loads(tags)
    result = _set_tags(object_type=object_type, full_name=full_name, tags=tags_dict, column_name=column_name)
    _output_result(result, output_format)


@tags_app.command("unset")
def tags_unset(
    object_type: str = typer.Option(..., "--object-type", "-o", help="Object type: catalog, schema, table, column"),
    full_name: str = typer.Option(..., "--full-name", "-n", help="Full object name"),
    tag_names: str = typer.Option(..., "--tag-names", "-t", help="Comma-separated tag names to remove"),
    column_name: Optional[str] = typer.Option(None, "--column", "-c", help="Column name (for column tags)"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Remove tags from an object.

    Example:
        aidevkit uc tags unset --object-type table --full-name cat.sch.tbl --tag-names "env,team"
    """
    names = [n.strip() for n in tag_names.split(",")]
    result = _unset_tags(object_type=object_type, full_name=full_name, tag_names=names, column_name=column_name)
    _output_result(result, output_format)


@tags_app.command("set-comment")
def tags_set_comment(
    object_type: str = typer.Option(..., "--object-type", "-o", help="Object type: catalog, schema, table, column"),
    full_name: str = typer.Option(..., "--full-name", "-n", help="Full object name"),
    comment: str = typer.Option(..., "--comment", "-c", help="Comment text"),
    column_name: Optional[str] = typer.Option(None, "--column", help="Column name (for column comments)"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Set comment on an object.

    Example:
        aidevkit uc tags set-comment --object-type table --full-name cat.sch.tbl --comment "Sales data table"
    """
    result = _set_comment(object_type=object_type, full_name=full_name, comment_text=comment, column_name=column_name)
    _output_result(result, output_format)


# ============================================================================
# Connections commands
# ============================================================================

@connections_app.command("create")
def connections_create(
    name: str = typer.Option(..., "--name", "-n", help="Connection name"),
    connection_type: str = typer.Option(..., "--type", "-t", help="Type: SNOWFLAKE, POSTGRESQL, MYSQL, etc."),
    options: str = typer.Option(..., "--options", "-o", help="Connection options as JSON"),
    comment: Optional[str] = typer.Option(None, "--comment", "-c", help="Connection comment"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create a connection.

    Example:
        aidevkit uc connections create --name my_pg --type POSTGRESQL --options '{"host": "...", "port": "5432", "user": "...", "password": "..."}'
    """
    opts = json.loads(options)
    result = _create_connection(name=name, connection_type=connection_type, options=opts, comment=comment)
    _output_result(result, output_format)


@connections_app.command("get")
def connections_get(
    name: str = typer.Option(..., "--name", "-n", help="Connection name"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get connection details.

    Example:
        aidevkit uc connections get --name my_pg
    """
    result = _to_dict(_get_connection(name=name))
    _output_result(result, output_format)


@connections_app.command("list")
def connections_list():
    """List all connections.

    Example:
        aidevkit uc connections list
    """
    connections = _list_connections()

    table = Table(title="Connections")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="yellow")
    table.add_column("Owner", style="green")

    for c in connections:
        cd = _to_dict(c)
        table.add_row(cd.get("name", ""), cd.get("connection_type", ""), cd.get("owner", ""))

    console.print(table)


@connections_app.command("delete")
def connections_delete(
    name: str = typer.Option(..., "--name", "-n", help="Connection name to delete"),
):
    """Delete a connection.

    Example:
        aidevkit uc connections delete --name my_pg
    """
    _delete_connection(name=name)
    rprint(f"[green]Connection {name} deleted[/green]")


# ============================================================================
# Monitors commands
# ============================================================================

@monitors_app.command("create")
def monitors_create(
    table_name: str = typer.Option(..., "--table-name", "-t", help="Full table name"),
    output_schema: str = typer.Option(..., "--output-schema", "-o", help="Output schema for metrics"),
    schedule: Optional[str] = typer.Option(None, "--schedule", "-s", help="Cron schedule"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create a Lakehouse monitor.

    Example:
        aidevkit uc monitors create --table-name cat.sch.tbl --output-schema cat.sch
    """
    result = _create_monitor(table_name=table_name, output_schema_name=output_schema, schedule_cron=schedule)
    _output_result(result, output_format)


@monitors_app.command("get")
def monitors_get(
    table_name: str = typer.Option(..., "--table-name", "-t", help="Full table name"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get monitor details.

    Example:
        aidevkit uc monitors get --table-name cat.sch.tbl
    """
    result = _get_monitor(table_name=table_name)
    _output_result(result, output_format)


@monitors_app.command("refresh")
def monitors_refresh(
    table_name: str = typer.Option(..., "--table-name", "-t", help="Full table name"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Trigger a monitor refresh.

    Example:
        aidevkit uc monitors refresh --table-name cat.sch.tbl
    """
    result = _run_monitor_refresh(table_name=table_name)
    _output_result(result, output_format)


@monitors_app.command("delete")
def monitors_delete(
    table_name: str = typer.Option(..., "--table-name", "-t", help="Full table name"),
):
    """Delete a monitor.

    Example:
        aidevkit uc monitors delete --table-name cat.sch.tbl
    """
    _delete_monitor(table_name=table_name)
    rprint(f"[green]Monitor for {table_name} deleted[/green]")


# ============================================================
# Metric Views commands
# ============================================================


@metric_views_app.command("create")
def metric_views_create(
    full_name: str = typer.Option(..., "--name", "-n", help="Full metric view name (catalog.schema.name)"),
    source: str = typer.Option(..., "--source", "-s", help="Source table/view name"),
    dimensions: str = typer.Option(..., "--dimensions", "-d", help='JSON array of dimensions: [{"name":"dim","expr":"col"}]'),
    measures: str = typer.Option(..., "--measures", "-m", help='JSON array of measures: [{"name":"total","expr":"SUM(amount)"}]'),
    version: str = typer.Option("1.1", "--version", help="Metric view version"),
    comment: Optional[str] = typer.Option(None, "--comment", "-c", help="Comment/description"),
    filter_expr: Optional[str] = typer.Option(None, "--filter", help="Filter expression"),
    joins: Optional[str] = typer.Option(None, "--joins", help="JSON array of join specs"),
    materialization: Optional[str] = typer.Option(None, "--materialization", help="JSON materialization config"),
    or_replace: bool = typer.Option(False, "--or-replace", help="Replace if exists"),
    warehouse_id: Optional[str] = typer.Option(None, "--warehouse-id", "-w", help="SQL warehouse ID"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create a metric view (reusable business metric). Requires DBR 17.2+.

    Example:
        aidevkit uc metric-views create --name cat.sch.sales_metrics \\
            --source cat.sch.orders \\
            --dimensions '[{"name":"region","expr":"region"}]' \\
            --measures '[{"name":"total_sales","expr":"SUM(amount)"}]'
    """
    dims = json.loads(dimensions)
    meas = json.loads(measures)
    join_spec = json.loads(joins) if joins else None
    mat_spec = json.loads(materialization) if materialization else None

    result = _create_metric_view(
        full_name=full_name,
        source=source,
        dimensions=dims,
        measures=meas,
        version=version,
        comment=comment,
        filter_expr=filter_expr,
        joins=join_spec,
        materialization=mat_spec,
        or_replace=or_replace,
        warehouse_id=warehouse_id,
    )
    _output_result(result, output_format)


@metric_views_app.command("alter")
def metric_views_alter(
    full_name: str = typer.Option(..., "--name", "-n", help="Full metric view name"),
    source: str = typer.Option(..., "--source", "-s", help="Source table/view name"),
    dimensions: str = typer.Option(..., "--dimensions", "-d", help='JSON array of dimensions'),
    measures: str = typer.Option(..., "--measures", "-m", help='JSON array of measures'),
    version: str = typer.Option("1.1", "--version", help="Metric view version"),
    comment: Optional[str] = typer.Option(None, "--comment", "-c", help="Comment/description"),
    filter_expr: Optional[str] = typer.Option(None, "--filter", help="Filter expression"),
    joins: Optional[str] = typer.Option(None, "--joins", help="JSON array of join specs"),
    materialization: Optional[str] = typer.Option(None, "--materialization", help="JSON materialization config"),
    warehouse_id: Optional[str] = typer.Option(None, "--warehouse-id", "-w", help="SQL warehouse ID"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Alter an existing metric view.

    Example:
        aidevkit uc metric-views alter --name cat.sch.sales_metrics \\
            --source cat.sch.orders \\
            --dimensions '[{"name":"region","expr":"region"}]' \\
            --measures '[{"name":"total_sales","expr":"SUM(amount)"},{"name":"avg_order","expr":"AVG(amount)"}]'
    """
    dims = json.loads(dimensions)
    meas = json.loads(measures)
    join_spec = json.loads(joins) if joins else None
    mat_spec = json.loads(materialization) if materialization else None

    result = _alter_metric_view(
        full_name=full_name,
        source=source,
        dimensions=dims,
        measures=meas,
        version=version,
        comment=comment,
        filter_expr=filter_expr,
        joins=join_spec,
        materialization=mat_spec,
        warehouse_id=warehouse_id,
    )
    _output_result(result, output_format)


@metric_views_app.command("describe")
def metric_views_describe(
    full_name: str = typer.Option(..., "--name", "-n", help="Full metric view name"),
    warehouse_id: Optional[str] = typer.Option(None, "--warehouse-id", "-w", help="SQL warehouse ID"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Describe a metric view (show definition and metadata).

    Example:
        aidevkit uc metric-views describe --name cat.sch.sales_metrics
    """
    result = _describe_metric_view(full_name=full_name, warehouse_id=warehouse_id)
    _output_result(result, output_format)


@metric_views_app.command("query")
def metric_views_query(
    full_name: str = typer.Option(..., "--name", "-n", help="Full metric view name"),
    measures: str = typer.Option(..., "--measures", "-m", help="Comma-separated measure names to retrieve"),
    dimensions: Optional[str] = typer.Option(None, "--dimensions", "-d", help="Comma-separated dimension names"),
    where: Optional[str] = typer.Option(None, "--where", help="Filter condition"),
    order_by: Optional[str] = typer.Option(None, "--order-by", help="Order by clause"),
    limit: Optional[int] = typer.Option(None, "--limit", "-l", help="Max rows to return"),
    warehouse_id: Optional[str] = typer.Option(None, "--warehouse-id", "-w", help="SQL warehouse ID"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Query a metric view.

    Example:
        aidevkit uc metric-views query --name cat.sch.sales_metrics \\
            --measures total_sales,avg_order --dimensions region --limit 10
    """
    measure_list = [m.strip() for m in measures.split(",")]
    dimension_list = [d.strip() for d in dimensions.split(",")] if dimensions else None

    result = _query_metric_view(
        full_name=full_name,
        measures=measure_list,
        dimensions=dimension_list,
        where=where,
        order_by=order_by,
        limit=limit,
        warehouse_id=warehouse_id,
    )
    _output_result({"data": result}, output_format)


@metric_views_app.command("drop")
def metric_views_drop(
    full_name: str = typer.Option(..., "--name", "-n", help="Full metric view name"),
    warehouse_id: Optional[str] = typer.Option(None, "--warehouse-id", "-w", help="SQL warehouse ID"),
):
    """Drop a metric view.

    Example:
        aidevkit uc metric-views drop --name cat.sch.sales_metrics
    """
    result = _drop_metric_view(full_name=full_name, warehouse_id=warehouse_id)
    rprint(f"[green]Metric view {full_name} dropped[/green]")


@metric_views_app.command("grant")
def metric_views_grant(
    full_name: str = typer.Option(..., "--name", "-n", help="Full metric view name"),
    principal: str = typer.Option(..., "--principal", "-p", help="User, group, or service principal"),
    privileges: str = typer.Option("SELECT", "--privileges", help="Comma-separated privileges (e.g., SELECT)"),
    warehouse_id: Optional[str] = typer.Option(None, "--warehouse-id", "-w", help="SQL warehouse ID"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Grant privileges on a metric view.

    Example:
        aidevkit uc metric-views grant --name cat.sch.sales_metrics \\
            --principal user@example.com --privileges SELECT
    """
    priv_list = [p.strip() for p in privileges.split(",")]
    result = _grant_metric_view(
        full_name=full_name,
        principal=principal,
        privileges=priv_list,
        warehouse_id=warehouse_id,
    )
    _output_result(result, output_format)


if __name__ == "__main__":
    app()
