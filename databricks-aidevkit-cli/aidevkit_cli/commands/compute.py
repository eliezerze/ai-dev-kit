"""Compute CLI commands - Execute code and manage clusters/warehouses.

Commands:
    aidevkit compute execute --code "print('hello')"
    aidevkit compute cluster create --name my-cluster
    aidevkit compute cluster start --cluster-id abc123
    aidevkit compute cluster terminate --cluster-id abc123
    aidevkit compute warehouse create --name my-warehouse
    aidevkit compute list
"""

import json
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from databricks_tools_core.compute import (
    run_code_on_serverless as _run_code_on_serverless,
    execute_databricks_command as _execute_databricks_command,
    create_cluster as _create_cluster,
    modify_cluster as _modify_cluster,
    start_cluster as _start_cluster,
    terminate_cluster as _terminate_cluster,
    delete_cluster as _delete_cluster,
    get_cluster_status as _get_cluster_status,
    list_clusters as _list_clusters,
    create_sql_warehouse as _create_sql_warehouse,
    modify_sql_warehouse as _modify_sql_warehouse,
    delete_sql_warehouse as _delete_sql_warehouse,
)

app = typer.Typer(help="Execute code and manage compute resources")
cluster_app = typer.Typer(help="Manage clusters")
warehouse_app = typer.Typer(help="Manage SQL warehouses")
app.add_typer(cluster_app, name="cluster")
app.add_typer(warehouse_app, name="warehouse")
console = Console()


def _output_result(result, output_format: str = "json"):
    """Output result in the specified format."""
    if hasattr(result, 'to_dict'):
        result = result.to_dict()
    if output_format == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


@app.command("execute")
def execute(
    code: Optional[str] = typer.Option(None, "--code", "-c", help="Code to execute"),
    file_path: Optional[str] = typer.Option(None, "--file", "-f", help="File path to execute"),
    language: str = typer.Option("python", "--language", "-l", help="Language: python, sql, scala, r"),
    compute_type: str = typer.Option("auto", "--compute", help="Compute type: auto, serverless, cluster"),
    cluster_id: Optional[str] = typer.Option(None, "--cluster-id", help="Cluster ID (for cluster compute)"),
    timeout: int = typer.Option(600, "--timeout", "-t", help="Timeout in seconds"),
    output_format: str = typer.Option("json", "--format", help="Output format"),
):
    """Execute code on Databricks.

    Auto-selects serverless for Python/SQL, cluster for Scala/R.

    Example:
        aidevkit compute execute --code "print('hello')"
        aidevkit compute execute --code "SELECT 1" --language sql
        aidevkit compute execute --file ./script.py
    """
    if not code and not file_path:
        rprint("[red]Error: Provide either --code or --file[/red]")
        raise typer.Exit(1)

    # Read file if provided
    if file_path and not code:
        with open(file_path, "r") as f:
            code = f.read()

    # Route to appropriate backend
    use_serverless = (
        compute_type == "serverless" or
        (compute_type == "auto" and language.lower() in ["python", "sql"] and not cluster_id)
    )

    if use_serverless:
        result = _run_code_on_serverless(
            code=code,
            language=language,
            timeout=timeout,
        )
    else:
        if not cluster_id:
            # Try to find a running cluster
            clusters = _list_clusters()
            running = [c for c in clusters if c.get("state") == "RUNNING"]
            if not running:
                rprint("[red]Error: No running cluster found. Provide --cluster-id or start a cluster.[/red]")
                raise typer.Exit(1)
            cluster_id = running[0].get("cluster_id")

        result = _execute_databricks_command(
            cluster_id=cluster_id,
            code=code,
            language=language,
            timeout=timeout,
        )

    _output_result(result, output_format)


@cluster_app.command("create")
def cluster_create(
    name: str = typer.Option(..., "--name", "-n", help="Cluster name"),
    spark_version: Optional[str] = typer.Option(None, "--spark-version", help="Spark version"),
    node_type: Optional[str] = typer.Option(None, "--node-type", help="Node type ID"),
    num_workers: int = typer.Option(1, "--workers", "-w", help="Number of workers"),
    autoscale_min: Optional[int] = typer.Option(None, "--autoscale-min", help="Autoscale min workers"),
    autoscale_max: Optional[int] = typer.Option(None, "--autoscale-max", help="Autoscale max workers"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create a new cluster.

    Example:
        aidevkit compute cluster create --name my-cluster --workers 2
        aidevkit compute cluster create --name my-cluster --autoscale-min 1 --autoscale-max 4
    """
    result = _create_cluster(
        cluster_name=name,
        spark_version=spark_version,
        node_type_id=node_type,
        num_workers=num_workers,
        autoscale_min_workers=autoscale_min,
        autoscale_max_workers=autoscale_max,
    )
    _output_result(result, output_format)


@cluster_app.command("modify")
def cluster_modify(
    cluster_id: str = typer.Option(..., "--cluster-id", "-c", help="Cluster ID"),
    num_workers: Optional[int] = typer.Option(None, "--workers", "-w", help="Number of workers"),
    autoscale_min: Optional[int] = typer.Option(None, "--autoscale-min", help="Autoscale min workers"),
    autoscale_max: Optional[int] = typer.Option(None, "--autoscale-max", help="Autoscale max workers"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Modify an existing cluster.

    Example:
        aidevkit compute cluster modify --cluster-id abc123 --workers 4
    """
    result = _modify_cluster(
        cluster_id=cluster_id,
        num_workers=num_workers,
        autoscale_min_workers=autoscale_min,
        autoscale_max_workers=autoscale_max,
    )
    _output_result(result, output_format)


@cluster_app.command("start")
def cluster_start(
    cluster_id: str = typer.Option(..., "--cluster-id", "-c", help="Cluster ID to start"),
):
    """Start a terminated cluster.

    Example:
        aidevkit compute cluster start --cluster-id abc123
    """
    _start_cluster(cluster_id=cluster_id)
    rprint(f"[green]Cluster {cluster_id} starting[/green]")


@cluster_app.command("terminate")
def cluster_terminate(
    cluster_id: str = typer.Option(..., "--cluster-id", "-c", help="Cluster ID to terminate"),
):
    """Terminate a running cluster.

    Example:
        aidevkit compute cluster terminate --cluster-id abc123
    """
    _terminate_cluster(cluster_id=cluster_id)
    rprint(f"[green]Cluster {cluster_id} terminating[/green]")


@cluster_app.command("delete")
def cluster_delete(
    cluster_id: str = typer.Option(..., "--cluster-id", "-c", help="Cluster ID to delete"),
):
    """Permanently delete a cluster.

    Example:
        aidevkit compute cluster delete --cluster-id abc123
    """
    _delete_cluster(cluster_id=cluster_id)
    rprint(f"[green]Cluster {cluster_id} deleted[/green]")


@cluster_app.command("get")
def cluster_get(
    cluster_id: str = typer.Option(..., "--cluster-id", "-c", help="Cluster ID"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get cluster details.

    Example:
        aidevkit compute cluster get --cluster-id abc123
    """
    result = _get_cluster_status(cluster_id=cluster_id)
    _output_result(result, output_format)


@app.command("list")
def list_compute(
    resource: str = typer.Option("clusters", "--resource", "-r", help="Resource type: clusters, warehouses"),
):
    """List compute resources.

    Example:
        aidevkit compute list
        aidevkit compute list --resource warehouses
    """
    if resource == "clusters":
        clusters = _list_clusters()
        if not clusters:
            rprint("[yellow]No clusters found[/yellow]")
            return

        table = Table(title="Clusters")
        table.add_column("Cluster ID", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("State", style="yellow")
        table.add_column("Workers")

        for c in clusters:
            workers = str(c.get("num_workers", c.get("autoscale", {}).get("max_workers", "")))
            table.add_row(
                c.get("cluster_id", ""),
                c.get("cluster_name", ""),
                c.get("state", ""),
                workers,
            )

        console.print(table)
    else:
        rprint("[yellow]Use 'aidevkit sql warehouse list' for SQL warehouses[/yellow]")


@warehouse_app.command("create")
def warehouse_create(
    name: str = typer.Option(..., "--name", "-n", help="Warehouse name"),
    size: str = typer.Option("SMALL", "--size", "-s", help="Cluster size: 2X-SMALL, X-SMALL, SMALL, MEDIUM, LARGE, etc."),
    min_clusters: int = typer.Option(1, "--min", help="Min clusters"),
    max_clusters: int = typer.Option(1, "--max", help="Max clusters"),
    auto_stop_mins: int = typer.Option(10, "--auto-stop", help="Auto-stop after N minutes"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create a SQL warehouse.

    Example:
        aidevkit compute warehouse create --name my-warehouse --size SMALL
    """
    result = _create_sql_warehouse(
        name=name,
        cluster_size=size,
        min_num_clusters=min_clusters,
        max_num_clusters=max_clusters,
        auto_stop_mins=auto_stop_mins,
    )
    _output_result(result, output_format)


@warehouse_app.command("modify")
def warehouse_modify(
    warehouse_id: str = typer.Option(..., "--warehouse-id", "-w", help="Warehouse ID"),
    size: Optional[str] = typer.Option(None, "--size", "-s", help="New cluster size"),
    min_clusters: Optional[int] = typer.Option(None, "--min", help="New min clusters"),
    max_clusters: Optional[int] = typer.Option(None, "--max", help="New max clusters"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Modify a SQL warehouse.

    Example:
        aidevkit compute warehouse modify --warehouse-id abc123 --size MEDIUM
    """
    result = _modify_sql_warehouse(
        warehouse_id=warehouse_id,
        cluster_size=size,
        min_num_clusters=min_clusters,
        max_num_clusters=max_clusters,
    )
    _output_result(result, output_format)


@warehouse_app.command("delete")
def warehouse_delete(
    warehouse_id: str = typer.Option(..., "--warehouse-id", "-w", help="Warehouse ID to delete"),
):
    """Delete a SQL warehouse.

    Example:
        aidevkit compute warehouse delete --warehouse-id abc123
    """
    _delete_sql_warehouse(warehouse_id=warehouse_id)
    rprint(f"[green]Warehouse {warehouse_id} deleted[/green]")


if __name__ == "__main__":
    app()
