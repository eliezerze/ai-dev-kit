"""Compute tools - Execute code and manage compute resources on Databricks.

Thin MCP wrapper around databricks_tools_core.compute.compute_api.
All business logic is in the core module.

4 tools:
- execute_code: Run code on serverless or cluster compute
- manage_cluster: Create, modify, start, terminate, or delete clusters
- manage_sql_warehouse: Create, modify, or delete SQL warehouses
- list_compute: List/inspect clusters, node types, and spark versions
"""

from typing import Dict, Any

from databricks_tools_core.compute.compute_api import (
    execute_code as _execute_code,
    manage_cluster as _manage_cluster,
    manage_sql_warehouse as _manage_sql_warehouse,
    list_compute as _list_compute,
)

from ..server import mcp


# CLI_MAPPING for skill transformation
CLI_MAPPING = {
    "execute_code": "aidevkit compute execute",
    "manage_cluster": {
        "create": "aidevkit compute cluster create",
        "modify": "aidevkit compute cluster modify",
        "start": "aidevkit compute cluster start",
        "terminate": "aidevkit compute cluster terminate",
        "delete": "aidevkit compute cluster delete",
        "get": "aidevkit compute cluster get",
    },
    "manage_sql_warehouse": {
        "create": "aidevkit compute warehouse create",
        "modify": "aidevkit compute warehouse modify",
        "delete": "aidevkit compute warehouse delete",
    },
    "list_compute": "aidevkit compute list",
}


# ---------------------------------------------------------------------------
# Tool 1: execute_code
# ---------------------------------------------------------------------------


@mcp.tool
def execute_code(
    code: str = None,
    file_path: str = None,
    compute_type: str = "auto",
    cluster_id: str = None,
    context_id: str = None,
    language: str = "python",
    timeout: int = None,
    destroy_context_on_completion: bool = False,
    workspace_path: str = None,
    run_name: str = None,
    job_extra_params: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Execute code on Databricks via serverless or cluster compute.

    Modes:
    - auto (default): Serverless unless cluster_id/context_id given or language is scala/r
    - serverless: No cluster needed, ~30s cold start, best for batch/one-off tasks
    - cluster: State persists via context_id, best for interactive work (but slow ~2min one-off cluster startup)

    - Cluster mode returns context_id. REUSE IT for subsequent calls to skip context creation (Variables/imports persist across calls).
    - Serverless has no context reuse (~30s cold start each time).

    file_path: Run local file (.py/.scala/.sql/.r), auto-detects language.
    workspace_path: Save as notebook in workspace (omit for ephemeral).
    .ipynb: Pass raw JSON with serverless, auto-detected.
    job_extra_params: Extra job params (serverless only). For dependencies:
        {"environments": [{"environment_key": "env", "spec": {"client": "4", "dependencies": ["pandas", "sklearn"]}}]}

    Timeouts: serverless=1800s, cluster=120s, file=600s.
    Returns: {success, output, error, cluster_id, context_id} or {run_id, run_url}."""
    return _execute_code(
        code=code,
        file_path=file_path,
        compute_type=compute_type,
        cluster_id=cluster_id,
        context_id=context_id,
        language=language,
        timeout=timeout,
        destroy_context_on_completion=destroy_context_on_completion,
        workspace_path=workspace_path,
        run_name=run_name,
        job_extra_params=job_extra_params,
    )


# ---------------------------------------------------------------------------
# Tool 2: manage_cluster
# ---------------------------------------------------------------------------


@mcp.tool
def manage_cluster(
    action: str,
    cluster_id: str = None,
    name: str = None,
    num_workers: int = None,
    spark_version: str = None,
    node_type_id: str = None,
    autotermination_minutes: int = None,
    data_security_mode: str = None,
    spark_conf: str = None,
    autoscale_min_workers: int = None,
    autoscale_max_workers: int = None,
) -> Dict[str, Any]:
    """Create, modify, start, terminate, or delete a cluster.

    Actions:
    - create: Requires name. Auto-picks DBR, node type, SINGLE_USER, 120min auto-stop.
    - modify: Requires cluster_id. Only specified params change. Running clusters restart.
    - start: Requires cluster_id. ASK USER FIRST (costs money, 3-8min startup).
    - terminate: Reversible stop. Requires cluster_id.
    - get: returns cluster details. Requires cluster_id.
    - delete: PERMANENT. CONFIRM WITH USER. Requires cluster_id.

    num_workers default 1, ignored if autoscale set. spark_conf: JSON string.
    Returns: {cluster_id, cluster_name, state, message}."""
    return _manage_cluster(
        action=action,
        cluster_id=cluster_id,
        name=name,
        num_workers=num_workers,
        spark_version=spark_version,
        node_type_id=node_type_id,
        autotermination_minutes=autotermination_minutes,
        data_security_mode=data_security_mode,
        spark_conf=spark_conf,
        autoscale_min_workers=autoscale_min_workers,
        autoscale_max_workers=autoscale_max_workers,
    )


# ---------------------------------------------------------------------------
# Tool 3: manage_sql_warehouse
# ---------------------------------------------------------------------------


@mcp.tool
def manage_sql_warehouse(
    action: str,
    warehouse_id: str = None,
    name: str = None,
    size: str = None,
    min_num_clusters: int = None,
    max_num_clusters: int = None,
    auto_stop_mins: int = None,
    warehouse_type: str = None,
    enable_serverless: bool = None,
) -> Dict[str, Any]:
    """Create, modify, or delete a SQL warehouse.

    Actions:
    - create: Requires name. Defaults: serverless PRO, Small, 120min auto-stop.
    - modify: Requires warehouse_id. Only specified params change.
    - delete: PERMANENT. CONFIRM WITH USER. Requires warehouse_id.

    size: "2X-Small" to "4X-Large". Use list_warehouses to list existing.
    Returns: {warehouse_id, name, state, message}."""
    return _manage_sql_warehouse(
        action=action,
        warehouse_id=warehouse_id,
        name=name,
        size=size,
        min_num_clusters=min_num_clusters,
        max_num_clusters=max_num_clusters,
        auto_stop_mins=auto_stop_mins,
        warehouse_type=warehouse_type,
        enable_serverless=enable_serverless,
    )


# ---------------------------------------------------------------------------
# Tool 4: list_compute
# ---------------------------------------------------------------------------


@mcp.tool
def list_compute(
    resource: str = "clusters",
    cluster_id: str = None,
    auto_select: bool = False,
) -> Dict[str, Any]:
    """List compute resources: clusters, node types, or spark versions.

    resource: "clusters" (default), "node_types", or "spark_versions".
    cluster_id: Get specific cluster status (use to poll after starting).
    auto_select: Return best running cluster (prefers "shared" > "demo" in name)."""
    return _list_compute(
        resource=resource,
        cluster_id=cluster_id,
        auto_select=auto_select,
    )
