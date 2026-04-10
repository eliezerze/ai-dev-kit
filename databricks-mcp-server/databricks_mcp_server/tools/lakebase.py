"""Lakebase tools - Manage Lakebase databases (Provisioned and Autoscaling).

Thin MCP wrapper around databricks_tools_core.lakebase.lakebase_api.
All business logic is in the core module.

4 tools:
- manage_lakebase_database: create_or_update, get, list, delete
- manage_lakebase_branch: create_or_update, delete
- manage_lakebase_sync: create_or_update, delete
- generate_lakebase_credential: Generate OAuth tokens
"""

from typing import Any, Dict, List, Optional

from databricks_tools_core.lakebase.lakebase_api import (
    manage_lakebase_database as _manage_lakebase_database,
    manage_lakebase_branch as _manage_lakebase_branch,
    manage_lakebase_sync as _manage_lakebase_sync,
    generate_lakebase_credential as _generate_lakebase_credential,
)

from ..manifest import track_resource
from ..server import mcp


# CLI_MAPPING for skill transformation
CLI_MAPPING = {
    "manage_lakebase_database": {
        "create_or_update": "aidevkit lakebase database create-or-update",
        "get": "aidevkit lakebase database get",
        "list": "aidevkit lakebase database list",
        "delete": "aidevkit lakebase database delete",
    },
    "manage_lakebase_branch": {
        "create_or_update": "aidevkit lakebase branch create-or-update",
        "delete": "aidevkit lakebase branch delete",
    },
    "manage_lakebase_sync": {
        "create_or_update": "aidevkit lakebase sync create-or-update",
        "delete": "aidevkit lakebase sync delete",
    },
    "generate_lakebase_credential": "aidevkit lakebase credential",
}


def _on_lakebase_created(resource_type: str, name: str, resource_id: str, url: Optional[str] = None) -> None:
    """Callback to track lakebase resource in MCP manifest."""
    track_resource(
        resource_type=resource_type,
        name=name,
        resource_id=resource_id,
        url=url,
    )


# ============================================================================
# Tool 1: manage_lakebase_database
# ============================================================================


@mcp.tool(timeout=120)
def manage_lakebase_database(
    action: str,
    name: Optional[str] = None,
    type: str = "provisioned",
    # For create_or_update:
    capacity: str = "CU_1",
    stopped: bool = False,
    display_name: Optional[str] = None,
    pg_version: str = "17",
    # For delete:
    force: bool = False,
) -> Dict[str, Any]:
    """Manage Lakebase PostgreSQL databases: create, update, get, list, delete.

    Actions:
    - create_or_update: Idempotent create/update. Requires name.
      type: "provisioned" (fixed capacity CU_1/2/4/8) or "autoscale" (auto-scaling with branches).
      capacity: For provisioned only. pg_version: For autoscale only.
      Returns: {created: bool, type, ...connection info}.
    - get: Get database details. Requires name.
      For autoscale, includes branches and endpoints.
      Returns: {name, type, state, ...}.
    - list: List all databases. Optional type filter.
      Returns: {databases: [{name, type, ...}]}.
    - delete: Delete database. Requires name.
      force=True cascades to children (provisioned). Autoscale deletes all branches/computes/data.
      Returns: {status, ...}.

    See databricks-lakebase-provisioned or databricks-lakebase-autoscale skill for details."""
    return _manage_lakebase_database(
        action=action,
        name=name,
        type=type,
        capacity=capacity,
        stopped=stopped,
        display_name=display_name,
        pg_version=pg_version,
        force=force,
        on_resource_created=_on_lakebase_created,
    )


# ============================================================================
# Tool 2: manage_lakebase_branch
# ============================================================================


@mcp.tool(timeout=120)
def manage_lakebase_branch(
    action: str,
    # For create_or_update:
    project_name: Optional[str] = None,
    branch_id: Optional[str] = None,
    source_branch: Optional[str] = None,
    ttl_seconds: Optional[int] = None,
    no_expiry: bool = False,
    is_protected: Optional[bool] = None,
    endpoint_type: str = "ENDPOINT_TYPE_READ_WRITE",
    autoscaling_limit_min_cu: Optional[float] = None,
    autoscaling_limit_max_cu: Optional[float] = None,
    scale_to_zero_seconds: Optional[int] = None,
    # For delete:
    name: Optional[str] = None,
) -> Dict[str, Any]:
    """Manage Autoscale branches: create, update, delete.

    Branches are isolated copy-on-write environments with their own compute endpoints.

    Actions:
    - create_or_update: Idempotent create/update. Requires project_name, branch_id.
      source_branch: Branch to fork from (default: production).
      ttl_seconds: Auto-delete after N seconds. is_protected: Prevent accidental deletion.
      autoscaling_limit_min/max_cu: Compute unit limits. scale_to_zero_seconds: Idle time before scaling to zero.
      Returns: {branch details, endpoint connection info, created: bool}.
    - delete: Delete branch and endpoints. Requires name (full branch name).
      Permanently deletes data/databases/roles. Cannot delete protected branches.
      Returns: {status, ...}.

    See databricks-lakebase-autoscale skill for branch workflows."""
    return _manage_lakebase_branch(
        action=action,
        project_name=project_name,
        branch_id=branch_id,
        source_branch=source_branch,
        ttl_seconds=ttl_seconds,
        no_expiry=no_expiry,
        is_protected=is_protected,
        endpoint_type=endpoint_type,
        autoscaling_limit_min_cu=autoscaling_limit_min_cu,
        autoscaling_limit_max_cu=autoscaling_limit_max_cu,
        scale_to_zero_seconds=scale_to_zero_seconds,
        name=name,
    )


# ============================================================================
# Tool 3: manage_lakebase_sync
# ============================================================================


@mcp.tool(timeout=120)
def manage_lakebase_sync(
    action: str,
    # For create_or_update:
    instance_name: Optional[str] = None,
    source_table_name: Optional[str] = None,
    target_table_name: Optional[str] = None,
    catalog_name: Optional[str] = None,
    database_name: str = "databricks_postgres",
    primary_key_columns: Optional[List[str]] = None,
    scheduling_policy: str = "TRIGGERED",
    # For delete:
    table_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Manage Lakebase sync (reverse ETL): create, delete.

    Actions:
    - create_or_update: Set up reverse ETL from Delta table to Lakebase.
      Requires instance_name, source_table_name, target_table_name.
      Creates catalog if needed, then synced table.
      source_table_name: Delta table (catalog.schema.table). target_table_name: Postgres destination.
      primary_key_columns: Required for incremental sync.
      scheduling_policy: TRIGGERED/SNAPSHOT/CONTINUOUS.
      Returns: {catalog, synced_table, created}.
    - delete: Remove synced table, optionally UC catalog. Source Delta table unaffected.
      Requires table_name. Optional catalog_name to also delete catalog.
      Returns: {synced_table, catalog (if deleted)}.

    See databricks-lakebase-provisioned skill for sync workflows."""
    return _manage_lakebase_sync(
        action=action,
        instance_name=instance_name,
        source_table_name=source_table_name,
        target_table_name=target_table_name,
        catalog_name=catalog_name,
        database_name=database_name,
        primary_key_columns=primary_key_columns,
        scheduling_policy=scheduling_policy,
        table_name=table_name,
    )


# ============================================================================
# Tool 4: generate_lakebase_credential
# ============================================================================


@mcp.tool(timeout=30)
def generate_lakebase_credential(
    instance_names: Optional[List[str]] = None,
    endpoint: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate OAuth token (~1hr) for Lakebase connection. Use as password with sslmode=require.

    Provide instance_names (provisioned) or endpoint (autoscale)."""
    return _generate_lakebase_credential(
        instance_names=instance_names,
        endpoint=endpoint,
    )
