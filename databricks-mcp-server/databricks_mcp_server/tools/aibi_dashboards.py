"""AI/BI Dashboard tools - Create and manage AI/BI dashboards.

Note: AI/BI dashboards were previously known as Lakeview dashboards.
The SDK/API still uses the 'lakeview' name internally.

This is a thin MCP wrapper around databricks_tools_core.aibi_dashboards.aibi_dashboards_api.
All business logic is in the core module.
"""

from typing import Any, Dict, Optional, Union

from databricks_tools_core.aibi_dashboards.aibi_dashboards_api import (
    manage_dashboard as _manage_dashboard,
)
from databricks_tools_core.aibi_dashboards import trash_dashboard as _trash_dashboard

from ..manifest import register_deleter, track_resource, remove_resource
from ..server import mcp


# CLI_MAPPING for skill transformation
CLI_MAPPING = {
    "manage_dashboard": {
        "create_or_update": "aidevkit dashboards create-or-update",
        "get": "aidevkit dashboards get",
        "list": "aidevkit dashboards list",
        "delete": "aidevkit dashboards delete",
        "publish": "aidevkit dashboards publish",
        "unpublish": "aidevkit dashboards unpublish",
    },
}


def _delete_dashboard_resource(resource_id: str) -> None:
    _trash_dashboard(dashboard_id=resource_id)


register_deleter("dashboard", _delete_dashboard_resource)


def _on_dashboard_created(resource_type: str, name: str, resource_id: str, url: Optional[str] = None) -> None:
    """Callback to track dashboard in MCP manifest."""
    track_resource(
        resource_type=resource_type,
        name=name,
        resource_id=resource_id,
        url=url,
    )


@mcp.tool(timeout=120)
def manage_dashboard(
    action: str,
    # For create_or_update:
    display_name: Optional[str] = None,
    parent_path: Optional[str] = None,
    serialized_dashboard: Optional[Union[str, dict]] = None,
    warehouse_id: Optional[str] = None,
    # For create_or_update publish option:
    publish: bool = True,
    # For get/delete/publish/unpublish:
    dashboard_id: Optional[str] = None,
    # For publish:
    embed_credentials: bool = True,
) -> Dict[str, Any]:
    """Manage AI/BI dashboards: create, update, get, list, delete, publish.

    CRITICAL: Before calling this tool to create or edit a dashboard, you MUST:
    0. Review the databricks-aibi-dashboards skill to understand widget definitions.
       You must EXACTLY follow the JSON structure detailed in the skill.
    1. Call get_table_stats_and_schema() to get table schemas for your queries.
    2. Call execute_sql() to TEST EVERY dataset query before using in dashboard.
    If you skip validation, widgets WILL show errors!

    Actions:
    - create_or_update: Create/update dashboard from JSON.
      Requires display_name, parent_path, serialized_dashboard, warehouse_id.
      publish=True (default) auto-publishes after create.
      Returns: {success, dashboard_id, path, url, published, error}.
    - get: Get dashboard details. Requires dashboard_id.
      Returns: dashboard config and metadata.
    - list: List all dashboards.
      Returns: {dashboards: [...]}.
    - delete: Soft-delete (moves to trash). Requires dashboard_id.
      Returns: {status, message}.
    - publish: Publish dashboard. Requires dashboard_id, warehouse_id.
      embed_credentials=True allows users without data access to view.
      Returns: {status, dashboard_id}.
    - unpublish: Unpublish dashboard. Requires dashboard_id.
      Returns: {status, dashboard_id}.

    Widget structure rules (for create_or_update):
    - queries is TOP-LEVEL SIBLING of spec (NOT inside spec, NOT named_queries)
    - fields[].name MUST match encodings fieldName exactly
    - Use datasetName (camelCase, not dataSetName)
    - Versions: counter/table/filter=2, bar/line/pie=3
    - Layout: 6-column grid
    - Filter types: filter-multi-select, filter-single-select, filter-date-range-picker
    - Text widget uses textbox_spec (no spec block)"""
    # Handle delete specially to also remove from manifest
    if action.lower() == "delete" and dashboard_id:
        result = _manage_dashboard(
            action=action,
            dashboard_id=dashboard_id,
        )
        try:
            remove_resource(resource_type="dashboard", resource_id=dashboard_id)
        except Exception:
            pass
        return result

    # For all other actions, delegate to core API
    return _manage_dashboard(
        action=action,
        display_name=display_name,
        parent_path=parent_path,
        serialized_dashboard=serialized_dashboard,
        warehouse_id=warehouse_id,
        publish=publish,
        dashboard_id=dashboard_id,
        embed_credentials=embed_credentials,
        on_resource_created=_on_dashboard_created,
    )
