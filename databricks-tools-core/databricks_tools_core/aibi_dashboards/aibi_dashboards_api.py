"""AI/BI Dashboards API - High-level dashboard operations.

This module provides action-based wrappers around low-level dashboard functions.
Used by both MCP server and CLI.

Tools:
- manage_dashboard: create_or_update, get, list, delete, publish, unpublish
"""

import json
from typing import Any, Dict, Optional, Union

from .dashboards import (
    create_or_update_dashboard as _create_or_update_dashboard,
    get_dashboard as _get_dashboard,
    list_dashboards as _list_dashboards,
    publish_dashboard as _publish_dashboard,
    trash_dashboard as _trash_dashboard,
    unpublish_dashboard as _unpublish_dashboard,
)


def _none_if_empty(value):
    """Convert empty strings to None (Claude agent sometimes passes '' instead of null)."""
    return None if value == "" else value


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
    # Callback for resource tracking (MCP manifest, etc.)
    on_resource_created: Optional[callable] = None,
) -> Dict[str, Any]:
    """Manage AI/BI dashboards: create, update, get, list, delete, publish.

    CRITICAL: Before calling this function to create or edit a dashboard, you MUST:
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

    Args:
        action: The action to perform.
        display_name: Dashboard display name (for create_or_update).
        parent_path: Workspace path for dashboard (for create_or_update).
        serialized_dashboard: Dashboard JSON config (str or dict, for create_or_update).
        warehouse_id: SQL warehouse ID (for create_or_update, publish).
        publish: Auto-publish after create (default True).
        dashboard_id: Dashboard ID (for get/delete/publish/unpublish).
        embed_credentials: Embed credentials in published dashboard (for publish).
        on_resource_created: Optional callback(resource_type, name, resource_id, url) for tracking.

    Returns:
        Dict with action-specific results.
    """
    act = action.lower()

    # Normalize empty strings
    display_name = _none_if_empty(display_name)
    parent_path = _none_if_empty(parent_path)
    warehouse_id = _none_if_empty(warehouse_id)
    dashboard_id = _none_if_empty(dashboard_id)

    if act == "create_or_update":
        if not all([display_name, parent_path, serialized_dashboard, warehouse_id]):
            return {"error": "create_or_update requires: display_name, parent_path, serialized_dashboard, warehouse_id"}

        # Handle dict or string for serialized_dashboard
        if isinstance(serialized_dashboard, dict):
            serialized_dashboard = json.dumps(serialized_dashboard)

        result = _create_or_update_dashboard(
            display_name=display_name,
            parent_path=parent_path,
            serialized_dashboard=serialized_dashboard,
            warehouse_id=warehouse_id,
            publish=publish,
        )

        # Invoke callback on successful create/update
        if on_resource_created and result.get("success") and result.get("dashboard_id"):
            try:
                on_resource_created(
                    resource_type="dashboard",
                    name=display_name,
                    resource_id=result["dashboard_id"],
                    url=result.get("url"),
                )
            except Exception:
                pass  # Don't let callback failure block main operation

        return result

    elif act == "get":
        if not dashboard_id:
            return {"error": "get requires: dashboard_id"}
        return _get_dashboard(dashboard_id=dashboard_id)

    elif act == "list":
        return _list_dashboards(page_size=200)

    elif act == "delete":
        if not dashboard_id:
            return {"error": "delete requires: dashboard_id"}
        return _trash_dashboard(dashboard_id=dashboard_id)

    elif act == "publish":
        if not dashboard_id:
            return {"error": "publish requires: dashboard_id"}
        if not warehouse_id:
            return {"error": "publish requires: warehouse_id"}
        return _publish_dashboard(
            dashboard_id=dashboard_id,
            warehouse_id=warehouse_id,
            embed_credentials=embed_credentials,
        )

    elif act == "unpublish":
        if not dashboard_id:
            return {"error": "unpublish requires: dashboard_id"}
        return _unpublish_dashboard(dashboard_id=dashboard_id)

    else:
        return {
            "error": f"Invalid action '{action}'. Valid actions: create_or_update, get, list, delete, publish, unpublish"
        }
