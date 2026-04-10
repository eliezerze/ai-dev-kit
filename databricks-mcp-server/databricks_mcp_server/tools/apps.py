"""App tools - Manage Databricks Apps lifecycle.

Consolidated into 1 tool:
- manage_app: create_or_update, get, list, delete

This module is a thin wrapper around databricks_tools_core.apps.apps_api.
All business logic lives in the workflows module.
"""

from typing import Any, Dict, Optional

from databricks_tools_core.apps.apps import delete_app as _delete_app
from databricks_tools_core.apps.apps_api import (
    manage_app as _manage_app,
    set_resource_tracking,
)

from ..manifest import register_deleter, track_resource, remove_resource
from ..server import mcp


# Register resource deleter for cleanup
def _delete_app_resource(resource_id: str) -> None:
    _delete_app(name=resource_id)


register_deleter("app", _delete_app_resource)

# Set up resource tracking callbacks for workflows
set_resource_tracking(track_fn=track_resource, remove_fn=remove_resource)


# CLI_MAPPING for skill transformation
CLI_MAPPING = {
    "manage_app": {
        "create_or_update": "aidevkit apps create-or-update",
        "get": "aidevkit apps get",
        "list": "aidevkit apps list",
        "delete": "aidevkit apps delete",
    },
}


@mcp.tool(timeout=180)
def manage_app(
    action: str,
    # For create_or_update/get/delete:
    name: Optional[str] = None,
    # For create_or_update:
    source_code_path: Optional[str] = None,
    description: Optional[str] = None,
    mode: Optional[str] = None,
    # For get:
    include_logs: bool = False,
    deployment_id: Optional[str] = None,
    # For list:
    name_contains: Optional[str] = None,
) -> Dict[str, Any]:
    """Manage Databricks Apps: create, deploy, get, list, delete.

    Actions:
    - create_or_update: Idempotent create. Deploys if source_code_path provided. Requires name.
      source_code_path: Volume or workspace path to deploy from.
      description: App description. mode: Deployment mode.
      Returns: {name, created: bool, url, status, deployment}.
    - get: Get app details. Requires name.
      include_logs=True for deployment logs. deployment_id for specific deployment.
      Returns: {name, url, status, logs}.
    - list: List all apps. Optional name_contains filter.
      Returns: {apps: [{name, url, status}, ...]}.
    - delete: Delete an app. Requires name.
      Returns: {name, status}.

    See databricks-app-python skill for app development guidance."""
    return _manage_app(
        action=action,
        name=name,
        source_code_path=source_code_path,
        description=description,
        mode=mode,
        include_logs=include_logs,
        deployment_id=deployment_id,
        name_contains=name_contains,
    )
