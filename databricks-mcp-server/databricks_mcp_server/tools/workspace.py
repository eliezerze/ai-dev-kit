"""Workspace management tool - switch between Databricks workspaces at runtime.

This module is a thin wrapper around databricks_tools_core.workspace_workflows.
All business logic lives in the workflows module.
"""

from typing import Any, Dict, Optional

from databricks_tools_core.workspace_workflows import manage_workspace as _manage_workspace

from ..server import mcp


# CLI_MAPPING for skill transformation
CLI_MAPPING = {
    "manage_workspace": {
        "status": "aidevkit workspace status",
        "list": "aidevkit workspace list",
        "switch": "aidevkit workspace switch",
        "login": "aidevkit workspace login",
    },
}


@mcp.tool(timeout=60)
def manage_workspace(
    action: str,
    profile: Optional[str] = None,
    host: Optional[str] = None,
) -> Dict[str, Any]:
    """Manage active Databricks workspace connection (session-scoped).

    Actions: status (current workspace), list (profiles from ~/.databrickscfg), switch (profile or host), login (OAuth via CLI).
    Returns: {host, profile, username} or {profiles: [...]}."""
    return _manage_workspace(action=action, profile=profile, host=host)
