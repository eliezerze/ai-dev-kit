"""User tools - Get information about the current Databricks user.

This module is a thin wrapper around databricks_tools_core.auth_workflows.
All business logic lives in the workflows module.
"""

from typing import Dict, Any

from databricks_tools_core.auth_workflows import get_current_user as _get_current_user

from ..server import mcp


# CLI_MAPPING for skill transformation
CLI_MAPPING = {
    "get_current_user": "aidevkit user whoami",
}


@mcp.tool(timeout=30)
def get_current_user() -> Dict[str, Any]:
    """Get current Databricks user identity.

    Returns: {username (email), home_path (/Workspace/Users/user@example.com/)}."""
    return _get_current_user()
