"""User workflows - High-level user operations.

This module contains the business logic for user operations, used by both
the MCP server and CLI.
"""

from typing import Any, Dict

from .auth import get_current_username


def get_current_user() -> Dict[str, Any]:
    """Get current Databricks user identity.

    Returns:
        Dict with username (email) and home_path (/Workspace/Users/user@example.com/).
    """
    username = get_current_username()
    home_path = f"/Workspace/Users/{username}/" if username else None
    return {
        "username": username,
        "home_path": home_path,
    }
