"""Genie tools - Create, manage, and query Databricks Genie Spaces.

Consolidated into 2 tools:
- manage_genie: create_or_update, get, list, delete, export, import
- ask_genie: query (hot path - kept separate)
"""

from typing import Any, Dict, List, Optional

from databricks_tools_core.agent_bricks import (
    manage_genie as _manage_genie,
    ask_genie as _ask_genie,
)
from databricks_tools_core.auth import get_workspace_client
from databricks_tools_core.identity import with_description_footer

from ..manifest import register_deleter, track_resource, remove_resource
from ..server import mcp


# CLI_MAPPING for skill transformation
CLI_MAPPING = {
    "manage_genie": {
        "create_or_update": "aidevkit genie create-or-update",
        "get": "aidevkit genie get",
        "list": "aidevkit genie list",
        "delete": "aidevkit genie delete",
        "export": "aidevkit genie export",
        "import": "aidevkit genie import",
    },
    "ask_genie": "aidevkit genie ask",
}


def _delete_genie_resource(resource_id: str) -> None:
    """Delete a genie space using SDK."""
    w = get_workspace_client()
    w.genie.trash_space(space_id=resource_id)


register_deleter("genie_space", _delete_genie_resource)


# ============================================================================
# Tool 1: manage_genie
# ============================================================================


@mcp.tool(timeout=60)
def manage_genie(
    action: str,
    # For create_or_update:
    display_name: Optional[str] = None,
    table_identifiers: Optional[List[str]] = None,
    warehouse_id: Optional[str] = None,
    description: Optional[str] = None,
    sample_questions: Optional[List[str]] = None,
    serialized_space: Optional[str] = None,
    # For get/delete/export:
    space_id: Optional[str] = None,
    # For get:
    include_serialized_space: bool = False,
    # For import:
    title: Optional[str] = None,
    parent_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Manage Genie Spaces: create, update, get, list, delete, export, import.

    Actions:
    - create_or_update: Idempotent by name. Requires display_name, table_identifiers.
      warehouse_id auto-detected if omitted. description: Explains space purpose.
      sample_questions: Example questions shown to users.
      serialized_space: Full config from export (preserves instructions/SQL examples).
      Returns: {space_id, display_name, operation: created|updated, warehouse_id, table_count}.
    - get: Get space details. Requires space_id.
      include_serialized_space=True for full config export.
      Returns: {space_id, display_name, description, warehouse_id, table_identifiers, sample_questions}.
    - list: List all spaces.
      Returns: {spaces: [{space_id, title, description}, ...]}.
    - delete: Delete a space. Requires space_id.
      Returns: {success, space_id}.
    - export: Export space config for migration/backup. Requires space_id.
      Returns: {space_id, title, description, warehouse_id, serialized_space}.
    - import: Import space from serialized_space. Requires warehouse_id, serialized_space.
      Optional title, description, parent_path overrides.
      Returns: {space_id, title, description, operation: imported}.

    See databricks-genie skill for configuration details."""
    # Add description footer for MCP-created resources
    if action.lower() == "create_or_update" and description is not None:
        description = with_description_footer(description)

    result = _manage_genie(
        action=action,
        display_name=display_name,
        table_identifiers=table_identifiers,
        warehouse_id=warehouse_id,
        description=description,
        sample_questions=sample_questions,
        serialized_space=serialized_space,
        space_id=space_id,
        include_serialized_space=include_serialized_space,
        title=title,
        parent_path=parent_path,
    )

    # Track/remove resources for MCP manifest
    if "error" not in result:
        act = action.lower()
        if act == "create_or_update" and result.get("space_id"):
            try:
                track_resource(
                    resource_type="genie_space",
                    name=result.get("display_name", ""),
                    resource_id=result["space_id"],
                )
            except Exception:
                pass
        elif act == "delete" and result.get("success"):
            try:
                remove_resource(resource_type="genie_space", resource_id=result.get("space_id", ""))
            except Exception:
                pass
        elif act == "import" and result.get("space_id"):
            try:
                track_resource(
                    resource_type="genie_space",
                    name=result.get("title", result["space_id"]),
                    resource_id=result["space_id"],
                )
            except Exception:
                pass

    return result


# ============================================================================
# Tool 2: ask_genie (HOT PATH - kept separate for performance)
# ============================================================================


@mcp.tool(timeout=120)
def ask_genie(
    space_id: str,
    question: str,
    conversation_id: Optional[str] = None,
    timeout_seconds: int = 120,
) -> Dict[str, Any]:
    """Ask natural language question to Genie Space. Pass conversation_id for follow-ups.

    Returns: {question, conversation_id, message_id, status, sql, description, columns, data, row_count, text_response, error}."""
    return _ask_genie(
        space_id=space_id,
        question=question,
        conversation_id=conversation_id,
        timeout_seconds=timeout_seconds,
    )
