"""Genie operations - High-level Genie Space management and queries.

This module contains the business logic for Genie operations,
used by both the MCP server and CLI.

Functions:
- manage_genie: create_or_update, get, list, delete, export, import
- ask_genie: query genie spaces
"""

from datetime import timedelta
from typing import Any, Dict, List, Optional

from databricks_tools_core.auth import get_workspace_client

from .manager import AgentBricksManager


# Singleton manager instance
_manager: Optional[AgentBricksManager] = None


def _get_manager() -> AgentBricksManager:
    """Get or create the singleton AgentBricksManager instance."""
    global _manager
    if _manager is None:
        _manager = AgentBricksManager()
    return _manager


def _find_space_by_name(name: str) -> Optional[Any]:
    """Find a Genie Space by name using SDK's list_spaces.

    Returns the GenieSpaceInfo if found, None otherwise.
    """
    w = get_workspace_client()
    page_token = None
    while True:
        response = w.genie.list_spaces(page_size=200, page_token=page_token)
        if response.spaces:
            for space in response.spaces:
                if space.title == name:
                    return space
        if response.next_page_token:
            page_token = response.next_page_token
        else:
            break
    return None


# ============================================================================
# manage_genie
# ============================================================================


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
    """
    act = action.lower()

    if act == "create_or_update":
        if not space_id and not display_name:
            return {"error": "create_or_update requires: display_name (or space_id for updates)"}
        if not space_id and not table_identifiers and not serialized_space:
            return {"error": "create_or_update requires: table_identifiers (or serialized_space)"}

        return _create_or_update_genie_space(
            display_name=display_name,
            table_identifiers=table_identifiers or [],
            warehouse_id=warehouse_id,
            description=description,
            sample_questions=sample_questions,
            space_id=space_id,
            serialized_space=serialized_space,
        )

    elif act == "get":
        if not space_id:
            return {"error": "get requires: space_id"}
        return _get_genie_space(space_id=space_id, include_serialized_space=include_serialized_space)

    elif act == "list":
        return _list_genie_spaces()

    elif act == "delete":
        if not space_id:
            return {"error": "delete requires: space_id"}
        return _delete_genie_space(space_id=space_id)

    elif act == "export":
        if not space_id:
            return {"error": "export requires: space_id"}
        return _export_genie_space(space_id=space_id)

    elif act == "import":
        if not warehouse_id or not serialized_space:
            return {"error": "import requires: warehouse_id, serialized_space"}
        return _import_genie_space(
            warehouse_id=warehouse_id,
            serialized_space=serialized_space,
            title=title,
            description=description,
            parent_path=parent_path,
        )

    else:
        return {"error": f"Invalid action '{action}'. Valid actions: create_or_update, get, list, delete, export, import"}


# ============================================================================
# ask_genie
# ============================================================================


def ask_genie(
    space_id: str,
    question: str,
    conversation_id: Optional[str] = None,
    timeout_seconds: int = 120,
) -> Dict[str, Any]:
    """Ask natural language question to Genie Space. Pass conversation_id for follow-ups.

    Returns: {question, conversation_id, message_id, status, sql, description, columns, data, row_count, text_response}.
    Returns error dict if space doesn't exist or user lacks access.
    """
    from databricks.sdk.errors import NotFound, PermissionDenied

    w = get_workspace_client()

    try:
        if conversation_id:
            result = w.genie.create_message_and_wait(
                space_id=space_id,
                conversation_id=conversation_id,
                content=question,
                timeout=timedelta(seconds=timeout_seconds),
            )
        else:
            result = w.genie.start_conversation_and_wait(
                space_id=space_id,
                content=question,
                timeout=timedelta(seconds=timeout_seconds),
            )
    except PermissionDenied:
        # Genie API returns PermissionDenied for non-existent spaces
        return {"error": f"Genie space '{space_id}' not found or you don't have permission to access it"}
    except NotFound:
        return {"error": f"Genie space '{space_id}' not found"}

    return _format_genie_response(question, result, space_id, w)


# ============================================================================
# Helper Functions
# ============================================================================


def _create_or_update_genie_space(
    display_name: Optional[str],
    table_identifiers: List[str],
    warehouse_id: Optional[str],
    description: Optional[str],
    sample_questions: Optional[List[str]],
    space_id: Optional[str],
    serialized_space: Optional[str],
) -> Dict[str, Any]:
    """Create or update a Genie Space."""
    manager = _get_manager()

    # Auto-detect warehouse if not provided
    if warehouse_id is None:
        warehouse_id = manager.get_best_warehouse_id()
        if warehouse_id is None:
            raise ValueError("No SQL warehouses available. Please provide a warehouse_id or create a warehouse.")

    operation = "created"

    # When serialized_space is provided
    if serialized_space:
        w = get_workspace_client()
        if space_id:
            # Update existing space with serialized config using SDK
            w.genie.update_space(
                space_id=space_id,
                serialized_space=serialized_space,
                title=display_name,
                description=description,
                warehouse_id=warehouse_id,
            )
            operation = "updated"
        else:
            # Check if exists by name, then create or update
            existing = _find_space_by_name(display_name) if display_name else None
            if existing:
                operation = "updated"
                space_id = existing.space_id
                w.genie.update_space(
                    space_id=space_id,
                    serialized_space=serialized_space,
                    title=display_name,
                    description=description,
                    warehouse_id=warehouse_id,
                )
            else:
                # Create new space with serialized config using SDK
                space = w.genie.create_space(
                    warehouse_id=warehouse_id,
                    serialized_space=serialized_space,
                    title=display_name,
                    description=description,
                )
                space_id = space.space_id or ""

    # When serialized_space is not provided
    else:
        if space_id:
            # Update existing space by ID using SDK for proper partial updates
            w = get_workspace_client()
            w.genie.update_space(
                space_id=space_id,
                description=description,
                title=display_name,
                warehouse_id=warehouse_id,
            )
            operation = "updated"
            # Handle sample questions separately if provided
            if sample_questions is not None:
                manager.genie_update_sample_questions(space_id, sample_questions)
            # Handle table_identifiers if provided (requires full update via manager)
            if table_identifiers:
                manager.genie_update(
                    space_id=space_id,
                    display_name=display_name,
                    description=description,
                    warehouse_id=warehouse_id,
                    table_identifiers=table_identifiers,
                )
        else:
            # Check if exists by name first using SDK
            existing = _find_space_by_name(display_name) if display_name else None
            if existing:
                operation = "updated"
                manager.genie_update(
                    space_id=existing.space_id,
                    display_name=display_name,
                    description=description,
                    warehouse_id=warehouse_id,
                    table_identifiers=table_identifiers,
                    sample_questions=sample_questions,
                )
                space_id = existing.space_id
            else:
                # Create new
                result = manager.genie_create(
                    display_name=display_name,
                    warehouse_id=warehouse_id,
                    table_identifiers=table_identifiers,
                    description=description,
                )
                space_id = result.get("space_id", "")

                # Add sample questions if provided
                if sample_questions and space_id:
                    manager.genie_add_sample_questions_batch(space_id, sample_questions)

    return {
        "space_id": space_id,
        "display_name": display_name,
        "operation": operation,
        "warehouse_id": warehouse_id,
        "table_count": len(table_identifiers),
    }


def _get_genie_space(space_id: str, include_serialized_space: bool) -> Dict[str, Any]:
    """Get a Genie Space by ID using SDK.

    Returns error dict if space doesn't exist or user lacks access.
    Note: Genie API returns PermissionDenied for non-existent spaces.
    """
    from databricks.sdk.errors import NotFound, PermissionDenied

    w = get_workspace_client()
    try:
        space = w.genie.get_space(space_id=space_id, include_serialized_space=include_serialized_space)
    except PermissionDenied:
        # Genie API returns PermissionDenied for non-existent spaces
        return {"error": f"Genie space '{space_id}' not found or you don't have permission to access it"}
    except NotFound:
        return {"error": f"Genie space '{space_id}' not found"}

    if not space:
        return {"error": f"Genie space '{space_id}' not found"}

    # Get sample questions using manager
    manager = _get_manager()
    questions_response = manager.genie_list_questions(space_id, question_type="SAMPLE_QUESTION")
    sample_questions = [q.get("question_text", "") for q in questions_response.get("curated_questions", [])]

    # Extract table identifiers from serialized_space if available
    table_identifiers = []
    if space.serialized_space:
        try:
            import json
            serialized = json.loads(space.serialized_space)
            for table in serialized.get("tables", []):
                if table.get("table_identifier"):
                    table_identifiers.append(table["table_identifier"])
        except (json.JSONDecodeError, KeyError):
            pass

    response = {
        "space_id": space.space_id or space_id,
        "display_name": space.title or "",
        "description": space.description or "",
        "warehouse_id": space.warehouse_id or "",
        "table_identifiers": table_identifiers,
        "sample_questions": sample_questions,
    }

    if include_serialized_space:
        response["serialized_space"] = space.serialized_space or ""

    return response


def _list_genie_spaces() -> Dict[str, Any]:
    """List all Genie Spaces with pagination."""
    w = get_workspace_client()
    spaces = []
    page_token = None

    while True:
        response = w.genie.list_spaces(page_size=200, page_token=page_token)
        if response.spaces:
            for space in response.spaces:
                spaces.append(
                    {
                        "space_id": space.space_id,
                        "title": space.title or "",
                        "description": space.description or "",
                    }
                )
        if response.next_page_token:
            page_token = response.next_page_token
        else:
            break

    return {"spaces": spaces}


def _delete_genie_space(space_id: str) -> Dict[str, Any]:
    """Delete a Genie Space using SDK."""
    w = get_workspace_client()
    w.genie.trash_space(space_id=space_id)
    return {"success": True, "space_id": space_id}


def _export_genie_space(space_id: str) -> Dict[str, Any]:
    """Export a Genie Space for migration/backup using SDK."""
    w = get_workspace_client()
    space = w.genie.get_space(space_id=space_id, include_serialized_space=True)
    return {
        "space_id": space.space_id or space_id,
        "title": space.title or "",
        "description": space.description or "",
        "warehouse_id": space.warehouse_id or "",
        "serialized_space": space.serialized_space or "",
    }


def _import_genie_space(
    warehouse_id: str,
    serialized_space: str,
    title: Optional[str],
    description: Optional[str],
    parent_path: Optional[str],
) -> Dict[str, Any]:
    """Import a Genie Space from serialized config using SDK."""
    w = get_workspace_client()
    space = w.genie.create_space(
        warehouse_id=warehouse_id,
        serialized_space=serialized_space,
        title=title,
        description=description,
        parent_path=parent_path,
    )
    imported_space_id = space.space_id or ""

    return {
        "space_id": imported_space_id,
        "title": space.title or title or "",
        "description": space.description or description or "",
        "operation": "imported",
    }


def _format_genie_response(question: str, genie_message: Any, space_id: str, w: Any) -> Dict[str, Any]:
    """Format a Genie SDK response into a clean dictionary."""
    result = {
        "question": question,
        "conversation_id": genie_message.conversation_id,
        "message_id": genie_message.id,
        "status": str(genie_message.status.value) if genie_message.status else "UNKNOWN",
    }

    # Extract data from attachments
    if genie_message.attachments:
        for attachment in genie_message.attachments:
            # Query attachment (SQL and results)
            if attachment.query:
                result["sql"] = attachment.query.query or ""
                result["description"] = attachment.query.description or ""

                # Get row count from metadata
                if attachment.query.query_result_metadata:
                    result["row_count"] = attachment.query.query_result_metadata.row_count

                # Fetch actual data (columns and rows)
                if attachment.attachment_id:
                    try:
                        data_result = w.genie.get_message_query_result_by_attachment(
                            space_id=space_id,
                            conversation_id=genie_message.conversation_id,
                            message_id=genie_message.id,
                            attachment_id=attachment.attachment_id,
                        )
                        if data_result.statement_response:
                            sr = data_result.statement_response
                            # Get columns
                            if sr.manifest and sr.manifest.schema and sr.manifest.schema.columns:
                                result["columns"] = [c.name for c in sr.manifest.schema.columns]
                            # Get data
                            if sr.result and sr.result.data_array:
                                result["data"] = sr.result.data_array
                    except Exception:
                        pass

            # Text attachment (explanation)
            if attachment.text:
                result["text_response"] = attachment.text.content or ""

    return result
