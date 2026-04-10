"""Agent Bricks tools - Manage Knowledge Assistants (KA) and Supervisor Agents (MAS).

Thin MCP wrapper around databricks_tools_core.agent_bricks.agent_bricks_api.
All business logic is in the core module.

For Genie Space tools, see genie.py

2 tools:
- manage_ka: create_or_update, get, find_by_name, delete
- manage_mas: create_or_update, get, find_by_name, delete
"""

from typing import Any, Dict, List, Optional

from databricks_tools_core.agent_bricks import AgentBricksManager
from databricks_tools_core.agent_bricks.agent_bricks_api import (
    manage_ka as _manage_ka,
    manage_mas as _manage_mas,
)
from databricks_tools_core.identity import with_description_footer

from ..manifest import register_deleter, track_resource, remove_resource
from ..server import mcp


# CLI_MAPPING for skill transformation
CLI_MAPPING = {
    "manage_ka": {
        "create_or_update": "aidevkit agent-bricks ka create-or-update",
        "get": "aidevkit agent-bricks ka get",
        "find_by_name": "aidevkit agent-bricks ka find-by-name",
        "delete": "aidevkit agent-bricks ka delete",
    },
    "manage_mas": {
        "create_or_update": "aidevkit agent-bricks mas create-or-update",
        "get": "aidevkit agent-bricks mas get",
        "find_by_name": "aidevkit agent-bricks mas find-by-name",
        "delete": "aidevkit agent-bricks mas delete",
    },
}


def _delete_ka_resource(resource_id: str) -> None:
    manager = AgentBricksManager()
    manager.delete(resource_id)


def _delete_mas_resource(resource_id: str) -> None:
    manager = AgentBricksManager()
    manager.delete(resource_id)


register_deleter("knowledge_assistant", _delete_ka_resource)
register_deleter("multi_agent_supervisor", _delete_mas_resource)


def _on_ka_created(resource_type: str, name: str, resource_id: str, url: Optional[str] = None) -> None:
    """Callback to track KA resource in MCP manifest."""
    track_resource(
        resource_type=resource_type,
        name=name,
        resource_id=resource_id,
        url=url,
    )


def _on_mas_created(resource_type: str, name: str, resource_id: str, url: Optional[str] = None) -> None:
    """Callback to track MAS resource in MCP manifest."""
    track_resource(
        resource_type=resource_type,
        name=name,
        resource_id=resource_id,
        url=url,
    )


# ============================================================================
# Tool 1: manage_ka
# ============================================================================


@mcp.tool(timeout=180)
def manage_ka(
    action: str,
    name: str = None,
    volume_path: str = None,
    description: str = None,
    instructions: str = None,
    tile_id: str = None,
    add_examples_from_volume: bool = True,
) -> Dict[str, Any]:
    """Manage Knowledge Assistant (KA) - RAG-based document Q&A.

    Actions: create_or_update (name+volume_path), get (tile_id), find_by_name (name), delete (tile_id).
    volume_path: UC Volume path with documents (e.g., /Volumes/catalog/schema/vol/docs).
    description: What this KA does (shown to users). instructions: How KA should answer queries.
    add_examples_from_volume: scan volume for JSON example files with question/guideline pairs.
    See agent-bricks skill for full details.
    Returns: create_or_update={tile_id, operation, endpoint_status}, get={tile_id, knowledge_sources, examples_count},
    find_by_name={found, tile_id, endpoint_name}, delete={success}."""
    # Handle delete specially to also remove from manifest
    if action.lower() == "delete" and tile_id:
        result = _manage_ka(action=action, tile_id=tile_id)
        if result.get("success"):
            try:
                remove_resource(resource_type="knowledge_assistant", resource_id=tile_id)
            except Exception:
                pass
        return result

    # Delegate to core API
    return _manage_ka(
        action=action,
        name=name,
        volume_path=volume_path,
        description=description,
        instructions=instructions,
        tile_id=tile_id,
        add_examples_from_volume=add_examples_from_volume,
        on_resource_created=_on_ka_created,
        with_description_footer=with_description_footer,
    )


# ============================================================================
# Tool 2: manage_mas
# ============================================================================


@mcp.tool(timeout=180)
def manage_mas(
    action: str,
    name: str = None,
    agents: List[Dict[str, str]] = None,
    description: str = None,
    instructions: str = None,
    tile_id: str = None,
    examples: List[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Manage Supervisor Agent (MAS) - orchestrates multiple agents for query routing.

    Actions: create_or_update (name+agents), get (tile_id), find_by_name (name), delete (tile_id).
    agents: [{name, description (critical for routing), ONE OF: endpoint_name|genie_space_id|ka_tile_id|uc_function_name|connection_name}].
    description: What this MAS does. instructions: Routing rules for the supervisor.
    examples: [{question, guideline}] to train routing behavior.
    See agent-bricks skill for full agent configuration details.
    Returns: create_or_update={tile_id, operation, endpoint_status, agents_count}, get={tile_id, agents, examples_count},
    find_by_name={found, tile_id, agents_count}, delete={success}."""
    # Handle delete specially to also remove from manifest
    if action.lower() == "delete" and tile_id:
        result = _manage_mas(action=action, tile_id=tile_id)
        if result.get("success"):
            try:
                remove_resource(resource_type="multi_agent_supervisor", resource_id=tile_id)
            except Exception:
                pass
        return result

    # Delegate to core API
    return _manage_mas(
        action=action,
        name=name,
        agents=agents,
        description=description,
        instructions=instructions,
        tile_id=tile_id,
        examples=examples,
        on_resource_created=_on_mas_created,
        with_description_footer=with_description_footer,
    )
