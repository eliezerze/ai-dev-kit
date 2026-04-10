"""CLI to MCP Tool Mapping Aggregator.

This module aggregates all CLI_MAPPING dicts from MCP tool files and generates
a flat mapping of CLI commands to MCP tool names for skill transformation.

The skill transformer uses this to replace CLI commands in skill files with
MCP tool references when installing for MCP mode.
"""

from typing import Dict


# Import CLI_MAPPING from each tool file
from .tools.agent_bricks import CLI_MAPPING as agent_bricks_mapping
from .tools.aibi_dashboards import CLI_MAPPING as aibi_dashboards_mapping
from .tools.apps import CLI_MAPPING as apps_mapping
from .tools.compute import CLI_MAPPING as compute_mapping
from .tools.file import CLI_MAPPING as file_mapping
from .tools.genie import CLI_MAPPING as genie_mapping
from .tools.jobs import CLI_MAPPING as jobs_mapping
from .tools.lakebase import CLI_MAPPING as lakebase_mapping
from .tools.pdf import CLI_MAPPING as pdf_mapping
from .tools.pipelines import CLI_MAPPING as pipelines_mapping
from .tools.serving import CLI_MAPPING as serving_mapping
from .tools.sql import CLI_MAPPING as sql_mapping
from .tools.unity_catalog import CLI_MAPPING as unity_catalog_mapping
from .tools.user import CLI_MAPPING as user_mapping
from .tools.vector_search import CLI_MAPPING as vector_search_mapping
from .tools.volume_files import CLI_MAPPING as volume_files_mapping
from .tools.workspace import CLI_MAPPING as workspace_mapping


def _flatten_mapping(mcp_tool_name: str, mapping_value, params: list[tuple[str, str]] = None) -> Dict[str, str]:
    """Flatten a CLI_MAPPING entry to {cli_command: mcp_tool_name}.

    Handles nested dicts recursively, building up parameter strings.
    """
    if params is None:
        params = []

    result = {}
    if isinstance(mapping_value, str):
        # Leaf node: CLI command string
        if params:
            param_str = ", ".join(f'{k}="{v}"' for k, v in params)
            result[mapping_value] = f"{mcp_tool_name}({param_str})"
        else:
            result[mapping_value] = mcp_tool_name
    elif isinstance(mapping_value, dict):
        # Nested dict: recurse with accumulated params
        for key, value in mapping_value.items():
            if isinstance(value, str):
                # Single-level nesting: action -> cli_cmd
                new_params = params + [("action", key)]
                param_str = ", ".join(f'{k}="{v}"' for k, v in new_params)
                result[value] = f"{mcp_tool_name}({param_str})"
            elif isinstance(value, dict):
                # Multi-level nesting (e.g., manage_uc_objects -> object_type -> action)
                new_params = params + [("object_type", key)]
                result.update(_flatten_mapping(mcp_tool_name, value, new_params))
    return result


def build_cli_to_mcp_mapping() -> Dict[str, str]:
    """Build a flat mapping of CLI commands to MCP tool calls.

    Returns:
        Dict mapping CLI command patterns to MCP tool names/calls.
        Example: {"aidevkit lakebase database create-or-update": "manage_lakebase_database(action=\"create_or_update\")"}
    """
    all_mappings = [
        agent_bricks_mapping,
        aibi_dashboards_mapping,
        apps_mapping,
        compute_mapping,
        file_mapping,
        genie_mapping,
        jobs_mapping,
        lakebase_mapping,
        pdf_mapping,
        pipelines_mapping,
        serving_mapping,
        sql_mapping,
        unity_catalog_mapping,
        user_mapping,
        vector_search_mapping,
        volume_files_mapping,
        workspace_mapping,
    ]

    result = {}
    for mapping in all_mappings:
        for mcp_tool_name, cli_info in mapping.items():
            result.update(_flatten_mapping(mcp_tool_name, cli_info))

    return result


# Pre-built mapping for use by skill_transformer
CLI_TO_MCP = build_cli_to_mcp_mapping()


def get_mcp_tool_for_cli_command(cli_command: str) -> str | None:
    """Look up the MCP tool for a given CLI command.

    Args:
        cli_command: The full CLI command (e.g., "aidevkit lakebase database create-or-update")

    Returns:
        The MCP tool call string, or None if not found.
    """
    return CLI_TO_MCP.get(cli_command)


if __name__ == "__main__":
    # Print all mappings for debugging
    import json
    print(json.dumps(CLI_TO_MCP, indent=2, sort_keys=True))
