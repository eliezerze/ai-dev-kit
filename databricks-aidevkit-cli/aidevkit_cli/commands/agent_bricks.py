"""Agent Bricks CLI commands - Manage Knowledge Assistants and Supervisor Agents.

Commands:
    aidevkit agent-bricks ka create-or-update --name "My KA" --volume-path /Volumes/...
    aidevkit agent-bricks ka get --tile-id abc123
    aidevkit agent-bricks ka find-by-name --name "My KA"
    aidevkit agent-bricks ka delete --tile-id abc123
    aidevkit agent-bricks mas create-or-update --name "My MAS" --agents '[...]'
    aidevkit agent-bricks mas get --tile-id abc123
    aidevkit agent-bricks mas find-by-name --name "My MAS"
    aidevkit agent-bricks mas delete --tile-id abc123
"""

import json
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from databricks_tools_core.agent_bricks import AgentBricksManager

app = typer.Typer(help="Manage Knowledge Assistants and Supervisor Agents")
ka_app = typer.Typer(help="Manage Knowledge Assistants (KA)")
mas_app = typer.Typer(help="Manage Multi-Agent Supervisors (MAS)")
app.add_typer(ka_app, name="ka")
app.add_typer(mas_app, name="mas")
console = Console()

# Module-level manager instance
_manager: Optional[AgentBricksManager] = None


def _get_manager() -> AgentBricksManager:
    global _manager
    if _manager is None:
        _manager = AgentBricksManager()
    return _manager


def _output_result(result, output_format: str = "json"):
    """Output result in the specified format."""
    if output_format == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


# Knowledge Assistant commands
@ka_app.command("create-or-update")
def ka_create_or_update(
    name: str = typer.Option(..., "--name", "-n", help="KA name"),
    volume_path: str = typer.Option(..., "--volume-path", "-v", help="UC Volume path with documents"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="KA description"),
    instructions: Optional[str] = typer.Option(None, "--instructions", "-i", help="How KA should answer queries"),
    tile_id: Optional[str] = typer.Option(None, "--tile-id", help="Tile ID (for update)"),
    add_examples: bool = typer.Option(True, "--add-examples/--no-examples", help="Scan volume for example files"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create or update a Knowledge Assistant.

    KA is a RAG-based document Q&A system.

    Example:
        aidevkit agent-bricks ka create-or-update --name "Product Docs KA" --volume-path /Volumes/catalog/schema/volume/docs
    """
    manager = _get_manager()

    # Build knowledge source
    knowledge_sources = [
        {
            "files_source": {
                "name": f"source_{name.replace(' ', '_').lower()}",
                "type": "files",
                "files": {"path": volume_path},
            }
        }
    ]

    result = manager.ka_create_or_update(
        name=name,
        knowledge_sources=knowledge_sources,
        description=description,
        instructions=instructions,
        tile_id=tile_id,
    )
    _output_result(result, output_format)


@ka_app.command("get")
def ka_get(
    tile_id: str = typer.Option(..., "--tile-id", "-t", help="KA tile ID"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get Knowledge Assistant details.

    Example:
        aidevkit agent-bricks ka get --tile-id abc123
    """
    manager = _get_manager()
    result = manager.ka_get(tile_id=tile_id)
    _output_result(result, output_format)


@ka_app.command("find-by-name")
def ka_find_by_name(
    name: str = typer.Option(..., "--name", "-n", help="KA name"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Find Knowledge Assistant by name.

    Example:
        aidevkit agent-bricks ka find-by-name --name "Product Docs KA"
    """
    manager = _get_manager()
    result = manager.find_by_name(name=name)

    if result is None:
        rprint(f"[yellow]KA '{name}' not found[/yellow]")
        raise typer.Exit(1)

    output = {"found": True, "tile_id": result.tile_id, "name": result.name}
    _output_result(output, output_format)


@ka_app.command("delete")
def ka_delete(
    tile_id: str = typer.Option(..., "--tile-id", "-t", help="KA tile ID to delete"),
):
    """Delete a Knowledge Assistant.

    Example:
        aidevkit agent-bricks ka delete --tile-id abc123
    """
    manager = _get_manager()
    manager.delete(tile_id=tile_id)
    rprint(f"[green]Knowledge Assistant {tile_id} deleted[/green]")


# Multi-Agent Supervisor commands
@mas_app.command("create-or-update")
def mas_create_or_update(
    name: str = typer.Option(..., "--name", "-n", help="MAS name"),
    agents: str = typer.Option(..., "--agents", "-a", help="JSON array of agent configs"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="MAS description"),
    instructions: Optional[str] = typer.Option(None, "--instructions", "-i", help="Routing rules for supervisor"),
    tile_id: Optional[str] = typer.Option(None, "--tile-id", help="Tile ID (for update)"),
    examples: Optional[str] = typer.Option(None, "--examples", "-e", help="JSON array of example question/guideline pairs"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create or update a Supervisor Agent (MAS).

    Agents config format: [{"name": "...", "description": "...", "endpoint_name": "..."}]
    Agent types: endpoint_name, genie_space_id, ka_tile_id, uc_function_name, connection_name

    Example:
        aidevkit agent-bricks mas create-or-update --name "My Supervisor" --agents '[{"name": "sales", "description": "Handles sales queries", "genie_space_id": "abc123"}]'
    """
    manager = _get_manager()

    agent_list = json.loads(agents)
    example_list = json.loads(examples) if examples else None

    # Build agent configs for API
    api_agents = []
    for agent in agent_list:
        agent_config = {
            "name": agent.get("name"),
            "description": agent.get("description"),
        }

        if agent.get("endpoint_name"):
            agent_config["agent_type"] = "serving_endpoint"
            agent_config["serving_endpoint"] = {"name": agent["endpoint_name"]}
        elif agent.get("genie_space_id"):
            agent_config["agent_type"] = "genie"
            agent_config["genie_space"] = {"id": agent["genie_space_id"]}
        elif agent.get("ka_tile_id"):
            tile_id_prefix = agent["ka_tile_id"].split("-")[0]
            agent_config["agent_type"] = "serving_endpoint"
            agent_config["serving_endpoint"] = {"name": f"ka-{tile_id_prefix}-endpoint"}
        elif agent.get("uc_function_name"):
            uc_parts = agent["uc_function_name"].split(".")
            agent_config["agent_type"] = "unity_catalog_function"
            agent_config["unity_catalog_function"] = {
                "uc_path": {"catalog": uc_parts[0], "schema": uc_parts[1], "name": uc_parts[2]}
            }
        elif agent.get("connection_name"):
            agent_config["agent_type"] = "external_mcp_server"
            agent_config["external_mcp_server"] = {"connection_name": agent["connection_name"]}

        api_agents.append(agent_config)

    if tile_id:
        existing = manager.mas_get(tile_id)
        if existing:
            result = manager.mas_update(
                tile_id=tile_id,
                name=name,
                description=description,
                instructions=instructions,
                agents=api_agents,
            )
        else:
            rprint(f"[red]MAS {tile_id} not found[/red]")
            raise typer.Exit(1)
    else:
        # Check by name
        existing = manager.mas_find_by_name(name)
        if existing:
            result = manager.mas_update(
                tile_id=existing.tile_id,
                name=name,
                description=description,
                instructions=instructions,
                agents=api_agents,
            )
        else:
            result = manager.mas_create(
                name=name,
                agents=api_agents,
                description=description,
                instructions=instructions,
            )

    _output_result(result, output_format)


@mas_app.command("get")
def mas_get(
    tile_id: str = typer.Option(..., "--tile-id", "-t", help="MAS tile ID"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get Supervisor Agent details.

    Example:
        aidevkit agent-bricks mas get --tile-id abc123
    """
    manager = _get_manager()
    result = manager.mas_get(tile_id=tile_id)
    _output_result(result, output_format)


@mas_app.command("find-by-name")
def mas_find_by_name(
    name: str = typer.Option(..., "--name", "-n", help="MAS name"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Find Supervisor Agent by name.

    Example:
        aidevkit agent-bricks mas find-by-name --name "My Supervisor"
    """
    manager = _get_manager()
    result = manager.mas_find_by_name(name=name)

    if result is None:
        rprint(f"[yellow]MAS '{name}' not found[/yellow]")
        raise typer.Exit(1)

    output = {"found": True, "tile_id": result.tile_id, "name": result.name}
    _output_result(output, output_format)


@mas_app.command("delete")
def mas_delete(
    tile_id: str = typer.Option(..., "--tile-id", "-t", help="MAS tile ID to delete"),
):
    """Delete a Supervisor Agent.

    Example:
        aidevkit agent-bricks mas delete --tile-id abc123
    """
    manager = _get_manager()
    manager.delete(tile_id=tile_id)
    rprint(f"[green]Supervisor Agent {tile_id} deleted[/green]")


if __name__ == "__main__":
    app()
