"""Pipeline tools - Manage Spark Declarative Pipelines (SDP).

Thin MCP wrapper around databricks_tools_core.spark_declarative_pipelines.pipelines_api.
All business logic is in the core module.

2 tools:
- manage_pipeline: create, create_or_update, get, update, delete, find_by_name
- manage_pipeline_run: start, get, stop, get_events
"""

from typing import List, Dict, Any, Optional

from databricks_tools_core.identity import get_default_tags
from databricks_tools_core.spark_declarative_pipelines.pipelines_api import (
    manage_pipeline as _manage_pipeline,
    manage_pipeline_run as _manage_pipeline_run,
)
from databricks_tools_core.spark_declarative_pipelines.pipelines import (
    delete_pipeline as _delete_pipeline,
)

from ..manifest import register_deleter, track_resource, remove_resource
from ..server import mcp


# CLI_MAPPING for skill transformation
CLI_MAPPING = {
    "manage_pipeline": {
        "create": "aidevkit pipelines create",
        "create_or_update": "aidevkit pipelines create-or-update",
        "get": "aidevkit pipelines get",
        "update": "aidevkit pipelines update",
        "delete": "aidevkit pipelines delete",
        "find_by_name": "aidevkit pipelines find-by-name",
    },
    "manage_pipeline_run": {
        "start": "aidevkit pipelines run-start",
        "get": "aidevkit pipelines run-get",
        "stop": "aidevkit pipelines run-stop",
        "get_events": "aidevkit pipelines run-events",
    },
}


def _delete_pipeline_resource(resource_id: str) -> None:
    _delete_pipeline(pipeline_id=resource_id)


register_deleter("pipeline", _delete_pipeline_resource)


def _on_pipeline_created(resource_type: str, name: str, resource_id: str, url: Optional[str] = None) -> None:
    """Callback to track pipeline in MCP manifest."""
    track_resource(
        resource_type=resource_type,
        name=name,
        resource_id=resource_id,
        url=url,
    )


# ============================================================================
# Tool 1: manage_pipeline
# ============================================================================


@mcp.tool(timeout=300)
def manage_pipeline(
    action: str,
    # For create/create_or_update/find_by_name:
    name: Optional[str] = None,
    # For create/create_or_update:
    root_path: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    workspace_file_paths: Optional[List[str]] = None,
    extra_settings: Optional[Dict[str, Any]] = None,
    # For create_or_update only:
    start_run: bool = False,
    wait_for_completion: bool = False,
    full_refresh: bool = True,
    timeout: int = 1800,
    # For get/update/delete:
    pipeline_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Manage Spark Declarative Pipelines: create, update, get, delete, find.

    Actions:
    - create: New pipeline. Requires name, root_path, catalog, schema, workspace_file_paths.
      Returns: {pipeline_id}.
    - create_or_update: Idempotent by name. Same params as create.
      start_run=True triggers run after create/update. wait_for_completion=True blocks until done.
      full_refresh=True reprocesses all data. Returns: {pipeline_id, created, success, state}.
    - get: Get pipeline details. Requires pipeline_id. Returns: full pipeline config.
    - update: Modify config. Requires pipeline_id + fields to change. Returns: {status}.
    - delete: Remove pipeline. Requires pipeline_id. Returns: {status}.
    - find_by_name: Find by name. Requires name. Returns: {found, pipeline_id}.

    root_path: Workspace folder for pipeline files (e.g., /Workspace/Users/me/pipelines).
    workspace_file_paths: List of notebook/file paths to include in pipeline.
    extra_settings: Additional config dict (clusters, photon, channel, continuous, etc).
    See databricks-spark-declarative-pipelines skill for configuration details."""
    # Handle delete specially to also remove from manifest
    if action.lower() == "delete" and pipeline_id:
        result = _manage_pipeline(action=action, pipeline_id=pipeline_id)
        try:
            remove_resource(resource_type="pipeline", resource_id=pipeline_id)
        except Exception:
            pass
        return result

    # Delegate to core API
    return _manage_pipeline(
        action=action,
        name=name,
        root_path=root_path,
        catalog=catalog,
        schema=schema,
        workspace_file_paths=workspace_file_paths,
        extra_settings=extra_settings,
        start_run=start_run,
        wait_for_completion=wait_for_completion,
        full_refresh=full_refresh,
        timeout=timeout,
        pipeline_id=pipeline_id,
        on_resource_created=_on_pipeline_created,
        get_default_tags=get_default_tags,
    )


# ============================================================================
# Tool 2: manage_pipeline_run
# ============================================================================


@mcp.tool(timeout=300)
def manage_pipeline_run(
    action: str,
    pipeline_id: str,
    # For start:
    refresh_selection: Optional[List[str]] = None,
    full_refresh: bool = False,
    full_refresh_selection: Optional[List[str]] = None,
    validate_only: bool = False,
    wait: bool = True,
    timeout: int = 300,
    # For get:
    update_id: Optional[str] = None,
    include_config: bool = False,
    full_error_details: bool = False,
    # For get_events:
    max_results: int = 5,
    event_log_level: str = "WARN",
) -> Dict[str, Any]:
    """Manage pipeline runs: start, monitor, stop, get events.

    Actions:
    - start: Trigger pipeline update. Requires pipeline_id.
      wait=True (default) blocks until complete. validate_only=True checks without running.
      full_refresh=True reprocesses all data. refresh_selection: specific tables to refresh.
      Returns: {update_id, state, success, error_summary}.
    - get: Get run status. Requires pipeline_id, update_id.
      include_config=True includes pipeline config. full_error_details=True for verbose errors.
      Returns: {update_id, state, success, error_summary}.
    - stop: Stop running pipeline. Requires pipeline_id.
      Returns: {status}.
    - get_events: Get events/logs for debugging. Requires pipeline_id.
      event_log_level: ERROR, WARN (default), INFO. max_results: number of events (default 5).
      update_id: filter to specific run.
      Returns: list of event dicts.

    See databricks-spark-declarative-pipelines skill for run management details."""
    return _manage_pipeline_run(
        action=action,
        pipeline_id=pipeline_id,
        refresh_selection=refresh_selection,
        full_refresh=full_refresh,
        full_refresh_selection=full_refresh_selection,
        validate_only=validate_only,
        wait=wait,
        timeout=timeout,
        update_id=update_id,
        include_config=include_config,
        full_error_details=full_error_details,
        max_results=max_results,
        event_log_level=event_log_level,
    )
