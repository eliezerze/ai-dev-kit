"""Pipelines API - High-level pipeline operations.

This module provides action-based wrappers around low-level pipeline functions.
Used by both MCP server and CLI.

Tools:
- manage_pipeline: create, create_or_update, get, update, delete, find_by_name
- manage_pipeline_run: start, get, stop, get_events
"""

from typing import Any, Dict, List, Optional

from .pipelines import (
    create_pipeline as _create_pipeline,
    get_pipeline as _get_pipeline,
    update_pipeline as _update_pipeline,
    delete_pipeline as _delete_pipeline,
    start_update as _start_update,
    get_update as _get_update,
    stop_pipeline as _stop_pipeline,
    get_pipeline_events as _get_pipeline_events,
    create_or_update_pipeline as _create_or_update_pipeline,
    find_pipeline_by_name as _find_pipeline_by_name,
)


def _none_if_empty(value):
    """Convert empty strings to None."""
    return None if value == "" else value


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
    # Callbacks
    on_resource_created: Optional[callable] = None,
    get_default_tags: Optional[callable] = None,
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

    Args:
        action: The action to perform.
        name: Pipeline name.
        root_path: Workspace folder for pipeline files.
        catalog: Unity Catalog name.
        schema: Schema name.
        workspace_file_paths: List of notebook/file paths.
        extra_settings: Additional config dict.
        start_run: Trigger run after create/update.
        wait_for_completion: Block until run completes.
        full_refresh: Reprocess all data.
        timeout: Wait timeout in seconds.
        pipeline_id: Pipeline ID (for get/update/delete).
        on_resource_created: Callback for resource tracking.
        get_default_tags: Callback for default tags.

    Returns:
        Dict with action-specific results.
    """
    act = action.lower()

    # Normalize empty strings
    name = _none_if_empty(name)
    root_path = _none_if_empty(root_path)
    catalog = _none_if_empty(catalog)
    schema = _none_if_empty(schema)
    pipeline_id = _none_if_empty(pipeline_id)

    if act == "create":
        if not all([name, root_path, catalog, schema, workspace_file_paths]):
            return {"error": "create requires: name, root_path, catalog, schema, workspace_file_paths"}

        # Auto-inject default tags
        settings = extra_settings or {}
        settings.setdefault("tags", {})
        if get_default_tags:
            try:
                default_tags = get_default_tags()
                settings["tags"] = {**default_tags, **settings["tags"]}
            except Exception:
                pass

        result = _create_pipeline(
            name=name,
            root_path=root_path,
            catalog=catalog,
            schema=schema,
            workspace_file_paths=workspace_file_paths,
            extra_settings=settings,
        )

        # Track resource
        if on_resource_created:
            try:
                if result.pipeline_id:
                    on_resource_created(
                        resource_type="pipeline",
                        name=name,
                        resource_id=result.pipeline_id,
                    )
            except Exception:
                pass

        return {"pipeline_id": result.pipeline_id}

    elif act == "create_or_update":
        if not all([name, root_path, catalog, schema, workspace_file_paths]):
            return {"error": "create_or_update requires: name, root_path, catalog, schema, workspace_file_paths"}

        # Auto-inject default tags
        settings = extra_settings or {}
        settings.setdefault("tags", {})
        if get_default_tags:
            try:
                default_tags = get_default_tags()
                settings["tags"] = {**default_tags, **settings["tags"]}
            except Exception:
                pass

        result = _create_or_update_pipeline(
            name=name,
            root_path=root_path,
            catalog=catalog,
            schema=schema,
            workspace_file_paths=workspace_file_paths,
            start_run=start_run,
            wait_for_completion=wait_for_completion,
            full_refresh=full_refresh,
            timeout=timeout,
            extra_settings=settings,
        )

        # Track resource
        if on_resource_created:
            try:
                result_dict = result.to_dict()
                pid = result_dict.get("pipeline_id")
                if pid:
                    on_resource_created(
                        resource_type="pipeline",
                        name=name,
                        resource_id=pid,
                    )
            except Exception:
                pass

        return result.to_dict()

    elif act == "get":
        if not pipeline_id:
            return {"error": "get requires: pipeline_id"}
        result = _get_pipeline(pipeline_id=pipeline_id)
        return result.as_dict() if hasattr(result, "as_dict") else vars(result)

    elif act == "update":
        if not pipeline_id:
            return {"error": "update requires: pipeline_id"}
        _update_pipeline(
            pipeline_id=pipeline_id,
            name=name,
            root_path=root_path,
            catalog=catalog,
            schema=schema,
            workspace_file_paths=workspace_file_paths,
            extra_settings=extra_settings,
        )
        return {"status": "updated", "pipeline_id": pipeline_id}

    elif act == "delete":
        if not pipeline_id:
            return {"error": "delete requires: pipeline_id"}
        _delete_pipeline(pipeline_id=pipeline_id)
        return {"status": "deleted", "pipeline_id": pipeline_id}

    elif act == "find_by_name":
        if not name:
            return {"error": "find_by_name requires: name"}
        pid = _find_pipeline_by_name(name=name)
        return {"found": pid is not None, "pipeline_id": pid, "name": name}

    else:
        return {
            "error": f"Invalid action '{action}'. Valid actions: create, create_or_update, get, update, delete, find_by_name"
        }


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

    Args:
        action: The action to perform.
        pipeline_id: Pipeline ID.
        refresh_selection: Tables to refresh.
        full_refresh: Reprocess all data.
        full_refresh_selection: Tables for full refresh.
        validate_only: Validate without running.
        wait: Block until complete.
        timeout: Wait timeout in seconds.
        update_id: Update/run ID.
        include_config: Include pipeline config in get.
        full_error_details: Include detailed errors.
        max_results: Max events to return.
        event_log_level: Minimum event level.

    Returns:
        Dict with action-specific results.
    """
    act = action.lower()

    # Normalize empty strings
    pipeline_id = _none_if_empty(pipeline_id)
    update_id = _none_if_empty(update_id)

    if act == "start":
        return _start_update(
            pipeline_id=pipeline_id,
            refresh_selection=refresh_selection,
            full_refresh=full_refresh,
            full_refresh_selection=full_refresh_selection,
            validate_only=validate_only,
            wait=wait,
            timeout=timeout,
            full_error_details=full_error_details,
        )

    elif act == "get":
        if not update_id:
            return {"error": "get requires: update_id"}
        return _get_update(
            pipeline_id=pipeline_id,
            update_id=update_id,
            include_config=include_config,
            full_error_details=full_error_details,
        )

    elif act == "stop":
        _stop_pipeline(pipeline_id=pipeline_id)
        return {"status": "stopped", "pipeline_id": pipeline_id}

    elif act == "get_events":
        # Convert log level to filter expression
        level_filters = {
            "ERROR": "level='ERROR'",
            "WARN": "level in ('ERROR', 'WARN')",
            "INFO": "",  # No filter = all events
        }
        filter_expr = level_filters.get(event_log_level.upper(), level_filters["WARN"])

        events = _get_pipeline_events(
            pipeline_id=pipeline_id,
            max_results=max_results,
            filter=filter_expr,
            update_id=update_id,
        )
        return {"events": [e.as_dict() if hasattr(e, "as_dict") else vars(e) for e in events]}

    else:
        return {"error": f"Invalid action '{action}'. Valid actions: start, get, stop, get_events"}
