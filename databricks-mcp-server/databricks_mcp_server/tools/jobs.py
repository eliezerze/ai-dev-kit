"""
Jobs MCP Tools

Thin MCP wrapper around databricks_tools_core.jobs.jobs_api.
All business logic is in the core module.

2 tools: manage_jobs and manage_job_runs.
"""

from typing import Any, Dict, List, Optional

from databricks_tools_core.identity import get_default_tags
from databricks_tools_core.jobs.jobs_api import (
    manage_jobs as _manage_jobs,
    manage_job_runs as _manage_job_runs,
)
from databricks_tools_core.jobs import delete_job as _delete_job

from ..manifest import register_deleter, track_resource, remove_resource
from ..server import mcp


# CLI_MAPPING for skill transformation
CLI_MAPPING = {
    "manage_jobs": {
        "create": "aidevkit jobs create",
        "get": "aidevkit jobs get",
        "list": "aidevkit jobs list",
        "find_by_name": "aidevkit jobs find-by-name",
        "update": "aidevkit jobs update",
        "delete": "aidevkit jobs delete",
    },
    "manage_job_runs": {
        "run_now": "aidevkit jobs run",
        "get": "aidevkit jobs run-get",
        "get_output": "aidevkit jobs run-output",
        "cancel": "aidevkit jobs run-cancel",
        "list": "aidevkit jobs runs-list",
        "wait": "aidevkit jobs run-wait",
    },
}


def _delete_job_resource(resource_id: str) -> None:
    _delete_job(job_id=int(resource_id))


register_deleter("job", _delete_job_resource)


def _on_job_created(resource_type: str, name: str, resource_id: str, url: Optional[str] = None) -> None:
    """Callback to track job in MCP manifest."""
    track_resource(
        resource_type=resource_type,
        name=name,
        resource_id=resource_id,
        url=url,
    )


# =============================================================================
# Tool 1: manage_jobs
# =============================================================================


@mcp.tool(timeout=60)
def manage_jobs(
    action: str,
    job_id: int = None,
    name: str = None,
    tasks: List[Dict[str, Any]] = None,
    job_clusters: List[Dict[str, Any]] = None,
    environments: List[Dict[str, Any]] = None,
    tags: Dict[str, str] = None,
    timeout_seconds: int = None,
    max_concurrent_runs: int = None,
    email_notifications: Dict[str, Any] = None,
    webhook_notifications: Dict[str, Any] = None,
    notification_settings: Dict[str, Any] = None,
    schedule: Dict[str, Any] = None,
    queue: Dict[str, Any] = None,
    run_as: Dict[str, Any] = None,
    git_source: Dict[str, Any] = None,
    parameters: List[Dict[str, Any]] = None,
    health: Dict[str, Any] = None,
    deployment: Dict[str, Any] = None,
    limit: int = 25,
    expand_tasks: bool = False,
) -> Dict[str, Any]:
    """Manage Databricks jobs: create, get, list, find_by_name, update, delete.

    create: requires name+tasks, serverless default, idempotent (returns existing if same name).
    get/update/delete: require job_id. find_by_name: returns job_id.
    tasks: [{task_key, notebook_task|spark_python_task|..., job_cluster_key or environment_key}].
    job_clusters: Shared cluster definitions tasks can reference. environments: Serverless env configs.
    schedule: {quartz_cron_expression, timezone_id}. git_source: {git_url, git_provider, git_branch}.
    See databricks-jobs skill for task configuration details.
    Returns: create={job_id}, get=full config, list={items}, find_by_name={job_id}, update/delete={status, job_id}."""
    # Handle delete specially to also remove from manifest
    if action.lower() == "delete" and job_id:
        result = _manage_jobs(action=action, job_id=job_id)
        try:
            remove_resource(resource_type="job", resource_id=str(job_id))
        except Exception:
            pass
        return result

    # Delegate to core API
    return _manage_jobs(
        action=action,
        job_id=job_id,
        name=name,
        tasks=tasks,
        job_clusters=job_clusters,
        environments=environments,
        tags=tags,
        timeout_seconds=timeout_seconds,
        max_concurrent_runs=max_concurrent_runs,
        email_notifications=email_notifications,
        webhook_notifications=webhook_notifications,
        notification_settings=notification_settings,
        schedule=schedule,
        queue=queue,
        run_as=run_as,
        git_source=git_source,
        parameters=parameters,
        health=health,
        deployment=deployment,
        limit=limit,
        expand_tasks=expand_tasks,
        on_resource_created=_on_job_created,
        get_default_tags=get_default_tags,
    )


# =============================================================================
# Tool 2: manage_job_runs
# =============================================================================


@mcp.tool(timeout=300)
def manage_job_runs(
    action: str,
    job_id: int = None,
    run_id: int = None,
    idempotency_token: str = None,
    jar_params: List[str] = None,
    notebook_params: Dict[str, str] = None,
    python_params: List[str] = None,
    spark_submit_params: List[str] = None,
    python_named_params: Dict[str, str] = None,
    pipeline_params: Dict[str, Any] = None,
    sql_params: Dict[str, str] = None,
    dbt_commands: List[str] = None,
    queue: Dict[str, Any] = None,
    active_only: bool = False,
    completed_only: bool = False,
    limit: int = 25,
    offset: int = 0,
    start_time_from: int = None,
    start_time_to: int = None,
    timeout: int = 3600,
    poll_interval: int = 10,
) -> Dict[str, Any]:
    """Manage job runs: run_now, get, get_output, cancel, list, wait.

    run_now: requires job_id, returns {run_id}. get/get_output/cancel/wait: require run_id.
    list: filter by job_id/active_only/completed_only. wait: blocks until complete (timeout default 3600s).
    Returns: run_now={run_id}, get=run details, get_output=logs+results, cancel={status}, list={items}, wait=full result."""
    return _manage_job_runs(
        action=action,
        job_id=job_id,
        run_id=run_id,
        idempotency_token=idempotency_token,
        jar_params=jar_params,
        notebook_params=notebook_params,
        python_params=python_params,
        spark_submit_params=spark_submit_params,
        python_named_params=python_named_params,
        pipeline_params=pipeline_params,
        sql_params=sql_params,
        dbt_commands=dbt_commands,
        queue=queue,
        active_only=active_only,
        completed_only=completed_only,
        limit=limit,
        offset=offset,
        start_time_from=start_time_from,
        start_time_to=start_time_to,
        timeout=timeout,
        poll_interval=poll_interval,
    )
