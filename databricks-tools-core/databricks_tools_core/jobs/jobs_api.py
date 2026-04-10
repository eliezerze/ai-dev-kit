"""Jobs API - High-level job operations.

This module provides action-based wrappers around low-level job functions.
Used by both MCP server and CLI.

Tools:
- manage_jobs: create, get, list, find_by_name, update, delete
- manage_job_runs: run_now, get, get_output, cancel, list, wait
"""

from typing import Any, Dict, List, Optional

from .models import JobRunResult
from .jobs import (
    list_jobs as _list_jobs,
    get_job as _get_job,
    find_job_by_name as _find_job_by_name,
    create_job as _create_job,
    update_job as _update_job,
    delete_job as _delete_job,
)
from .runs import (
    run_job_now as _run_job_now,
    get_run as _get_run,
    get_run_output as _get_run_output,
    cancel_run as _cancel_run,
    list_runs as _list_runs,
    wait_for_run as _wait_for_run,
)


def _none_if_empty(value):
    """Convert empty strings to None."""
    return None if value == "" else value


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
    # Callbacks
    on_resource_created: Optional[callable] = None,
    get_default_tags: Optional[callable] = None,
) -> Dict[str, Any]:
    """Manage Databricks jobs: create, get, list, find_by_name, update, delete.

    create: requires name+tasks, serverless default, idempotent (returns existing if same name).
    get/update/delete: require job_id. find_by_name: returns job_id.
    tasks: [{task_key, notebook_task|spark_python_task|..., job_cluster_key or environment_key}].
    job_clusters: Shared cluster definitions tasks can reference. environments: Serverless env configs.
    schedule: {quartz_cron_expression, timezone_id}. git_source: {git_url, git_provider, git_branch}.
    See databricks-jobs skill for task configuration details.
    Returns: create={job_id}, get=full config, list={items}, find_by_name={job_id}, update/delete={status, job_id}.

    Args:
        action: The action to perform.
        job_id: Job ID (for get/update/delete).
        name: Job name (for create/find_by_name).
        tasks: List of task definitions.
        job_clusters: Shared cluster definitions.
        environments: Serverless environment configs.
        tags: Job tags dict.
        timeout_seconds: Job timeout.
        max_concurrent_runs: Max parallel runs.
        email_notifications: Email notification settings.
        webhook_notifications: Webhook settings.
        notification_settings: Notification config.
        schedule: Cron schedule config.
        queue: Queue settings.
        run_as: Service principal or user to run as.
        git_source: Git source for notebooks.
        parameters: Job-level parameters.
        health: Health rules.
        deployment: Deployment config.
        limit: Max results for list.
        expand_tasks: Include task details in list.
        on_resource_created: Callback(resource_type, name, resource_id) for tracking.
        get_default_tags: Callback() -> Dict[str,str] for default tags.

    Returns:
        Dict with action-specific results.
    """
    act = action.lower()
    name = _none_if_empty(name)

    if act == "create":
        # Idempotency guard: check if a job with this name already exists.
        existing_job_id = _find_job_by_name(name=name)
        if existing_job_id is not None:
            return {
                "job_id": existing_job_id,
                "already_exists": True,
                "message": (
                    f"Job '{name}' already exists with job_id={existing_job_id}. "
                    "Returning existing job instead of creating a duplicate. "
                    "Use manage_jobs(action='update') to modify it, or "
                    "manage_jobs(action='delete') first to recreate."
                ),
            }

        # Auto-inject default tags if callback provided
        merged_tags = tags or {}
        if get_default_tags:
            try:
                default_tags = get_default_tags()
                merged_tags = {**default_tags, **merged_tags}
            except Exception:
                pass

        result = _create_job(
            name=name,
            tasks=tasks,
            job_clusters=job_clusters,
            environments=environments,
            tags=merged_tags,
            timeout_seconds=timeout_seconds,
            max_concurrent_runs=max_concurrent_runs or 1,
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
        )

        # Track resource on successful create
        if on_resource_created:
            try:
                job_id_val = result.get("job_id") if isinstance(result, dict) else None
                if job_id_val:
                    on_resource_created(
                        resource_type="job",
                        name=name,
                        resource_id=str(job_id_val),
                    )
            except Exception:
                pass

        return result

    elif act == "get":
        return _get_job(job_id=job_id)

    elif act == "list":
        return {"items": _list_jobs(name=name, limit=limit, expand_tasks=expand_tasks)}

    elif act == "find_by_name":
        return {"job_id": _find_job_by_name(name=name)}

    elif act == "update":
        _update_job(
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
        )
        return {"status": "updated", "job_id": job_id}

    elif act == "delete":
        _delete_job(job_id=job_id)
        return {"status": "deleted", "job_id": job_id}

    else:
        raise ValueError(f"Invalid action: '{action}'. Valid: create, get, list, find_by_name, update, delete")


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
    Returns: run_now={run_id}, get=run details, get_output=logs+results, cancel={status}, list={items}, wait=full result.

    Args:
        action: The action to perform.
        job_id: Job ID (for run_now, list).
        run_id: Run ID (for get, get_output, cancel, wait).
        idempotency_token: Token for idempotent run_now.
        jar_params: JAR task parameters.
        notebook_params: Notebook parameters.
        python_params: Python task parameters.
        spark_submit_params: Spark submit parameters.
        python_named_params: Named Python parameters.
        pipeline_params: Pipeline parameters.
        sql_params: SQL parameters.
        dbt_commands: DBT commands.
        queue: Queue settings.
        active_only: Filter to active runs only.
        completed_only: Filter to completed runs only.
        limit: Max results for list.
        offset: Offset for list pagination.
        start_time_from: Filter runs started after this timestamp.
        start_time_to: Filter runs started before this timestamp.
        timeout: Wait timeout in seconds.
        poll_interval: Wait poll interval in seconds.

    Returns:
        Dict with action-specific results.
    """
    act = action.lower()

    if act == "run_now":
        run_id_result = _run_job_now(
            job_id=job_id,
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
        )
        return {"run_id": run_id_result}

    elif act == "get":
        return _get_run(run_id=run_id)

    elif act == "get_output":
        return _get_run_output(run_id=run_id)

    elif act == "cancel":
        _cancel_run(run_id=run_id)
        return {"status": "cancelled", "run_id": run_id}

    elif act == "list":
        return {
            "items": _list_runs(
                job_id=job_id,
                active_only=active_only,
                completed_only=completed_only,
                limit=limit,
                offset=offset,
                start_time_from=start_time_from,
                start_time_to=start_time_to,
            )
        }

    elif act == "wait":
        result = _wait_for_run(run_id=run_id, timeout=timeout, poll_interval=poll_interval)
        return result.to_dict()

    else:
        raise ValueError(f"Invalid action: '{action}'. Valid: run_now, get, get_output, cancel, list, wait")
