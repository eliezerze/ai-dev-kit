"""Jobs CLI commands - Manage Databricks Jobs.

Commands:
    aidevkit jobs create --name my-job --tasks '[...]'
    aidevkit jobs get --job-id 123
    aidevkit jobs list
    aidevkit jobs find-by-name --name my-job
    aidevkit jobs update --job-id 123 --tasks '[...]'
    aidevkit jobs delete --job-id 123
    aidevkit jobs run --job-id 123
    aidevkit jobs run-get --run-id 456
    aidevkit jobs run-output --run-id 456
    aidevkit jobs run-cancel --run-id 456
    aidevkit jobs runs-list --job-id 123
    aidevkit jobs run-wait --run-id 456
"""

import json
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from databricks_tools_core.jobs import (
    create_job as _create_job,
    get_job as _get_job,
    list_jobs as _list_jobs,
    find_job_by_name as _find_job_by_name,
    update_job as _update_job,
    delete_job as _delete_job,
    run_job_now as _run_job_now,
    get_run as _get_run,
    get_run_output as _get_run_output,
    cancel_run as _cancel_run,
    list_runs as _list_runs,
    wait_for_run as _wait_for_run,
)

app = typer.Typer(help="Manage Databricks Jobs")
console = Console()


def _output_result(result, output_format: str = "json"):
    """Output result in the specified format."""
    if output_format == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


@app.command("create")
def create(
    name: str = typer.Option(..., "--name", "-n", help="Job name"),
    tasks: str = typer.Option(..., "--tasks", "-t", help="JSON array of task definitions"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="Job description"),
    schedule: Optional[str] = typer.Option(None, "--schedule", help="Cron schedule (JSON)"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Create a new job.

    Example:
        aidevkit jobs create --name my-job --tasks '[{"task_key": "task1", "notebook_task": {"notebook_path": "/path"}}]'
    """
    tasks_list = json.loads(tasks)
    schedule_obj = json.loads(schedule) if schedule else None

    result = _create_job(
        name=name,
        tasks=tasks_list,
        description=description,
        schedule=schedule_obj,
    )
    _output_result(result, output_format)


@app.command("get")
def get(
    job_id: int = typer.Option(..., "--job-id", "-j", help="Job ID"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get job details.

    Example:
        aidevkit jobs get --job-id 123
    """
    result = _get_job(job_id=job_id)
    _output_result(result, output_format)


@app.command("list")
def list_jobs(
    name_contains: Optional[str] = typer.Option(None, "--filter", help="Filter by name contains"),
    limit: int = typer.Option(100, "--limit", "-l", help="Max jobs to return"),
):
    """List all jobs.

    Example:
        aidevkit jobs list
        aidevkit jobs list --filter etl --limit 50
    """
    result = _list_jobs(name=name_contains, limit=limit)

    jobs = result.get("jobs", []) if isinstance(result, dict) else result
    if not jobs:
        rprint("[yellow]No jobs found[/yellow]")
        return

    table = Table(title="Jobs")
    table.add_column("Job ID", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Creator", style="yellow")

    for j in jobs[:50]:  # Limit display
        job_id = str(j.get("job_id", ""))
        name = j.get("settings", {}).get("name", "") if isinstance(j.get("settings"), dict) else ""
        creator = j.get("creator_user_name", "")
        table.add_row(job_id, name, creator)

    console.print(table)
    if len(jobs) > 50:
        rprint(f"[dim]Showing 50 of {len(jobs)} jobs[/dim]")


@app.command("find-by-name")
def find_by_name(
    name: str = typer.Option(..., "--name", "-n", help="Exact job name"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Find job by exact name.

    Example:
        aidevkit jobs find-by-name --name my-job
    """
    result = _find_job_by_name(name=name)
    _output_result(result, output_format)


@app.command("update")
def update(
    job_id: int = typer.Option(..., "--job-id", "-j", help="Job ID to update"),
    name: Optional[str] = typer.Option(None, "--name", "-n", help="New job name"),
    tasks: Optional[str] = typer.Option(None, "--tasks", "-t", help="New tasks JSON array"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="New description"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Update an existing job.

    Example:
        aidevkit jobs update --job-id 123 --name new-name
    """
    tasks_list = json.loads(tasks) if tasks else None

    result = _update_job(
        job_id=job_id,
        name=name,
        tasks=tasks_list,
        description=description,
    )
    _output_result(result, output_format)


@app.command("delete")
def delete(
    job_id: int = typer.Option(..., "--job-id", "-j", help="Job ID to delete"),
):
    """Delete a job.

    Example:
        aidevkit jobs delete --job-id 123
    """
    _delete_job(job_id=job_id)
    rprint(f"[green]Job {job_id} deleted successfully[/green]")


@app.command("run")
def run(
    job_id: int = typer.Option(..., "--job-id", "-j", help="Job ID to run"),
    params: Optional[str] = typer.Option(None, "--params", "-p", help="Job parameters (JSON object)"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Run a job immediately.

    Example:
        aidevkit jobs run --job-id 123
        aidevkit jobs run --job-id 123 --params '{"key": "value"}'
    """
    params_dict = json.loads(params) if params else None
    result = _run_job_now(job_id=job_id, job_parameters=params_dict)
    _output_result(result, output_format)


@app.command("run-get")
def run_get(
    run_id: int = typer.Option(..., "--run-id", "-r", help="Run ID"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get run details.

    Example:
        aidevkit jobs run-get --run-id 456
    """
    result = _get_run(run_id=run_id)
    _output_result(result, output_format)


@app.command("run-output")
def run_output(
    run_id: int = typer.Option(..., "--run-id", "-r", help="Run ID"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Get run output/results.

    Example:
        aidevkit jobs run-output --run-id 456
    """
    result = _get_run_output(run_id=run_id)
    _output_result(result, output_format)


@app.command("run-cancel")
def run_cancel(
    run_id: int = typer.Option(..., "--run-id", "-r", help="Run ID to cancel"),
):
    """Cancel a running job.

    Example:
        aidevkit jobs run-cancel --run-id 456
    """
    _cancel_run(run_id=run_id)
    rprint(f"[green]Run {run_id} cancelled[/green]")


@app.command("runs-list")
def runs_list(
    job_id: Optional[int] = typer.Option(None, "--job-id", "-j", help="Filter by job ID"),
    active_only: bool = typer.Option(False, "--active", "-a", help="Only active runs"),
    limit: int = typer.Option(25, "--limit", "-l", help="Max runs to return"),
):
    """List job runs.

    Example:
        aidevkit jobs runs-list --job-id 123
        aidevkit jobs runs-list --active
    """
    result = _list_runs(job_id=job_id, active_only=active_only, limit=limit)

    runs = result.get("runs", []) if isinstance(result, dict) else result
    if not runs:
        rprint("[yellow]No runs found[/yellow]")
        return

    table = Table(title="Job Runs")
    table.add_column("Run ID", style="cyan")
    table.add_column("Job ID", style="green")
    table.add_column("State", style="yellow")
    table.add_column("Result", style="magenta")

    for r in runs[:25]:
        state = r.get("state", {})
        life_cycle = state.get("life_cycle_state", "") if isinstance(state, dict) else ""
        result_state = state.get("result_state", "") if isinstance(state, dict) else ""
        table.add_row(
            str(r.get("run_id", "")),
            str(r.get("job_id", "")),
            life_cycle,
            result_state,
        )

    console.print(table)


@app.command("run-wait")
def run_wait(
    run_id: int = typer.Option(..., "--run-id", "-r", help="Run ID to wait for"),
    timeout: int = typer.Option(3600, "--timeout", "-t", help="Timeout in seconds"),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Wait for a run to complete.

    Example:
        aidevkit jobs run-wait --run-id 456 --timeout 1800
    """
    result = _wait_for_run(run_id=run_id, timeout=timeout)
    _output_result(result, output_format)


if __name__ == "__main__":
    app()
