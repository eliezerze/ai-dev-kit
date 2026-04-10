"""Spark Declarative Pipelines (SDP) operations"""

from . import pipelines as pipelines, workspace_files as workspace_files

# High-level API (used by MCP and CLI)
from .pipelines_api import (
    manage_pipeline as api_manage_pipeline,
    manage_pipeline_run as api_manage_pipeline_run,
)
