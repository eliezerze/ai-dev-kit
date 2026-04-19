"""Tag-based table filtering for the Databricks MCP server.

Restricts every SQL execution and table-discovery tool to only those
Unity Catalog tables that carry a configured tag.

In this fork the filter is **on by default** with ``mcp-ready=yes`` —
no environment variables are required. Override via env vars (see
``FilterConfig.from_env``) or disable entirely with::

    MCP_TABLE_FILTER_TAG_NAME=""
"""

from .config import FilterConfig
from ._filter import TableTagFilter, get_table_filter, reset_singleton

__all__ = [
    "FilterConfig",
    "TableTagFilter",
    "get_table_filter",
    "reset_singleton",
]
