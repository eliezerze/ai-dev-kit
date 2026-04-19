"""Tag-based table filtering for the Databricks MCP server.

Restricts every SQL execution and table-discovery tool to only those
Unity Catalog tables that carry a configured tag. Driven by environment
variables — see ``FilterConfig.from_env``.

Disabled by default. To turn on, set::

    MCP_TABLE_FILTER_TAG_NAME=mcp-ready
    MCP_TABLE_FILTER_TAG_VALUE=yes
"""

from .config import FilterConfig
from ._filter import TableTagFilter, get_table_filter, reset_singleton

__all__ = [
    "FilterConfig",
    "TableTagFilter",
    "get_table_filter",
    "reset_singleton",
]
