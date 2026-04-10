"""
Vector Search Operations

Functions for managing Databricks Vector Search endpoints, indexes,
and performing similarity queries.
"""

from .endpoints import (
    create_vs_endpoint,
    get_vs_endpoint,
    list_vs_endpoints,
    delete_vs_endpoint,
)
from .indexes import (
    create_vs_index,
    get_vs_index,
    list_vs_indexes,
    delete_vs_index,
    sync_vs_index,
    query_vs_index,
    upsert_vs_data,
    delete_vs_data,
    scan_vs_index,
)

# High-level API (used by MCP and CLI)
from .vector_search_api import (
    manage_vs_endpoint as api_manage_vs_endpoint,
    manage_vs_index as api_manage_vs_index,
    query_vs_index as api_query_vs_index,
    manage_vs_data as api_manage_vs_data,
)

__all__ = [
    # Endpoints
    "create_vs_endpoint",
    "get_vs_endpoint",
    "list_vs_endpoints",
    "delete_vs_endpoint",
    # Indexes
    "create_vs_index",
    "get_vs_index",
    "list_vs_indexes",
    "delete_vs_index",
    "sync_vs_index",
    "query_vs_index",
    "upsert_vs_data",
    "delete_vs_data",
    "scan_vs_index",
    # High-level API
    "api_manage_vs_endpoint",
    "api_manage_vs_index",
    "api_query_vs_index",
    "api_manage_vs_data",
]
