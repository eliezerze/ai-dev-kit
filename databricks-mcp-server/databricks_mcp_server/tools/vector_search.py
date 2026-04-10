"""Vector Search tools - Manage endpoints, indexes, and query vector data.

Thin MCP wrapper around databricks_tools_core.vector_search.vector_search_api.
All business logic is in the core module.

4 tools:
- manage_vs_endpoint: create_or_update, get, list, delete
- manage_vs_index: create_or_update, get, list, delete
- query_vs_index: query vectors (hot path - kept separate)
- manage_vs_data: upsert, delete, scan, sync
"""

from typing import Any, Dict, List, Optional, Union

from databricks_tools_core.vector_search.vector_search_api import (
    manage_vs_endpoint as _manage_vs_endpoint,
    manage_vs_index as _manage_vs_index,
    query_vs_index as _query_vs_index,
    manage_vs_data as _manage_vs_data,
)

from ..manifest import track_resource
from ..server import mcp


# CLI_MAPPING for skill transformation
CLI_MAPPING = {
    "manage_vs_endpoint": {
        "create_or_update": "aidevkit vector-search endpoint create-or-update",
        "get": "aidevkit vector-search endpoint get",
        "list": "aidevkit vector-search endpoint list",
        "delete": "aidevkit vector-search endpoint delete",
    },
    "manage_vs_index": {
        "create_or_update": "aidevkit vector-search index create-or-update",
        "get": "aidevkit vector-search index get",
        "list": "aidevkit vector-search index list",
        "delete": "aidevkit vector-search index delete",
    },
    "query_vs_index": "aidevkit vector-search query",
    "manage_vs_data": {
        "upsert": "aidevkit vector-search data upsert",
        "delete": "aidevkit vector-search data delete",
        "scan": "aidevkit vector-search data scan",
        "sync": "aidevkit vector-search data sync",
    },
}


def _on_vs_resource_created(resource_type: str, name: str, resource_id: str, url: Optional[str] = None) -> None:
    """Callback to track vector search resource in MCP manifest."""
    track_resource(
        resource_type=resource_type,
        name=name,
        resource_id=resource_id,
        url=url,
    )


# ============================================================================
# Tool 1: manage_vs_endpoint
# ============================================================================


@mcp.tool(timeout=120)
def manage_vs_endpoint(
    action: str,
    name: Optional[str] = None,
    endpoint_type: str = "STANDARD",
) -> Dict[str, Any]:
    """Manage Vector Search endpoints: create, get, list, delete.

    Actions:
    - create_or_update: Idempotent create. Returns existing if found. Requires name.
      endpoint_type: "STANDARD" (<100ms latency) or "STORAGE_OPTIMIZED" (~250ms, 1B+ vectors).
      Async creation - poll with action="get" until state=ONLINE.
      Returns: {name, endpoint_type, state, created: bool}.
    - get: Get endpoint details. Requires name.
      Returns: {name, state, num_indexes, ...}.
    - list: List all endpoints.
      Returns: {endpoints: [{name, state, ...}, ...]}.
    - delete: Delete endpoint. All indexes must be deleted first. Requires name.
      Returns: {name, status}.

    See databricks-vector-search skill for endpoint configuration."""
    return _manage_vs_endpoint(
        action=action,
        name=name,
        endpoint_type=endpoint_type,
        on_resource_created=_on_vs_resource_created,
    )


# ============================================================================
# Tool 2: manage_vs_index
# ============================================================================


@mcp.tool(timeout=120)
def manage_vs_index(
    action: str,
    # For create_or_update:
    name: Optional[str] = None,
    endpoint_name: Optional[str] = None,
    primary_key: Optional[str] = None,
    index_type: str = "DELTA_SYNC",
    delta_sync_index_spec: Optional[Dict[str, Any]] = None,
    direct_access_index_spec: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Manage Vector Search indexes: create, get, list, delete.

    Actions:
    - create_or_update: Idempotent create. Returns existing if found. Auto-triggers initial sync for DELTA_SYNC.
      Requires name, endpoint_name, primary_key.
      index_type: "DELTA_SYNC" (auto-sync from Delta table) or "DIRECT_ACCESS" (manual CRUD via manage_vs_data).
      delta_sync_index_spec: {source_table, embedding_source_columns OR embedding_vector_columns, pipeline_type}.
        - embedding_source_columns: List of text columns for managed embeddings (Databricks generates vectors).
        - embedding_vector_columns: List of {name, dimension} for self-managed embeddings (you provide vectors).
        - pipeline_type: "TRIGGERED" (manual sync) or "CONTINUOUS" (auto-sync on changes).
      direct_access_index_spec: {embedding_vector_columns: [{name, dimension}], schema_json}.
      Returns: {name, created: bool, sync_triggered}.
    - get: Get index details. Requires name (format: catalog.schema.index_name).
      Returns: {name, state, index_type, ...}.
    - list: List indexes. Optional endpoint_name to filter. Omit for all indexes across all endpoints.
      Returns: {indexes: [...]}.
    - delete: Delete index. Requires name.
      Returns: {name, status}.

    See databricks-vector-search skill for full spec details and examples."""
    return _manage_vs_index(
        action=action,
        name=name,
        endpoint_name=endpoint_name,
        primary_key=primary_key,
        index_type=index_type,
        delta_sync_index_spec=delta_sync_index_spec,
        direct_access_index_spec=direct_access_index_spec,
        on_resource_created=_on_vs_resource_created,
    )


# ============================================================================
# Tool 3: query_vs_index (HOT PATH - kept separate for performance)
# ============================================================================


@mcp.tool(timeout=60)
def query_vs_index(
    index_name: str,
    columns: List[str],
    query_text: Optional[str] = None,
    query_vector: Optional[List[float]] = None,
    num_results: int = 5,
    filters_json: Optional[Union[str, dict]] = None,
    filter_string: Optional[str] = None,
    query_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Query a Vector Search index for similar documents.

    Use ONE OF:
    - query_text: For managed embeddings (Databricks generates vector from text).
    - query_vector: For self-managed embeddings (you provide the vector).

    columns: List of columns to return in results.
    num_results: Number of results to return (default 5).
    Filters (use one based on endpoint type):
    - filters_json: For STANDARD endpoints. Dict like {"field": "value"} or {"field NOT": "value"}.
    - filter_string: For STORAGE_OPTIMIZED endpoints. SQL WHERE clause like "field = 'value'".
    query_type: "ANN" (default, approximate) or "HYBRID" (combines vector + keyword search).

    Returns: {columns, data (with similarity score appended), num_results}."""
    return _query_vs_index(
        index_name=index_name,
        columns=columns,
        query_text=query_text,
        query_vector=query_vector,
        num_results=num_results,
        filters_json=filters_json,
        filter_string=filter_string,
        query_type=query_type,
    )


# ============================================================================
# Tool 4: manage_vs_data
# ============================================================================


@mcp.tool(timeout=120)
def manage_vs_data(
    action: str,
    index_name: str,
    # For upsert:
    inputs_json: Optional[Union[str, list]] = None,
    # For delete:
    primary_keys: Optional[List[str]] = None,
    # For scan:
    num_results: int = 100,
) -> Dict[str, Any]:
    """Manage Vector Search index data: upsert, delete, scan, sync.

    Actions:
    - upsert: Insert or update records. Requires inputs_json.
      inputs_json: List of records, each with primary key + embedding vector.
      Example: [{"id": "doc1", "text": "...", "embedding": [0.1, 0.2, ...]}]
      Returns: {status, upserted_count}.
    - delete: Delete records by primary key. Requires primary_keys.
      primary_keys: List of primary key values to delete.
      Returns: {status, deleted_count}.
    - scan: Scan index contents. Optional num_results (default 100).
      Returns: {columns, data, num_results}.
    - sync: Trigger re-sync for TRIGGERED DELTA_SYNC indexes.
      Returns: {index_name, status: "sync_triggered"}.

    For DIRECT_ACCESS indexes, use upsert/delete to manage data.
    For DELTA_SYNC indexes, use sync to trigger refresh from source table."""
    return _manage_vs_data(
        action=action,
        index_name=index_name,
        inputs_json=inputs_json,
        primary_keys=primary_keys,
        num_results=num_results,
    )
