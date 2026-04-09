"""Serving workflows - High-level model serving operations.

This module contains the business logic for serving endpoint operations,
used by both the MCP server and CLI.

Tools:
- manage_serving_endpoint: get, list, query
"""

from typing import Any, Dict, List, Optional

from .endpoints import (
    get_serving_endpoint_status as _get_serving_endpoint_status,
    query_serving_endpoint as _query_serving_endpoint,
    list_serving_endpoints as _list_serving_endpoints,
)


def manage_serving_endpoint(
    action: str,
    # For get/query:
    name: Optional[str] = None,
    # For query (use one input format):
    messages: Optional[List[Dict[str, str]]] = None,
    inputs: Optional[Dict[str, Any]] = None,
    dataframe_records: Optional[List[Dict[str, Any]]] = None,
    # For query options:
    max_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
    # For list:
    limit: int = 50,
) -> Dict[str, Any]:
    """Manage Model Serving endpoints: get status, list, query.

    Actions:
    - get: Get endpoint status. Requires name.
      Returns: {name, state (READY/NOT_READY/NOT_FOUND), config_update, served_entities, error}.
    - list: List all endpoints. Optional limit (default 50).
      Returns: {endpoints: [{name, state, creation_timestamp, creator, served_entities_count}, ...]}.
    - query: Query an endpoint. Requires name + one input format.
      Input formats (use one):
      - messages: Chat/agent endpoints. Format: [{"role": "user", "content": "..."}]
      - inputs: Custom pyfunc models (dict matching model signature)
      - dataframe_records: ML models. Format: [{"feature1": 1.0, ...}]
      max_tokens, temperature: Optional for chat endpoints.
      Returns: {choices: [...]} for chat or {predictions: [...]} for ML.

    Args:
        action: The action to perform (get, list, query).
        name: Endpoint name (required for get/query).
        messages: Chat messages for chat/agent endpoints.
        inputs: Input dict for custom pyfunc models.
        dataframe_records: Input records for ML models.
        max_tokens: Max tokens for chat endpoints.
        temperature: Temperature for chat endpoints.
        limit: Max endpoints to return for list action.

    Returns:
        Dict with endpoint information or error.
    """
    act = action.lower()

    if act == "get":
        if not name:
            return {"error": "get requires: name"}
        return _get_serving_endpoint_status(name=name)

    elif act == "list":
        endpoints = _list_serving_endpoints(limit=limit)
        return {"endpoints": endpoints}

    elif act == "query":
        if not name:
            return {"error": "query requires: name"}
        if not any([messages, inputs, dataframe_records]):
            return {"error": "query requires one of: messages, inputs, dataframe_records"}
        return _query_serving_endpoint(
            name=name,
            messages=messages,
            inputs=inputs,
            dataframe_records=dataframe_records,
            max_tokens=max_tokens,
            temperature=temperature,
        )

    else:
        return {"error": f"Invalid action '{action}'. Valid actions: get, list, query"}
