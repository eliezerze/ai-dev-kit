"""Volume file tools - Manage files in Unity Catalog Volumes.

Consolidated into 1 tool:
- manage_volume_files: list, upload, download, delete, mkdir, get_info

This module is a thin wrapper around databricks_tools_core.unity_catalog.unity_catalog_api.
All business logic lives in the workflows module.
"""

from typing import Dict, Any, Optional

from databricks_tools_core.unity_catalog.unity_catalog_api import manage_volume_files as _manage_volume_files

from ..server import mcp


# CLI_MAPPING for skill transformation
CLI_MAPPING = {
    "manage_volume_files": {
        "list": "aidevkit volume-files list",
        "upload": "aidevkit volume-files upload",
        "download": "aidevkit volume-files download",
        "delete": "aidevkit volume-files delete",
        "mkdir": "aidevkit volume-files mkdir",
        "get_info": "aidevkit volume-files info",
    },
}


@mcp.tool(timeout=300)
def manage_volume_files(
    action: str,
    volume_path: str,
    # For upload:
    local_path: Optional[str] = None,
    # For download:
    local_destination: Optional[str] = None,
    # For list:
    max_results: int = 500,
    # For delete:
    recursive: bool = False,
    # Common:
    max_workers: int = 4,
    overwrite: bool = True,
) -> Dict[str, Any]:
    """Manage Unity Catalog Volume files: list, upload, download, delete, mkdir, get_info.

    Actions:
    - list: List files in volume path. Returns: {files: [{name, path, is_directory, file_size}], truncated}.
      max_results: Limit results (default 500, max 1000).
    - upload: Upload local file/folder/glob to volume. Auto-creates directories.
      Requires volume_path, local_path. Returns: {total_files, successful, failed}.
    - download: Download file from volume to local path.
      Requires volume_path, local_destination. Returns: {success, error}.
    - delete: Delete file/directory from volume.
      recursive=True for non-empty directories. Returns: {files_deleted, directories_deleted}.
    - mkdir: Create directory in volume (like mkdir -p). Idempotent.
      Returns: {success}.
    - get_info: Get file/directory metadata.
      Returns: {name, path, is_directory, file_size, last_modified}.

    volume_path format: /Volumes/catalog/schema/volume/path/to/file_or_dir
    Supports tilde expansion (~) and glob patterns for local_path."""
    return _manage_volume_files(
        action=action,
        volume_path=volume_path,
        local_path=local_path,
        local_destination=local_destination,
        max_results=max_results,
        recursive=recursive,
        max_workers=max_workers,
        overwrite=overwrite,
    )
