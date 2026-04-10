"""File tools - Upload and delete files and folders in Databricks workspace.

Consolidated into 1 tool:
- manage_workspace_files: upload, delete

This module is a thin wrapper around databricks_tools_core.file.file_api.
All business logic lives in the workflows module.
"""

from typing import Any, Dict, Optional

from databricks_tools_core.file.file_api import manage_workspace_files as _manage_workspace_files

from ..server import mcp


# CLI_MAPPING for skill transformation
CLI_MAPPING = {
    "manage_workspace_files": {
        "upload": "aidevkit workspace-files upload",
        "delete": "aidevkit workspace-files delete",
    },
}


@mcp.tool(timeout=120)
def manage_workspace_files(
    action: str,
    workspace_path: str,
    # For upload:
    local_path: Optional[str] = None,
    max_workers: int = 10,
    overwrite: bool = True,
    # For delete:
    recursive: bool = False,
) -> Dict[str, Any]:
    """Manage workspace files: upload, delete.

    Actions:
    - upload: Upload files/folders to workspace. Requires local_path, workspace_path.
      Supports files, folders, globs, tilde expansion.
      max_workers: Parallel upload threads (default 10). overwrite: Replace existing (default True).
      Returns: {local_folder, remote_folder, total_files, successful, failed, success, failed_uploads}.
    - delete: Delete file/folder from workspace. Requires workspace_path.
      recursive=True for non-empty folders. Has safety checks for protected paths.
      Returns: {workspace_path, success, error}.

    workspace_path format: /Workspace/Users/user@example.com/path/to/files"""
    return _manage_workspace_files(
        action=action,
        workspace_path=workspace_path,
        local_path=local_path,
        max_workers=max_workers,
        overwrite=overwrite,
        recursive=recursive,
    )
