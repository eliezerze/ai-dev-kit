"""File workflows - High-level workspace file operations.

This module contains the business logic for workspace file operations,
used by both the MCP server and CLI.

Tools:
- manage_workspace_files: upload, delete
"""

from typing import Any, Dict, Optional

from .workspace import (
    delete_from_workspace as _delete_from_workspace,
    upload_to_workspace as _upload_to_workspace,
)


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

    Args:
        action: The action to perform (upload, delete).
        workspace_path: Workspace path (format: /Workspace/Users/user@example.com/path/to/files).
        local_path: Local path for upload (supports globs and tilde expansion).
        max_workers: Parallel upload threads for upload action.
        overwrite: Replace existing files on upload.
        recursive: Allow recursive deletion for non-empty folders.

    Returns:
        Dict with operation result or error.
    """
    act = action.lower()

    if act == "upload":
        if not local_path:
            return {"error": "upload requires: local_path"}
        result = _upload_to_workspace(
            local_path=local_path,
            workspace_path=workspace_path,
            max_workers=max_workers,
            overwrite=overwrite,
        )
        return {
            "local_folder": result.local_folder,
            "remote_folder": result.remote_folder,
            "total_files": result.total_files,
            "successful": result.successful,
            "failed": result.failed,
            "success": result.success,
            "failed_uploads": [
                {"local_path": r.local_path, "error": r.error} for r in result.get_failed_uploads()
            ]
            if result.failed > 0
            else [],
        }

    elif act == "delete":
        result = _delete_from_workspace(
            workspace_path=workspace_path,
            recursive=recursive,
        )
        return {
            "workspace_path": result.workspace_path,
            "success": result.success,
            "error": result.error,
        }

    else:
        return {"error": f"Invalid action '{action}'. Valid actions: upload, delete"}
