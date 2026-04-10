"""PDF workflows - High-level PDF generation operations.

This module contains the business logic for PDF operations, used by both
the MCP server and CLI.

Tools:
- generate_and_upload_pdf: Convert HTML to PDF and upload to volume
"""

from typing import Any, Dict, Optional

from .generator import generate_and_upload_pdf as _generate_and_upload_pdf


def generate_and_upload_pdf(
    html_content: str,
    filename: str,
    catalog: str,
    schema: str,
    volume: str = "raw_data",
    folder: Optional[str] = None,
) -> Dict[str, Any]:
    """Convert complete HTML (with styles) to PDF and upload to Unity Catalog volume.

    Args:
        html_content: Complete HTML content to convert to PDF.
        filename: Name for the PDF file.
        catalog: Unity Catalog catalog name.
        schema: Schema name within the catalog.
        volume: Volume name (default: "raw_data").
        folder: Optional subfolder within the volume.

    Returns:
        Dict with success status, volume_path, and any error.
    """
    result = _generate_and_upload_pdf(
        html_content=html_content,
        filename=filename,
        catalog=catalog,
        schema=schema,
        volume=volume,
        folder=folder,
    )

    return {
        "success": result.success,
        "volume_path": result.volume_path,
        "error": result.error,
    }
