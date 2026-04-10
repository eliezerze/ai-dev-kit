#!/usr/bin/env python3
"""Skill file transformer for CLI ↔ MCP mode.

This module transforms skill files (SKILL.md) to replace CLI commands with MCP tool
references when installing for MCP mode, or vice versa.

Usage:
    python -m databricks_tools_core.skill_transformer --mode mcp <skills_dir>
    python -m databricks_tools_core.skill_transformer --mode cli <skills_dir>  # (no-op, skills are CLI by default)
"""

import argparse
import re
import sys
from pathlib import Path
from typing import Dict


def get_cli_to_mcp_mapping() -> Dict[str, str]:
    """Get the CLI command to MCP tool mapping.

    Imports lazily to avoid circular dependencies.
    """
    try:
        from databricks_mcp_server.cli_mapping import CLI_TO_MCP
        return CLI_TO_MCP
    except ImportError:
        print("Warning: databricks_mcp_server not installed, using empty mapping")
        return {}


def transform_for_mcp(content: str, cli_to_mcp: Dict[str, str] = None) -> str:
    """Replace CLI commands with MCP tool references in skill content.

    Args:
        content: The skill file content (markdown).
        cli_to_mcp: Mapping of CLI commands to MCP tool calls.

    Returns:
        Transformed content with MCP tool references.
    """
    if cli_to_mcp is None:
        cli_to_mcp = get_cli_to_mcp_mapping()

    # Sort by length (longest first) to avoid partial matches
    sorted_commands = sorted(cli_to_mcp.keys(), key=len, reverse=True)

    for cli_cmd in sorted_commands:
        mcp_tool = cli_to_mcp[cli_cmd]

        # Pattern 1: CLI command in code blocks (```bash ... ```)
        # Replace `aidevkit xxx yyy` with `MCP tool: xxx_yyy(...)`
        pattern = rf'`{re.escape(cli_cmd)}([^`]*)`'
        replacement = rf'`MCP tool: {mcp_tool}\1`'
        content = re.sub(pattern, replacement, content)

        # Pattern 2: CLI command at start of line in code block (without backticks)
        # Match "aidevkit xxx yyy" at start of line (with optional leading whitespace)
        pattern = rf'^(\s*){re.escape(cli_cmd)}(\s.*)?$'
        replacement = rf'\1# MCP tool: {mcp_tool}\2'
        content = re.sub(pattern, replacement, content, flags=re.MULTILINE)

    return content


def transform_skill_file(skill_path: Path, mode: str, dry_run: bool = False) -> bool:
    """Transform a single skill file.

    Args:
        skill_path: Path to the SKILL.md file.
        mode: "mcp" to transform CLI -> MCP, "cli" is a no-op (skills are CLI by default).
        dry_run: If True, print what would be changed without modifying files.

    Returns:
        True if the file was modified (or would be modified in dry_run mode).
    """
    if mode == "cli":
        # Skills are written in CLI syntax by default, nothing to do
        return False

    content = skill_path.read_text(encoding="utf-8")
    transformed = transform_for_mcp(content)

    if content == transformed:
        return False

    if dry_run:
        print(f"Would transform: {skill_path}")
        # Show diff preview
        import difflib
        diff = difflib.unified_diff(
            content.splitlines(keepends=True),
            transformed.splitlines(keepends=True),
            fromfile=str(skill_path),
            tofile=str(skill_path) + " (transformed)",
            n=2,
        )
        print("".join(list(diff)[:50]))  # Show first 50 lines of diff
    else:
        skill_path.write_text(transformed, encoding="utf-8")
        print(f"Transformed: {skill_path}")

    return True


def transform_skills_directory(skills_dir: Path, mode: str, dry_run: bool = False) -> int:
    """Transform all skill files in a directory.

    Args:
        skills_dir: Path to the skills directory (e.g., databricks-skills/).
        mode: "mcp" or "cli".
        dry_run: If True, print what would be changed without modifying files.

    Returns:
        Number of files transformed.
    """
    if not skills_dir.is_dir():
        print(f"Error: {skills_dir} is not a directory")
        return 0

    skill_files = list(skills_dir.glob("*/SKILL.md"))
    if not skill_files:
        print(f"No SKILL.md files found in {skills_dir}")
        return 0

    print(f"Found {len(skill_files)} skill files in {skills_dir}")

    transformed_count = 0
    for skill_path in skill_files:
        if transform_skill_file(skill_path, mode, dry_run):
            transformed_count += 1

    return transformed_count


def main():
    parser = argparse.ArgumentParser(
        description="Transform skill files for CLI or MCP mode"
    )
    parser.add_argument(
        "--mode",
        choices=["mcp", "cli"],
        required=True,
        help="Target mode: 'mcp' transforms CLI commands to MCP tool references, 'cli' is a no-op",
    )
    parser.add_argument(
        "skills_dir",
        type=Path,
        help="Path to the skills directory (e.g., databricks-skills/)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be changed without modifying files",
    )

    args = parser.parse_args()

    transformed = transform_skills_directory(args.skills_dir, args.mode, args.dry_run)

    if args.dry_run:
        print(f"\nDry run: would transform {transformed} files")
    else:
        print(f"\nTransformed {transformed} files")

    return 0 if transformed >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
