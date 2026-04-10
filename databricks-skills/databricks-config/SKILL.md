---
name: databricks-config
description: "Manage Databricks workspace connections: check current workspace, switch profiles, list available workspaces, or authenticate to a new workspace. Use when the user mentions \"switch workspace\", \"which workspace\", \"current profile\", \"databrickscfg\", \"connect to workspace\", or \"databricks auth\"."
---

Use the `aidevkit workspace` CLI for all workspace operations.

## Steps

1. Map user intent to command:
   - status / which workspace / current → `aidevkit workspace status`
   - list / available workspaces → `aidevkit workspace list`
   - switch to X → `aidevkit workspace switch --profile <name>` (or `--host <url>`)
   - login / connect / authenticate → `aidevkit workspace login --host <url>`

2. Present the result. For `status`/`switch`/`login`: show host, profile, username. For `list`: formatted table with the active profile marked.

> **Note:** The switch is session-scoped. For permanent profile setup, use `databricks auth login -p <profile>` and update `~/.databrickscfg` with `cluster_id` or `serverless_compute_id = auto`.

---

## CLI Quick Reference (aidevkit CLI)

```bash
# Check current workspace status
aidevkit workspace status

# List all configured profiles
aidevkit workspace list

# Switch to a different profile
aidevkit workspace switch --profile prod

# Switch to a workspace by URL
aidevkit workspace switch --host https://adb-xxx.azuredatabricks.net

# Login to a workspace (opens browser for OAuth)
aidevkit workspace login --host https://adb-xxx.azuredatabricks.net

# Get current user info
aidevkit auth whoami
```
