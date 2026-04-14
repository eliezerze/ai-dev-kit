---
name: databricks-genie
description: "Create and query Databricks Genie Spaces for natural language SQL exploration. Use when building Genie Spaces, exporting and importing Genie Spaces, migrating Genie Spaces between workspaces or environments, or asking questions via the Genie Conversation API."
---

# Databricks Genie

Create, manage, and query Genie Spaces - natural language interfaces for SQL-based data exploration.

## Overview

Genie Spaces allow users to ask natural language questions about structured data in Unity Catalog. The system translates questions into SQL queries, executes them on a SQL warehouse, and presents results conversationally.

## CLI Commands

### Space Management

```bash
# List all Genie Spaces
databricks genie list-spaces

# Create a Genie Space
databricks genie create-space --json '{
  "warehouse_id": "WAREHOUSE_ID",
  "title": "Sales Analytics",
  "description": "Explore sales data",
  "parent_path": "/Workspace/Users/you@company.com/genie_spaces",
  "serialized_space": "{\"version\": 2, \"data_sources\": {\"tables\": [{\"identifier\": \"catalog.schema.table\"}]}}"
}'

# Get space details (with full config)
databricks genie get-space SPACE_ID --include-serialized-space

# Update a Genie Space
databricks genie update-space SPACE_ID --json '{
  "title": "Updated Name",
  "description": "Updated description"
}'

# Delete a Genie Space
databricks genie trash-space SPACE_ID
```

### Export & Import

```bash
# Export space configuration
databricks genie export-space SPACE_ID > exported.json

# Import space from exported config
databricks genie import-space --json @exported.json
```

### Table Inspection

```bash
# Inspect table schemas before creating a space
databricks experimental aitools tools discover-schema catalog.schema.table1 catalog.schema.table2
```

## serialized_space Format

The `serialized_space` field is a JSON string containing the full space configuration.

### Structure

```json
{
  "version": 2,
  "config": {
    "sample_questions": [...]
  },
  "data_sources": {
    "tables": [{"identifier": "catalog.schema.table"}]
  },
  "instructions": {
    "example_question_sqls": [...],
    "text_instructions": [...]
  }
}
```

### Field Format Requirements

**IMPORTANT:** All items in `sample_questions`, `example_question_sqls`, and `text_instructions` require a unique `id` field.

| Field | Format |
|-------|--------|
| `config.sample_questions[]` | `{"id": "32hexchars", "question": ["..."]}` |
| `instructions.example_question_sqls[]` | `{"id": "32hexchars", "question": ["..."], "sql": ["..."]}` |
| `instructions.text_instructions[]` | `{"id": "32hexchars", "content": ["..."]}` |

- **ID format:** 32-character lowercase hex UUID without hyphens. Generate with `uuid.uuid4().hex` in Python.
- **Text fields are arrays:** `question`, `sql`, and `content` are arrays of strings, not plain strings.

### Example

```json
{
  "version": 2,
  "config": {
    "sample_questions": [
      {"id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4", "question": ["What were total sales last month?"]}
    ]
  },
  "data_sources": {
    "tables": [{"identifier": "catalog.schema.orders"}]
  },
  "instructions": {
    "example_question_sqls": [
      {
        "id": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5",
        "question": ["Show top customers"],
        "sql": ["SELECT customer_name, SUM(amount) AS total ", "FROM catalog.schema.orders ", "GROUP BY 1 ORDER BY 2 DESC"]
      }
    ]
  }
}
```

## Migration Workflow

### Clone (Same Workspace)

```bash
databricks genie export-space SOURCE_ID > space.json
databricks genie import-space --json @space.json
```

### Cross-Workspace with Catalog Remapping

When migrating between environments (dev → prod), catalog names often differ. Remap them:

```bash
# 1. Export from source workspace
DATABRICKS_CONFIG_PROFILE=source databricks genie export-space SPACE_ID > exported.json

# 2. Remap catalog name
sed -i '' 's/source_catalog/target_catalog/g' exported.json

# 3. Import to target workspace
DATABRICKS_CONFIG_PROFILE=target databricks genie import-space --json @exported.json
```

## Conversation API

Use `scripts/conversation.py` to ask questions programmatically:

```bash
# Ask a question
python scripts/conversation.py ask SPACE_ID "What were total sales last month?"

# Follow-up in same conversation
python scripts/conversation.py ask SPACE_ID "Break down by region" --conversation-id CONV_ID

# With timeout
python scripts/conversation.py ask SPACE_ID "Complex query" --timeout 120
```

See [conversation.md](conversation.md) for full details.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `sample_question.id must be provided` | Add 32-char hex UUID `id` to each sample question |
| `Expected an array for question` | Use `"question": ["text"]` not `"question": "text"` |
| No warehouse available | Create a SQL warehouse or provide `warehouse_id` |
| Empty `serialized_space` on export | Requires CAN EDIT permission on the space |
| Tables not found after migration | Remap catalog name in `serialized_space` before import |

## Related Skills

- **[databricks-synthetic-data-gen](../databricks-synthetic-data-gen/SKILL.md)** - Generate data for Genie tables
- **[databricks-spark-declarative-pipelines](../databricks-spark-declarative-pipelines/SKILL.md)** - Build bronze/silver/gold tables
