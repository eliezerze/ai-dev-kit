# Rapid Pipeline Iteration with CLI

Use CLI commands to create, run, and iterate on **SDP pipelines**. This is the fastest approach for prototyping without managing bundle files.

**IMPORTANT: Default to serverless pipelines.** Only use classic clusters if user explicitly requires R language, Spark RDD APIs, or JAR libraries.

### Step 1: Write Pipeline Files Locally

Create `.sql` or `.py` files in a local folder. For syntax examples, see:
- [sql/1-syntax-basics.md](sql/1-syntax-basics.md) for SQL syntax
- [python/1-syntax-basics.md](python/1-syntax-basics.md) for Python syntax

### Step 2: Upload to Databricks Workspace

```bash
# Upload local folder to workspace
databricks workspace import-dir ./my_pipeline /Workspace/Users/user@example.com/my_pipeline
```

### Step 3: Create Pipeline

```bash
# Create pipeline with JSON config
databricks pipelines create --json '{
  "name": "my_orders_pipeline",
  "catalog": "my_catalog",
  "schema": "my_schema",
  "serverless": true,
  "libraries": [
    {"notebook": {"path": "/Workspace/Users/user@example.com/my_pipeline/bronze/ingest_orders.sql"}},
    {"notebook": {"path": "/Workspace/Users/user@example.com/my_pipeline/silver/clean_orders.sql"}},
    {"notebook": {"path": "/Workspace/Users/user@example.com/my_pipeline/gold/daily_summary.sql"}}
  ],
  "development": true
}'
```

Save the returned `pipeline_id` for subsequent operations.

### Step 4: Run Pipeline

```bash
# Start a full refresh run
databricks pipelines start-update --pipeline-id <pipeline_id> --full-refresh

# Check run status
databricks pipelines get --pipeline-id <pipeline_id>
```

### Step 5: Validate Results

**On Success** - Verify tables were created with correct data:

```bash
# Check table schemas and row counts
databricks sql execute --warehouse-id WAREHOUSE_ID --query "
DESCRIBE TABLE EXTENDED my_catalog.my_schema.bronze_orders;
"

databricks sql execute --warehouse-id WAREHOUSE_ID --query "
SELECT COUNT(*) as row_count FROM my_catalog.my_schema.bronze_orders;
"
```

Or use Python for detailed stats:
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Get table info
table = w.tables.get("my_catalog.my_schema.bronze_orders")
print(f"Columns: {len(table.columns)}")
print(f"Created: {table.created_at}")
```

**On Failure** - Get pipeline events and errors:

```bash
# Get pipeline details with recent events
databricks pipelines get --pipeline-id <pipeline_id>

# Get specific run events
databricks pipelines list-pipeline-events --pipeline-id <pipeline_id>
```

### Step 6: Iterate Until Working

1. Review errors from pipeline status or events
2. Fix issues in local files
3. Re-upload: `databricks workspace import-dir ./my_pipeline /Workspace/Users/user@example.com/my_pipeline --overwrite`
4. Update and run: `databricks pipelines update --pipeline-id <pipeline_id> --json '...'` then `databricks pipelines start-update --pipeline-id <pipeline_id>`
5. Repeat until pipeline completes successfully

---

## Quick Reference: CLI Commands

### Pipeline Lifecycle

| Command | Description |
|---------|-------------|
| `databricks pipelines create --json '{...}'` | Create new pipeline |
| `databricks pipelines get --pipeline-id ID` | Get pipeline details and status |
| `databricks pipelines update --pipeline-id ID --json '{...}'` | Update pipeline config |
| `databricks pipelines delete --pipeline-id ID` | Delete a pipeline |
| `databricks pipelines list` | List all pipelines |

### Run Management

| Command | Description |
|---------|-------------|
| `databricks pipelines start-update --pipeline-id ID` | Start pipeline update |
| `databricks pipelines start-update --pipeline-id ID --full-refresh` | Start with full refresh |
| `databricks pipelines stop --pipeline-id ID` | Stop running pipeline |
| `databricks pipelines list-pipeline-events --pipeline-id ID` | Get events/logs |
| `databricks pipelines list-updates --pipeline-id ID` | List recent runs |

### Supporting Commands

| Command | Description |
|---------|-------------|
| `databricks workspace import-dir` | Upload files/folders to workspace |
| `databricks workspace ls` | List workspace files |
| `databricks sql execute` | Run ad-hoc SQL to inspect data |

---

## Python SDK Alternative

For more programmatic control, use the Databricks SDK:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create pipeline
pipeline = w.pipelines.create(
    name="my_orders_pipeline",
    catalog="my_catalog",
    schema="my_schema",
    serverless=True,
    libraries=[
        {"notebook": {"path": "/Workspace/Users/user@example.com/my_pipeline/bronze/ingest_orders.sql"}}
    ],
    development=True
)
print(f"Created pipeline: {pipeline.pipeline_id}")

# Start update
update = w.pipelines.start_update(
    pipeline_id=pipeline.pipeline_id,
    full_refresh=True
)

# Poll for completion
import time
while True:
    status = w.pipelines.get(pipeline_id=pipeline.pipeline_id)
    if status.state in ["IDLE", "FAILED"]:
        print(f"Pipeline state: {status.state}")
        break
    time.sleep(10)
```

---
