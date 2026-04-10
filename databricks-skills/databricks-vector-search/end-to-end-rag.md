# End-to-End RAG with Vector Search

Build a complete Retrieval-Augmented Generation pipeline: prepare documents, create a vector index, query it, and wire it into an agent.

## CLI Commands Used

| Command | Step |
|---------|------|
| `aidevkit sql execute` | Create source table, insert documents |
| `aidevkit vector-search endpoint create-or-update` | Create compute endpoint |
| `aidevkit vector-search index create-or-update` | Create Delta Sync index with managed embeddings |
| `aidevkit vector-search data sync` | Trigger index sync |
| `aidevkit vector-search index get` | Check index status |
| `aidevkit vector-search query` | Test similarity search |

---

## Step 1: Prepare Source Table

The source Delta table needs a primary key column and a text column to embed.

```sql
CREATE TABLE IF NOT EXISTS catalog.schema.knowledge_base (
    doc_id STRING,
    title STRING,
    content STRING,
    category STRING,
    updated_at TIMESTAMP DEFAULT current_timestamp()
);

INSERT INTO catalog.schema.knowledge_base VALUES
('doc-001', 'Getting Started', 'Databricks is a unified analytics platform...', 'overview', current_timestamp()),
('doc-002', 'Unity Catalog', 'Unity Catalog provides centralized governance...', 'governance', current_timestamp()),
('doc-003', 'Delta Lake', 'Delta Lake is an open-source storage layer...', 'storage', current_timestamp());
```

Or via CLI:

```bash
aidevkit sql execute --sql "CREATE TABLE IF NOT EXISTS catalog.schema.knowledge_base (
    doc_id STRING,
    title STRING,
    content STRING,
    category STRING,
    updated_at TIMESTAMP DEFAULT current_timestamp()
)"
```

## Step 2: Create Vector Search Endpoint

```bash
aidevkit vector-search endpoint create-or-update --name "my-rag-endpoint" --type STORAGE_OPTIMIZED
```

Endpoint creation is asynchronous. Check status:

```bash
aidevkit vector-search endpoint get --name "my-rag-endpoint"
# Wait for state: "ONLINE"
```

## Step 3: Create Delta Sync Index

```bash
aidevkit vector-search index create-or-update \
    --name "catalog.schema.knowledge_base_index" \
    --endpoint-name "my-rag-endpoint" \
    --primary-key "doc_id" \
    --index-type "DELTA_SYNC" \
    --delta-sync-spec '{
        "source_table": "catalog.schema.knowledge_base",
        "embedding_source_columns": [
            {
                "name": "content",
                "embedding_model_endpoint_name": "databricks-gte-large-en"
            }
        ],
        "pipeline_type": "TRIGGERED",
        "columns_to_sync": ["doc_id", "title", "content", "category"]
    }'
```

Key decisions:
- **`embedding_source_columns`**: Databricks computes embeddings automatically from the `content` column
- **`pipeline_type`**: `TRIGGERED` for manual sync (cheaper), `CONTINUOUS` for auto-sync on table changes
- **`columns_to_sync`**: Only sync columns you need in query results (reduces storage and improves performance)

## Step 4: Sync and Verify

```bash
# Trigger initial sync
aidevkit vector-search data sync --index-name "catalog.schema.knowledge_base_index"

# Check status
aidevkit vector-search index get --name "catalog.schema.knowledge_base_index"
# Wait for state: "ONLINE"
```

## Step 5: Query the Index

```bash
# Semantic search
aidevkit vector-search query \
    --index-name "catalog.schema.knowledge_base_index" \
    --columns "doc_id,title,content,category" \
    --query-text "How do I govern my data?" \
    --num-results 3
```

### With Filters

The filter syntax depends on the endpoint type used when creating the index.

```bash
# Storage-Optimized endpoint (used in this walkthrough): SQL-like filter syntax
aidevkit vector-search query \
    --index-name "catalog.schema.knowledge_base_index" \
    --columns "doc_id,title,content" \
    --query-text "How do I govern my data?" \
    --num-results 3 \
    --filters "category = 'governance'"

# Standard endpoint (if you created a Standard endpoint instead): JSON filters_json
aidevkit vector-search query \
    --index-name "catalog.schema.my_standard_index" \
    --columns "doc_id,title,content" \
    --query-text "How do I govern my data?" \
    --num-results 3 \
    --filters-json '{"category": "governance"}'
```

### Hybrid Search (Vector + Keyword)

```bash
aidevkit vector-search query \
    --index-name "catalog.schema.knowledge_base_index" \
    --columns "doc_id,title,content" \
    --query-text "Delta Lake ACID transactions" \
    --num-results 5 \
    --query-type "HYBRID"
```

---

## Step 6: Use in an Agent

### As a Tool in a ChatAgent

Use `VectorSearchRetrieverTool` to wire the index into an agent deployed on Model Serving:

```python
from databricks.agents import ChatAgent
from databricks.agents.tools import VectorSearchRetrieverTool
from databricks.sdk import WorkspaceClient

# Define the retriever tool
retriever_tool = VectorSearchRetrieverTool(
    index_name="catalog.schema.knowledge_base_index",
    columns=["doc_id", "title", "content"],
    num_results=3,
)

class RAGAgent(ChatAgent):
    def __init__(self):
        self.w = WorkspaceClient()

    def predict(self, messages, context=None):
        query = messages[-1].content

        results = self.w.vector_search_indexes.query_index(
            index_name="catalog.schema.knowledge_base_index",
            columns=["title", "content"],
            query_text=query,
            num_results=3,
        )

        context_docs = "\n\n".join(
            f"**{row[0]}**: {row[1]}"
            for row in results.result.data_array
        )

        response = self.w.serving_endpoints.query(
            name="databricks-meta-llama-3-3-70b-instruct",
            messages=[
                {"role": "system", "content": f"Answer using this context:\n{context_docs}"},
                {"role": "user", "content": query},
            ],
        )

        return {"content": response.choices[0].message.content}
```

---

## Updating the Index

### Add New Documents

```sql
INSERT INTO catalog.schema.knowledge_base VALUES
('doc-004', 'MLflow', 'MLflow is an open-source platform for ML lifecycle...', 'ml', current_timestamp());
```

Then sync:

```bash
aidevkit vector-search data sync --index-name "catalog.schema.knowledge_base_index"
```

### Delete Documents

```sql
DELETE FROM catalog.schema.knowledge_base WHERE doc_id = 'doc-001';
```

Then sync — the index automatically handles deletions via Delta change data feed.

---

## Common Issues

| Issue | Solution |
|-------|----------|
| **Index stuck in PROVISIONING** | Endpoint may still be creating. Check `aidevkit vector-search endpoint get` first |
| **Query returns no results** | Index may not be synced yet. Run `aidevkit vector-search data sync` and wait for ONLINE state |
| **"Column not found in index"** | Column must be in `columns_to_sync`. Recreate index with the column included |
| **Embeddings not computed** | Ensure `embedding_model_endpoint_name` is a valid serving endpoint |
| **Stale results after table update** | For TRIGGERED pipelines, you must call `aidevkit vector-search data sync` manually |
| **Filter not working** | Standard endpoints use dict-format filters (`--filters-json`), Storage-Optimized use SQL-like string filters (`--filters`) |
