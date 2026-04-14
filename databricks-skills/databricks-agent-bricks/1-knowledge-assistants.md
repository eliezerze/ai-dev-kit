# Knowledge Assistants (KA)

Knowledge Assistants are document-based Q&A systems using RAG (Retrieval-Augmented Generation).

## How It Works

1. **Indexes** documents from a Volume (PDFs, text files)
2. **Retrieves** relevant chunks when a question is asked
3. **Generates** an answer using the retrieved context

## When to Use

- Collection of documents (policies, manuals, guides, reports)
- Users need to find specific information without reading entire documents
- Conversational interface to documentation

## Creating a Knowledge Assistant

### Step 1: Find Your Volume

```bash
# List volumes in the schema
databricks volumes list catalog schema --output json

# Browse volume contents
databricks experimental aitools tools query --warehouse WH "LIST '/Volumes/catalog/schema/volume/'"
```

### Step 2: Create the KA

```bash
databricks knowledge-assistants create-knowledge-assistant \
  "HR Policy Assistant" \
  "Answers questions about HR policies and procedures"
```

Response:
```json
{
  "id": "dab408a2-f8f4-439e-b65d-cc3cc2c45bbd",
  "name": "knowledge-assistants/dab408a2-f8f4-439e-b65d-cc3cc2c45bbd",
  "endpoint_name": "ka-dab408a2-endpoint",
  "state": "CREATING"
}
```

### Step 3: Add Knowledge Source

```bash
databricks knowledge-assistants create-knowledge-source \
  "knowledge-assistants/{ka_id}" \
  --json '{
    "display_name": "HR Documents",
    "description": "HR policy PDFs",
    "source_type": "files",
    "files": {"path": "/Volumes/my_catalog/my_schema/hr_docs/"}
  }'
```

**Source types:**

| Type | Config | Use Case |
|------|--------|----------|
| `files` | `files.path` | PDFs/text in a Volume |
| `index` | `index.index_name`, `index.text_col`, `index.doc_uri_col` | Existing Vector Search index |

### Step 4: Sync and Wait

```bash
# Trigger indexing
databricks knowledge-assistants sync-knowledge-sources "knowledge-assistants/{ka_id}"

# Check status
databricks knowledge-assistants get-knowledge-assistant "knowledge-assistants/{ka_id}"
```

| State | Meaning | Duration |
|-------|---------|----------|
| `CREATING` | Provisioning endpoint | 2-5 minutes |
| `ONLINE` | Ready to use | - |
| `OFFLINE` | Not running | - |

## Managing Knowledge Assistants

```bash
# List all KAs
databricks knowledge-assistants list-knowledge-assistants

# Get details
databricks knowledge-assistants get-knowledge-assistant "knowledge-assistants/{ka_id}"

# List knowledge sources
databricks knowledge-assistants list-knowledge-sources "knowledge-assistants/{ka_id}"

# Update KA
databricks knowledge-assistants update-knowledge-assistant "knowledge-assistants/{ka_id}" "*" "New Name" "New Description"

# Delete KA
databricks knowledge-assistants delete-knowledge-assistant "knowledge-assistants/{ka_id}"
```

## Updating Content

To update indexed documents:

1. Add/remove/modify files in the volume
2. Re-sync:
   ```bash
   databricks knowledge-assistants sync-knowledge-sources "knowledge-assistants/{ka_id}"
   ```

## Using KA in Supervisor Agents

KAs can be added to Supervisor Agents using their tile ID:

```bash
# Get KA tile ID
databricks knowledge-assistants list-knowledge-assistants --output json | jq '.[].id'

# Use in Supervisor Agent
python scripts/mas_manager.py create_mas "Support MAS" '{
    "agents": [
        {
            "name": "policy_agent",
            "ka_tile_id": "dab408a2-f8f4-439e-b65d-cc3cc2c45bbd",
            "description": "Answers HR policy questions from documents"
        }
    ]
}'
```

The endpoint name follows pattern: `ka-{tile_id}-endpoint`

## Troubleshooting

### KA stays in CREATING state
- Wait up to 10 minutes
- Check workspace capacity and quotas
- Verify the volume path is accessible

### Documents not indexed
- Ensure files are supported format (PDF, TXT, MD)
- Check file permissions in the volume
- Verify volume path is correct (trailing slash matters)

### Poor answer quality
- Add instructions to guide the AI's behavior
- Ensure documents are well-structured
- Consider breaking large documents into smaller files
