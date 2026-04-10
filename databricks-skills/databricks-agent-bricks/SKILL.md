---
name: databricks-agent-bricks
description: "Create and manage Databricks Agent Bricks: Knowledge Assistants (KA) for document Q&A, Genie Spaces for SQL exploration, and Supervisor Agents (MAS) for multi-agent orchestration. Use when building conversational AI applications on Databricks."
---

# Agent Bricks

Create and manage Databricks Agent Bricks - pre-built AI components for building conversational applications.

## Overview

Agent Bricks are three types of pre-built AI tiles in Databricks:

| Brick | Purpose | Data Source |
|-------|---------|-------------|
| **Knowledge Assistant (KA)** | Document-based Q&A using RAG | PDF/text files in Volumes |
| **Genie Space** | Natural language to SQL | Unity Catalog tables |
| **Supervisor Agent (MAS)** | Multi-agent orchestration | Model serving endpoints |

## Prerequisites

Before creating Agent Bricks, ensure you have the required data:

### For Knowledge Assistants
- **Documents in a Volume**: PDF, text, or other files stored in a Unity Catalog volume
- Generate synthetic documents using the `databricks-unstructured-pdf-generation` skill if needed

### For Genie Spaces
- **See the `databricks-genie` skill** for comprehensive Genie Space guidance
- Tables in Unity Catalog with the data to explore
- Generate raw data using the `databricks-synthetic-data-gen` skill
- Create tables using the `databricks-spark-declarative-pipelines` skill

### For Supervisor Agents
- **Model Serving Endpoints**: Deployed agent endpoints (KA endpoints, custom agents, fine-tuned models)
- **Genie Spaces**: Existing Genie spaces can be used directly as agents for SQL-based queries
- Mix and match endpoint-based and Genie-based agents in the same Supervisor Agent

### For Unity Catalog Functions
- **Existing UC Function**: Function already registered in Unity Catalog
- Agent service principal has `EXECUTE` privilege on the function

### For External MCP Servers
- **Existing UC HTTP Connection**: Connection configured with `is_mcp_connection: 'true'`
- Agent service principal has `USE CONNECTION` privilege on the connection

## Typical Workflow

### 1. Generate Source Data

Before creating Agent Bricks, generate the required source data:

**For KA (document Q&A)**:
```
1. Use `databricks-unstructured-pdf-generation` skill to generate PDFs
2. PDFs are saved to a Volume with companion JSON files (question/guideline pairs)
```

**For Genie (SQL exploration)**:
```
1. Use `databricks-synthetic-data-gen` skill to create raw parquet data
2. Use `databricks-spark-declarative-pipelines` skill to create bronze/silver/gold tables
```

### 2. Create the Agent Brick

Use `aidevkit agent-bricks ka create-or-update` or `aidevkit agent-bricks mas create-or-update` with your data sources.

### 3. Wait for Provisioning

Newly created KA and MAS tiles need time to provision. The endpoint status will progress:
- `PROVISIONING` - Being created (can take 2-5 minutes)
- `ONLINE` - Ready to use
- `OFFLINE` - Not running

### 4. Add Examples (Automatic)

For KA, if `add_examples_from_volume=true`, examples are automatically extracted from JSON files in the volume and added once the endpoint is `ONLINE`.

## Best Practices

1. **Use meaningful names**: Names are sanitized automatically (spaces become underscores)
2. **Provide descriptions**: Helps users understand what the brick does
3. **Add instructions**: Guide the AI's behavior and tone
4. **Include sample questions**: Shows users how to interact with the brick
5. **Use the workflow**: Generate data first, then create the brick

## Example: Multi-Modal Supervisor Agent

```bash
# Create a supervisor agent with multiple agent types
aidevkit agent-bricks mas create-or-update \
    --name "Enterprise Support Supervisor" \
    --agents '[
        {"name":"knowledge_base","ka_tile_id":"f32c5f73-466b-...","description":"Answers questions about company policies"},
        {"name":"analytics_engine","genie_space_id":"01abc123...","description":"Runs SQL analytics on usage metrics"},
        {"name":"ml_classifier","endpoint_name":"custom-classification-endpoint","description":"Classifies support tickets"},
        {"name":"data_enrichment","uc_function_name":"support.utils.enrich_ticket_data","description":"Enriches ticket data"},
        {"name":"ticket_operations","connection_name":"ticket_system_mcp","description":"Manages support tickets"}
    ]' \
    --description "Comprehensive enterprise support agent" \
    --instructions "Route policy questions to knowledge_base, analytics to analytics_engine, classification to ml_classifier"
```

## Related Skills

- **[databricks-genie](../databricks-genie/SKILL.md)** - Comprehensive Genie Space creation, curation, and Conversation API guidance
- **[databricks-unstructured-pdf-generation](../databricks-unstructured-pdf-generation/SKILL.md)** - Generate synthetic PDFs to feed into Knowledge Assistants
- **[databricks-synthetic-data-gen](../databricks-synthetic-data-gen/SKILL.md)** - Create raw data for Genie Space tables
- **[databricks-spark-declarative-pipelines](../databricks-spark-declarative-pipelines/SKILL.md)** - Build bronze/silver/gold tables consumed by Genie Spaces
- **[databricks-model-serving](../databricks-model-serving/SKILL.md)** - Deploy custom agent endpoints used as MAS agents
- **[databricks-vector-search](../databricks-vector-search/SKILL.md)** - Build vector indexes for RAG applications paired with KAs

---

## CLI Quick Reference

### Knowledge Assistants (KA)

```bash
# Create or update a Knowledge Assistant
aidevkit agent-bricks ka create-or-update --name "My KA" \
    --volume-path "/Volumes/catalog/schema/volume/docs" \
    --description "Answers questions about company policies"

# Get KA details
aidevkit agent-bricks ka get --tile-id ka_tile_123

# Find KA by name
aidevkit agent-bricks ka find-by-name --name "My KA"

# Delete KA
aidevkit agent-bricks ka delete --tile-id ka_tile_123
```

### Supervisor Agents (MAS)

```bash
# Create or update a Supervisor Agent
aidevkit agent-bricks mas create-or-update --name "Support Agent" \
    --agents '[{"name":"kb","ka_tile_id":"ka123","description":"Policy questions"}]' \
    --description "Multi-agent support bot"

# Get MAS details
aidevkit agent-bricks mas get --tile-id mas_tile_456

# Find MAS by name
aidevkit agent-bricks mas find-by-name --name "Support Agent"

# Delete MAS
aidevkit agent-bricks mas delete --tile-id mas_tile_456
```

---

## See Also

- `1-knowledge-assistants.md` - Detailed KA patterns and examples
- `databricks-genie` skill - Detailed Genie patterns, curation, and examples
- `2-supervisor-agents.md` - Detailed MAS patterns and examples
