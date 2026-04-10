---
name: databricks-execution-compute
description: >-
  Execute code and manage compute on Databricks. Use this skill when the user
  mentions: "run code", "execute", "run on databricks", "serverless", "no
  cluster", "run python", "run scala", "run sql", "run R", "run file", "push
  and run", "notebook run", "batch script", "model training", "run script on
  cluster", "create cluster", "new cluster", "resize cluster", "modify cluster",
  "delete cluster", "terminate cluster", "create warehouse", "new warehouse",
  "resize warehouse", "delete warehouse", "node types", "runtime versions",
  "DBR versions", "spin up compute", "provision cluster".
---

# Databricks Execution & Compute

Run code on Databricks. Three execution modes—choose based on workload.

## Execution Mode Decision Matrix

| Aspect | [Databricks Connect](references/1-databricks-connect.md) ⭐ | [Serverless Job](references/2-serverless-job.md) | [Interactive Cluster](references/3-interactive-cluster.md) |
|--------|-------------------|----------------|---------------------|
| **Use for** | Spark code (ETL, data gen) | Heavy processing (ML) | State across tool calls, Scala/R |
| **Startup** | Instant | ~25-50s cold start | ~5min if stopped |
| **State** | Within Python process | None | Via context_id |
| **Languages** | Python (PySpark) | Python, SQL | Python, Scala, SQL, R |
| **Dependencies** | `withDependencies()` | CLI with environments spec | Install on cluster |

### Decision Flow

```
Spark-based code? → Databricks Connect (fastest)
  └─ Python 3.12 missing? → Install it + databricks-connect
  └─ Install fails? → Ask user (don't auto-switch modes)

Heavy/long-running (ML)? → Serverless Job (independent)
Need state across calls? → Interactive Cluster (list and ask which one to use)
Scala/R? → Interactive Cluster (list and ask which one to use)
```


## How to Run Code

**Read the reference file for your chosen mode before proceeding.**

### Databricks Connect (no MCP tool, run locally) → [reference](references/1-databricks-connect.md)

```bash
python my_spark_script.py
```

### Serverless Job → [reference](references/2-serverless-job.md)

```bash
aidevkit compute execute --file ./script.py
```

### Interactive Cluster → [reference](references/3-interactive-cluster.md)

```bash
# Check for running clusters first
aidevkit compute list --resource clusters
# Ask the customer which one to use

# Run code on specific cluster
aidevkit compute execute --code "print(spark.version)" --compute cluster --cluster-id abc123
```

---

## CLI Quick Reference (aidevkit CLI)

### Code Execution
```bash
# Execute Python code on serverless
aidevkit compute execute --code "print('hello')"

# Execute SQL
aidevkit compute execute --code "SELECT * FROM catalog.schema.table LIMIT 10" --language sql

# Execute from file
aidevkit compute execute --file ./script.py

# Execute on specific cluster
aidevkit compute execute --code "print(spark.version)" --compute cluster --cluster-id abc123
```

### Cluster Management
```bash
# List clusters
aidevkit compute list --resource clusters

# Create cluster
aidevkit compute cluster create --name my-cluster --workers 2

# Create with autoscale
aidevkit compute cluster create --name my-cluster --autoscale-min 1 --autoscale-max 4

# Get cluster status
aidevkit compute cluster get --cluster-id abc123

# Start/terminate/delete cluster
aidevkit compute cluster start --cluster-id abc123
aidevkit compute cluster terminate --cluster-id abc123
aidevkit compute cluster delete --cluster-id abc123

# Modify cluster
aidevkit compute cluster modify --cluster-id abc123 --workers 4
```

### SQL Warehouse Management
```bash
# Create warehouse
aidevkit compute warehouse create --name my-warehouse --size SMALL

# Modify warehouse
aidevkit compute warehouse modify --warehouse-id wh123 --size MEDIUM

# Delete warehouse
aidevkit compute warehouse delete --warehouse-id wh123
```

---

## Related Skills

- **[databricks-synthetic-data-gen](../databricks-synthetic-data-gen/SKILL.md)** — Data generation using Spark + Faker
- **[databricks-jobs](../databricks-jobs/SKILL.md)** — Production job orchestration
- **[databricks-dbsql](../databricks-dbsql/SKILL.md)** — SQL warehouse and AI functions
