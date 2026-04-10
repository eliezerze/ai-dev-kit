---
name: databricks-unity-catalog
description: "Unity Catalog system tables and volumes. Use when querying system tables (audit, lineage, billing) or working with volume file operations (upload, download, list files in /Volumes/)."
---

# Unity Catalog

Guidance for Unity Catalog system tables, volumes, and governance.

## When to Use This Skill

Use this skill when:
- Working with **volumes** (upload, download, list files in `/Volumes/`)
- Querying **lineage** (table dependencies, column-level lineage)
- Analyzing **audit logs** (who accessed what, permission changes)
- Monitoring **billing and usage** (DBU consumption, cost analysis)
- Tracking **compute resources** (cluster usage, warehouse metrics)
- Reviewing **job execution** (run history, success rates, failures)
- Analyzing **query performance** (slow queries, warehouse utilization)
- Profiling **data quality** (data profiling, drift detection, metric tables)

## Reference Files

| Topic | File | Description |
|-------|------|-------------|
| System Tables | [5-system-tables.md](5-system-tables.md) | Lineage, audit, billing, compute, jobs, query history |
| Volumes | [6-volumes.md](6-volumes.md) | Volume file operations, permissions, best practices |
| Data Profiling | [7-data-profiling.md](7-data-profiling.md) | Data profiling, drift detection, profile metrics |

## Quick Start

### Volume File Operations (CLI)

```bash
# List files in a volume
aidevkit volume-files list --path /Volumes/catalog/schema/volume/path/

# Upload files to volume
aidevkit volume-files upload --local /tmp/data.csv --volume /Volumes/catalog/schema/volume/

# Download from volume
aidevkit volume-files download --volume /Volumes/catalog/schema/volume/file.csv --local /tmp/file.csv

# Create directory in volume
aidevkit volume-files mkdir --path /Volumes/catalog/schema/volume/new_folder
```

### Enable System Tables Access

```sql
-- Grant access to system tables
GRANT USE CATALOG ON CATALOG system TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA system.access TO `data_engineers`;
GRANT SELECT ON SCHEMA system.access TO `data_engineers`;
```

### Common Queries

```sql
-- Table lineage: What tables feed into this table?
SELECT source_table_full_name, source_column_name
FROM system.access.table_lineage
WHERE target_table_full_name = 'catalog.schema.table'
  AND event_date >= current_date() - 7;

-- Audit: Recent permission changes
SELECT event_time, user_identity.email, action_name, request_params
FROM system.access.audit
WHERE action_name LIKE '%GRANT%' OR action_name LIKE '%REVOKE%'
ORDER BY event_time DESC
LIMIT 100;

-- Billing: DBU usage by workspace
SELECT workspace_id, sku_name, SUM(usage_quantity) AS total_dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - 30
GROUP BY workspace_id, sku_name;
```

## Best Practices

1. **Filter by date** - System tables can be large; always use date filters
2. **Use appropriate retention** - Check your workspace's retention settings
3. **Grant minimal access** - System tables contain sensitive metadata
4. **Schedule reports** - Create scheduled queries for regular monitoring

---

## CLI Quick Reference (aidevkit CLI)

### Catalogs
```bash
aidevkit uc catalog create --name my_catalog
aidevkit uc catalog get --name my_catalog
aidevkit uc catalog list
aidevkit uc catalog delete --name my_catalog
```

### Schemas
```bash
aidevkit uc schema create --catalog my_catalog --name my_schema
aidevkit uc schema get --catalog my_catalog --name my_schema
aidevkit uc schema list --catalog my_catalog
aidevkit uc schema delete --catalog my_catalog --name my_schema
```

### Volumes
```bash
aidevkit uc volume create --catalog my_catalog --schema my_schema --name my_volume --type MANAGED
aidevkit uc volume get --volume my_catalog.my_schema.my_volume
aidevkit uc volume list --catalog my_catalog --schema my_schema
aidevkit uc volume delete --volume my_catalog.my_schema.my_volume
```

### Volume Files
```bash
aidevkit volume-files list --path /Volumes/catalog/schema/volume/folder/
aidevkit volume-files upload --local /tmp/data.csv --volume /Volumes/catalog/schema/volume/
aidevkit volume-files download --volume /Volumes/catalog/schema/volume/file.csv --local /tmp/file.csv
aidevkit volume-files delete --path /Volumes/catalog/schema/volume/file.csv
aidevkit volume-files mkdir --path /Volumes/catalog/schema/volume/new_folder
aidevkit volume-files info --path /Volumes/catalog/schema/volume/file.csv
```

### Grants
```bash
aidevkit uc grants grant --securable catalog.my_catalog --principal user@example.com --privilege USE_CATALOG
aidevkit uc grants get --securable catalog.my_catalog --principal user@example.com
aidevkit uc grants revoke --securable catalog.my_catalog --principal user@example.com --privilege USE_CATALOG
```

### Functions
```bash
aidevkit uc function list --catalog my_catalog --schema my_schema
aidevkit uc function get --function my_catalog.my_schema.my_function
aidevkit uc function delete --function my_catalog.my_schema.my_function
```

---

## Related Skills

- **[databricks-spark-declarative-pipelines](../databricks-spark-declarative-pipelines/SKILL.md)** - for pipelines that write to Unity Catalog tables
- **[databricks-jobs](../databricks-jobs/SKILL.md)** - for job execution data visible in system tables
- **[databricks-synthetic-data-gen](../databricks-synthetic-data-gen/SKILL.md)** - for generating data stored in Unity Catalog Volumes
- **[databricks-aibi-dashboards](../databricks-aibi-dashboards/SKILL.md)** - for building dashboards on top of Unity Catalog data

## Resources

- [Unity Catalog System Tables](https://docs.databricks.com/administration-guide/system-tables/)
- [Audit Log Reference](https://docs.databricks.com/administration-guide/account-settings/audit-logs.html)
