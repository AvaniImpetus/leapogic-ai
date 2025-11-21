# postgres_cross_account_data_migration.py

## Overview

This module provides the `PostgresCrossAccountDataMigration` class for migrating data from a source PostgreSQL database
to a target PostgreSQL database. It extends `GlueELTStep` and handles schema extraction, table discovery, and
incremental data migration.

## Class

### PostgresCrossAccountDataMigration

**Purpose:**  
An AWS Glue ELT step for migrating PostgreSQL data from source to target database. Discovers all tables in a schema and
migrates data with support for staging and incremental loading.

---

## Methods

### executeFlow()

**Purpose:**  
Main execution method. Orchestrates the migration of a schema from source to target PostgreSQL database.

**Args:**

- `executor` (object): The Glue executor object with methods for querying and data operations.
- `*args`: Variable positional arguments.
- `**kwargs`: Variable keyword arguments.

**Returns:**

- `bool`: `True` if migration completed successfully.

**Raises:**

- `Exception`: If validation fails, table retrieval fails, or data migration fails.

**Example:**

```python
step = PostgresCrossAccountDataMigration()
result = step.executeFlow(executor)
```

**Workflow:**

1. Validates input parameters (source/target secrets, schema name).
2. Retrieves list of tables from source schema.
3. Iterates through each table and migrates data via `_migrate_table_data()`.

---
