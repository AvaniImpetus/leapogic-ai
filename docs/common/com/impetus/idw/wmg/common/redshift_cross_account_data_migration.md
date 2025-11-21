# redshift_cross_account_data_migration.py

## Overview

This module provides the `RedshiftCrossAccountDataMigration` class for migrating data to Amazon Redshift from a source
database. Supports both configuration file-based migration and schema-based migration with incremental loading
capabilities.

## Classes

### RedshiftCrossAccountDataMigration

**Purpose:**  
AWS Glue ELT step for migrating data to Redshift. Supports two modes: configuration file-based (for flexible table
mappings) and schema-based (auto-discovery from target schema).

---

## Methods

### executeFlow()

**Purpose:**  
Main execution orchestrator. Supports both configuration file and schema-based data migration modes.

**Args:**

- `executor` (object): Glue executor object.
- `*args`: Variable positional arguments.
- `**kwargs`: Variable keyword arguments.

**Returns:**

- None. Performs migration and writes results to S3 if error_output_path is provided.

**Raises:**

- Exception if configuration is invalid or migration fails.

**Input Parameters:**

- `config_path` (str, optional): S3 path to CSV configuration file.
- `config_file_sep` (str, optional): CSV separator. Default: `'#'`.
- `source_schema` (str, optional): Source schema for auto-discovery mode.
- `target_schema` (str, optional): Target Redshift schema.
- `secret_name` (str, optional): Redshift secret name.
- `error_output_path` (str, optional): S3 path for failed rows output.

**Configuration File Format (CSV):**

```
source_table,source_columns,source_filter,target_table,secret_name,load_type
shared_db.schema.table1,,1=1,schema.table1,secret_name,append
shared_db.schema.table2,col1;col2,date>2023-01-01,schema.table2,secret_name,overwrite
```

---