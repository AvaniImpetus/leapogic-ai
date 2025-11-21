# td_rs_ddl_comparator.py

## Overview

This module provides the `TeradataRedshiftDDLComparator` class for comparing Data Definition Language (DDL) statements
between Teradata and Redshift databases. It extracts table schemas from both databases and generates detailed comparison
reports in Excel format.

## Classes

### TeradataRedshiftDDLComparator

**Purpose:**  
Compares Teradata and Redshift schemas and generates Excel reports identifying DDL differences (column names, data
types, constraints, defaults, etc.).

---

## Methods

### executeFlow()

**Purpose:**  
Main execution orchestrator. Validates parameters and executes comparison in either batch or single mode.

**Args:**

- `executor` (object): Glue executor.
- `*args`: Variable positional arguments.
- `**kwargs`: Variable keyword arguments.

**Input Parameters:**

- Option 1 (Single Mode):
    - `redshift_schema`: Redshift schema name
    - `teradata_schema`: Teradata schema name
    - `teradata_secret`: Teradata connection secret
    - `redshift_secret`: Redshift connection secret
- Option 2 (Batch Mode):
    - `config_file_path`: Excel configuration file path

**Returns:**

- None. Generates report as side effect.

**Raises:**

- `Exception`: If parameter validation fails.

---

## Usage Example

```python
from com.impetus.idw.wmg.common.td_rs_ddl_comparator import TeradataRedshiftDDLComparator

comparator = TeradataRedshiftDDLComparator()
comparator.executeFlow(executor)
# Generates: td_rs_ddl_comparison_20231215_143022.xlsx
```

