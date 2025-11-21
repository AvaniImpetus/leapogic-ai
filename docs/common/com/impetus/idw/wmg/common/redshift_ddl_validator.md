# redshift_ddl_validator.py

## File Overview

This is a comprehensive module for validating and comparing Redshift database schema definitions. It validates DDL
statements by executing them against Redshift and comparing source vs target schema objects (tables, views) with
detailed reporting of column mismatches, constraints, and other structural differences.

## Classes

### RedshiftDDLValidator

**Purpose:**  
Validates Redshift DDL statements and compares database object definitions between source and target schemas. Generates
detailed Excel reports identifying structural differences.

---

## Input Parameters

The module supports two modes of operation:

**Mode 1: Single Schema Validation**

- `source_schema`: Source schema name
- `target_schema`: Target schema name(s) (comma-separated for multiple)
- `table_names`: Table names to compare (optional, comma-separated)
- `view_names`: View names to compare (optional, comma-separated)
- `redshift_secret`: Redshift connection secret name
- `output_path`: Output path for Excel report

**Mode 2: Batch Validation using Config Sheet**

- `config_file_path`: Path to Excel configuration file
- `config_sheet_name`: Sheet name in config file (default: `'Config'`)
- `redshift_secret`: Redshift connection secret name
- `output_path`: Output path for Excel report

**Config Sheet Format:**

```
Source Schema | Target Schema | Table Names | View Names | Enabled
source_db     | target_db     | tbl1,tbl2   | view1      | True
source_db2    | target_db2    |             |            | True
```

---

## Output Report Format

**Excel File Name:**

- Single run: `ddl_validation_{source_schema}_to_{target_schemas}.xlsx`
- Batch run: `ddl_validation_{dd_mm_yyyy}.xlsx`

**Report Sheet: "Object Validation Report"**

**Columns:**

1. `Source Schema`: Source schema name
2. `Target Schema`: Target schema name
3. `Object Name`: Table or view name
4. `Object Type`: "Table" or "View"
5. `Column Name Status`: Matched / {count} column not matched
6. `Column Datatype Status`: Matched / {count} datatype mismatches
7. `Column Collation Status`: Matched / {count} collation mismatches
8. `Primary Key Status`: Matched / {count} constraint mismatches
9. `Foreign Key Status`: Matched / {count} constraint mismatches
10. `Unique Key Status`: Matched / {count} constraint mismatches
11. `Default Value Status`: Matched / {count} default mismatches
12. `Overall Status`: Matched / Not Matched / Not Available
13. `Comments`: Detailed difference descriptions

---

## Methods

### executeFlow()

**Purpose:**  
Main execution method that orchestrates DDL validation. Determines execution mode and calls appropriate runner.

**Args:**

- `executor` (object): Glue executor object.
- `*args`: Variable positional arguments.
- `**kwargs`: Variable keyword arguments.

**Returns:**

- None. Generates report as side effect.

**Raises:**

- Exception if validation fails.

**Execution Modes:**

1. If `config_file_path` provided: Batch mode via `batch_run()`.
2. Otherwise: Single mode via `single_mode_run()`.

---

### batch_run()

**Purpose:**  
Processes multiple schema validation configurations from an Excel file.

**Args:**

- `config_file_path` (str): S3 or local path to Excel configuration file.
- `redshift_secret` (str): Redshift connection secret name.

**Returns:**

- List of validation results (one per configuration row).

**Raises:**

- Exception if file read or validation fails.

---

### single_mode_run()

**Purpose:**  
Validates a single source-to-target schema pair.

**Args:**

- `redshift_secret` (str): Redshift connection secret.

**Returns:**

- Validation results (written to Excel report).

**Raises:**

- Exception if schema retrieval or comparison fails.

---

## Usage Example

```python
from com.impetus.idw.wmg.common.redshift_ddl_validator import RedshiftDDLValidator

validator = RedshiftDDLValidator()
validator.executeFlow(executor)  # Validates per parameter configuration
# Generates: ddl_validation_source_to_target.xlsx
```

