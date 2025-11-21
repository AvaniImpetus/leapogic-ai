# CodebaseComparator Class Documentation

## Overview

The `CodebaseComparator` class is a powerful utility designed to **compare codebases between Git repositories and AWS S3
storage locations**. It generates detailed comparison reports in Excel format, highlighting differences in files,
missing files, and providing line-by-line mismatch analysis. This class is particularly useful in migration workflows
where you need to validate that code has been correctly deployed to S3 or track differences between source and target
environments.

### Purpose

The primary purpose of `CodebaseComparator` is to:

- **Extract and compare** source code from Git repositories and S3 storage
- **Identify file-level differences** with match ratios and mismatch counts
- **Detect missing files** in either Git or S3
- **Highlight line-by-line differences** with visual formatting (colored text)
- **Generate comprehensive Excel reports** with multiple sheets for easy analysis
- **Validate DDL (Data Definition Language)** across Iceberg and Redshift databases
- **Provide detailed auditing** for code migration and deployment processes

---

## Class Hierarchy

```
DataProcessingStep (parent)
  └── GlueELTStep (parent)
      └── CodebaseComparator (current class)
```

`CodebaseComparator` extends `GlueELTStep`, which provides execution framework, parameter management, and S3 operations
capabilities.

---

## Key Features

1. **Multi-source comparison** - Compares Git zip files with S3 codebase directories
2. **File-level analysis** - Calculates match ratios and identifies common/missing files
3. **Line-by-line comparison** - Detects and highlights specific line differences
4. **Excel report generation** - Creates formatted reports with multiple sheets
5. **DDL validation** - Optionally compares database schemas for Iceberg and Redshift
6. **Error handling** - Captures file comparison failures and reports them separately
7. **AWS S3 integration** - Seamlessly downloads files from S3 and uploads reports
8. **Domain-specific comparison** - Finds and compares domain-specific code directories

---

## Class Attributes (Configuration Constants)

| Attribute                   | Value             | Purpose                                       |
|-----------------------------|-------------------|-----------------------------------------------|
| `local_folder`              | `/tmp/code_diff`  | Local temporary directory for extracted files |
| `git_code_dir`              | `git_code`        | Subdirectory for Git codebase extraction      |
| `s3_code_dir`               | `s3_code`         | Subdirectory for S3 codebase download         |
| `output_dir`                | `output`          | Directory for generated reports               |
| `report_file_name_prefix`   | `code_comparison` | Prefix for Excel report filenames             |
| `match_color`               | `green`           | Color for matching content in reports         |
| `unmatch_color`             | `red`             | Color for mismatched content in reports       |
| `match_tag`                 | `match`           | Internal tag for matched content              |
| `unmatch_tag`               | `mismatch`        | Internal tag for mismatched content           |
| `fully_matched_option`      | `Yes`             | Report display for fully matched files        |
| `fully_unmatched_option`    | `No`              | Report display for partially matched files    |
| `missing_in_git_tag`        | `Missing in git`  | Report tag for files not in Git               |
| `missing_in_s3_tag`         | `Missing in s3`   | Report tag for files not in S3                |
| `domain_parent_folder_name` | `src`             | Parent folder name for domain directories     |

---

## Core Methods

### executeFlow(executor, *args, **kwargs)

**Purpose:** Main orchestration method that coordinates the entire comparison workflow.

**Parameters:**

- `executor`: The Glue executor instance for S3 operations
- `*args`: Variable positional arguments (unused in this implementation)
- `**kwargs`: Variable keyword arguments passed from parent execution

**What it does:**

1. Retrieves and validates all required parameters
2. Extracts Git codebase from S3 zip file
3. Downloads S3 codebase to local storage
4. Compares directories and generates reports
5. Optionally runs DDL comparison for databases
6. Uploads the final report back to S3

**Workflow Diagram:**

```
Start
  ↓
Get Parameters (git_zip_path, s3_codebase_path, output_path, etc.)
  ↓
Validate Inputs
  ↓
Extract Git ZIP file
  ↓
Download S3 codebase to local storage
  ↓
Compare Directories
  ↓
Generate Excel Report
  ↓
(Optional) Compare DDL schemas
  ↓
Upload Report to S3
  ↓
End
```

**Example Parameters Used:**

```python
# Required parameters
git_zip_path: "s3://bucket/repo.zip"
s3_codebase_path: "s3://bucket/codebase/domain_name"
output_path: "s3://bucket/reports"

# Optional parameters for DDL validation
redshift_secret: "my-redshift-credentials"
redshift_zip_s3_path: "s3://bucket/redshift_ddl.zip"
table_names_csv_path: "s3://bucket/table_names.csv"
```

---

### get_all_files(base_dir) [Static Method]

**Purpose:** Recursively retrieves all files in a directory tree, returning their relative paths.

**Parameters:**

- `base_dir` (str): Base directory to start traversal

**Returns:**

- `list`: List of relative file paths from `base_dir`

**Example:**

```python
# Directory structure:
# /tmp/code/
#   ├── src/
#   │   ├── file1.py
#   │   └── subdir/
#   │       └── file2.py
#   └── config.yaml

files = CodebaseComparator.get_all_files("/tmp/code")
# Returns:
# [
#     'src/file1.py',
#     'src/subdir/file2.py',
#     'config.yaml'
# ]
```

---

## Usage Guide

### Basic Usage Example

```python
from com.impetus.idw.wmg.common.codebase_comparator import CodebaseComparator

# Create an instance
comparator = CodebaseComparator()

# Execute comparison with all parameters
comparator.execute(
    git_zip_path="s3://my-bucket/git-repo.zip",
    s3_codebase_path="s3://my-bucket/codebase/accounting_domain",
    output_path="s3://my-bucket/comparison-reports"
)
```

### Advanced Usage with DDL Comparison

```python
comparator = CodebaseComparator()

comparator.execute(
    # Code comparison parameters
    git_zip_path="s3://my-bucket/git-repo.zip",
    s3_codebase_path="s3://my-bucket/codebase/finance_domain",
    output_path="s3://my-bucket/reports",
    
    # DDL comparison parameters (optional)
    redshift_zip_s3_path="s3://my-bucket/redshift-ddl.zip",
    redshift_secret="prod/redshift/credentials",
    table_names_csv_path="s3://my-bucket/table_mapping.csv"
)
```

### Custom Parameter Module Usage

```python
# Define parameters in a module
# parameters.py
git_zip_path = "s3://company-bucket/repository.zip"
s3_codebase_path = "s3://company-bucket/deployments/domain1"
output_path = "s3://company-bucket/audit-reports"
log_level = "DEBUG"

# Use with CodebaseComparator
comparator = CodebaseComparator()
comparator.execute(
    parameter_module="parameters",
    workflowName="codebase_validation",
    sessionName="daily_validation"
)
```

---

## Important Parameters Reference

### Required Parameters

| Parameter          | Type   | Description                   | Example                       |
|--------------------|--------|-------------------------------|-------------------------------|
| `git_zip_path`     | String | S3 path to Git repository ZIP | `s3://bucket/repo.zip`        |
| `s3_codebase_path` | String | S3 path to deployed codebase  | `s3://bucket/codebase/domain` |
| `output_path`      | String | S3 path for output reports    | `s3://bucket/reports`         |

### Optional Parameters

| Parameter              | Type   | Description                        | Example                        |
|------------------------|--------|------------------------------------|--------------------------------|
| `redshift_zip_s3_path` | String | S3 path to Redshift DDL ZIP        | `s3://bucket/ddl/redshift.zip` |
| `redshift_secret`      | String | AWS Secrets Manager secret name    | `prod/redshift/creds`          |
| `table_names_csv_path` | String | S3 path to CSV with table mappings | `s3://bucket/tables.csv`       |
| `log_level`            | String | Logging level (DEBUG, INFO, etc.)  | `DEBUG`                        |

### CSV Format for Table Names

The `table_names_csv_path` should point to a CSV with this structure:

```csv
table_name,target
customers_table,iceberg
orders_table,iceberg
sales_ledger,redshift
transactions_table,
```

- `table_name`: Name of the database table
- `target`: Target database system (`iceberg`, `redshift`, or empty for default iceberg)

---

## Exception Handling

The class handles various exceptions gracefully:

### Input Validation Exceptions

```python
# Missing required parameters
raise Exception('GIT codebase zip file path not provided')
raise Exception('s3 codebase file path not provided')
raise Exception('Output path not provided')

# Domain not found
raise Exception(
    f's3 location {s3_codebase_path} '
    f'for folder {s3_codebase_dir_name} is not available in zip.'
)
```

### File Comparison Exceptions

- Files that fail comparison are logged and added to the "Failed Files" sheet
- Exceptions don't stop the entire process; comparison continues for other files

### DDL Comparison Exceptions

- DDL comparison errors are caught, logged, but don't fail the entire workflow
- Main code comparison still completes successfully

---

## Output Report Structure

### File Naming Convention

```
code_comparison_{domain_name}.xlsx
```

### Report Content

**Sheet 1 - Code Comparison Summary:**

- Quick overview of all files compared
- Match ratios for each file
- File line counts
- Mismatch counts

**Sheet 2 - Code Comparison Detail:**

- Line-by-line differences
- Color-coded highlighting (green for matching, red for different)
- Specific line numbers
- Exact content comparison

**Sheet 3 - Code Comparison Missing Files:**

- Files only in Git (missing in S3)
- Files only in S3 (missing in Git)

**Sheet 4 - Code Comparison Failed Files:**

- Files that couldn't be compared
- Usually binary files or access issues

---

## Logging

The class uses Python's `logging` module extensively. All operations are logged at appropriate levels:

```python
log = logging.getLogger(__name__)  # Class-level logger

# Debug logs
self.log.debug('validating required input...')
self.log.debug(f"comparing directories from git {git_dir} and from s3 {s3_dir}")

# Info logs
self.log.info(f"GIT ZIP path: {git_zip_s3_path}")
self.log.info(f"Report written to '{output_excel}'")

# Error logs
self.log.error(f'Error occurred while comparing ddl: {e}')
self.log.error(f"Error occurred while comparing files: {git_path} and {s3_path}: {e}")
```

**Set log level via parameter:**

```python
comparator.execute(
    log_level="DEBUG",  # Shows all debug messages
    ...
)
```

---

## Related Classes

1. **DDLComparator** - Compares database schema definitions
2. **GlueELTStep** - Parent class providing execution framework
3. **GlueExecutorDAL** - Provides Spark/Glue execution capabilities
4. **glue_utils** - AWS Glue utility functions

---

## Common Issues and Solutions

| Issue                                     | Cause                               | Solution                                                  |
|-------------------------------------------|-------------------------------------|-----------------------------------------------------------|
| "GIT codebase zip file path not provided" | Missing `git_zip_path` parameter    | Ensure parameter is set in job configuration              |
| "s3 location ... is not available in zip" | Domain not found in src/ folder     | Verify domain folder exists in src/ directory in Git repo |
| Empty Excel report                        | No common files between directories | Check if both Git and S3 paths point to same domain       |
| File comparison fails silently            | Binary files or encoding issues     | Check failed files sheet in report                        |
| DDL comparison doesn't run                | Missing CSV file                    | Provide `table_names_csv_path` parameter                  |

---

## Class Methods Summary Table

| Method                    | Purpose              | Input                        | Output                       |
|---------------------------|----------------------|------------------------------|------------------------------|
| `executeFlow()`           | Main orchestration   | executor, **kwargs           | None (generates report)      |
| `extract_zip()`           | Extract Git zip      | zip S3 path                  | Local directory path         |
| `validate_input()`        | Validate parameters  | 3 required paths             | None (raises if invalid)     |
| `validate_ddl_input()`    | Validate DDL params  | 3 optional paths             | Tuple of (bool, bool, dict)  |
| `get_table_names()`       | Parse table CSV      | CSV S3 path                  | Dictionary of tables by type |
| `download_s3_folder()`    | Download from S3     | S3 path, local dir           | None (downloads files)       |
| `find_domain_path()`      | Find domain folder   | Root dir, domain name        | Directory path or None       |
| `get_all_files()`         | List all files       | Base directory               | List of relative paths       |
| `highlight_differences()` | Highlight text diffs | Two strings                  | Tuple of formatted lists     |
| `compare_files()`         | Compare two files    | Git file path, S3 file path  | Tuple with statistics        |
| `compare_directories()`   | Compare two dirs     | Git dir, S3 dir, output path | None (creates Excel)         |

---

## Dependencies

- **Python Libraries:**
    - `logging` - Standard logging
    - `traceback` - Exception traceback handling
    - `zipfile` - ZIP file extraction
    - `xlsxwriter` - Excel report generation
    - `subprocess` - Execute AWS CLI commands
    - `difflib` - Text/sequence comparison
    - `os` - File system operations

- **Internal Modules:**
    - `com.impetus.idw.wmg.common.ddl_comparator.DDLComparator`
    - `com.impetus.idw.wmg.elt.glue_elt_step.GlueELTStep`
    - `com.impetus.idw.wmg.common.glue_utils`

- **External Services:**
    - AWS S3 (for downloading/uploading files)
    - AWS CLI (for S3 operations)
    - AWS Secrets Manager (for credentials)
    - AWS Glue (for executor)

---

## Entry Point

The class can be executed as a standalone script:

```python
if __name__ == '__main__':
    step = CodebaseComparator()
    step.start()
```

This calls the `start()` method inherited from `GlueELTStep`, which:

1. Calls `execute()` with `start_job=True`
2. Analyzes job resources after completion
3. Closes connections safely
4. Handles any exceptions with custom messaging

---

## Summary

The `CodebaseComparator` class is an essential tool for validating code migrations and deployments. It provides
comprehensive comparison capabilities, detailed reporting, and optional database schema validation. By following this
guide, you can effectively use this class to ensure code consistency between Git repositories and S3 deployments.
