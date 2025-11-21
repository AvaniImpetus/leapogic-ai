# teradata_object_extraction_util.py

## File Overview

This module provides the `TeradataObjectDDLExtractor` class for extracting DDL (Data Definition Language) statements
from Teradata database objects (tables and views). Extracts schemas, manages dependent objects, packages DDL into
organized files, and uploads to S3.

---

## Classes

### TeradataObjectDDLExtractor

**Purpose:**  
Extracts DDL statements for Teradata tables and views, organizes by schema, resolves dependent objects recursively,
packages into ZIP format, and uploads to S3 for migration/documentation purposes.

---

## Methods

### executeFlow()

**Purpose:**  
Main execution orchestrator. Extracts DDL for specified or auto-discovered objects.

**Args:**

- `executor` (object): Glue executor.
- `*args`: Variable positional arguments.
- `**kwargs`: Variable keyword arguments.

**Input Parameters:**

- `output_path` (str, required): S3 path where ZIP will be uploaded.
- `secret_name` (str, required): Teradata connection secret name.
- `schema_name` (str, optional): Schema name(s) to extract (comma-separated). Auto-discovers if provided.
- `table_names` (str, optional): Specific table names (comma-separated).
- `view_names` (str, optional): Specific view names (comma-separated).
- `extract_dependent_objects` (bool, optional): Extract dependent objects recursively. Default: `True`.

**Returns:**

- None. Uploads ZIP file to S3 as side effect.

**Raises:**

- `Exception`: If required parameters missing or extraction fails.

---

### remove_sql_comments()

**Purpose:**
Removes single-line (`--`) and multi-line (`/* */`) comments from SQL queries while preserving comments within quoted
strings.

**Args**:

- `query` (str): SQL query potentially containing comments

**Returns**:

- `str`: Query with comments removed

## Output Format

Extracted DDL is organized as:

```
s3://output-path/
├── schema_name_1/
│   ├── tables.sql          # All table DDLs for this schema
│   └── views.sql           # All view DDLs for this schema
├── schema_name_2/
│   ├── tables.sql
│   └── views.sql
└── extracted_ddl.zip       # ZIP archive of all files
```

Each SQL file contains:

```sql
-- DDL for Object 1
CREATE TABLE schema.table1 (columns...);

-- DDL for Object 2
CREATE TABLE schema.table2 (columns...);
```

---

## Features

- **Schema Auto-Discovery:** If `schema_name` provided, automatically discovers all tables and views.
- **Dependent Object Resolution:** Optionally extracts dependent objects recursively (e.g., tables referenced by views).
- **ZIP Packaging:** Organizes all DDL files by schema in a single ZIP archive for easy download.
- **S3 Integration:** Automatically uploads results to specified S3 location.
- **Error Handling:** Captures and logs extraction failures; continues processing.

---

## Usage Example

```python
from com.impetus.idw.wmg.common.teradata_object_extraction_util import TeradataObjectDDLExtractor

extractor = TeradataObjectDDLExtractor()
extractor.executeFlow(executor)
# Extracts DDL from Teradata and uploads to S3 as extracted_ddl.zip
```

