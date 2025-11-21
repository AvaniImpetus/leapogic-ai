# redshift_object_extraction_util.py

## Overview

This module provides utilities for extracting DDL (Data Definition Language) statements from database objects (tables
and views) in Redshift or other databases. It includes the `DBObject` class and `RedshiftObjectDDLExtractor` class for
schema extraction, packaging, and S3 upload.

## Class Index

1. `DBObject`
2. `RedshiftObjectDDLExtractor`

---

## Classes

### DBObject

**Purpose:**  
A data class representing a database object (table or view) with its DDL definition and dependent objects.

**Constructor: `__init__(object_name, object_type, object_def, dependent_objects)`**

**Constructor Args:**

- `object_name` (str): Name of the database object.
- `object_type` (str): Type of object ("Table" or "View").
- `object_def` (str): DDL statement for creating the object.
- `dependent_objects` (list): List of dependent `DBObject` instances.

**Properties:**

- `object_name`: Database object name.
- `object_type`: Object type.
- `object_def`: DDL definition.
- `dependent_objects`: Dependent objects list.

---

### RedshiftObjectDDLExtractor

**Purpose:**  
AWS Glue ELT step for extracting DDL statements from database objects (tables and views) and uploading them to S3
organized by schema.

---

## Methods

### executeFlow()

**Purpose:**  
Main execution method. Extracts DDL for specified objects or entire schemas and uploads to S3.

**Args:**

- `executor` (object): Glue executor.
- `*args`: Variable positional arguments.
- `**kwargs`: Variable keyword arguments.

**Returns:**

- None. Uploads files to S3 as side effect.

**Input Parameters:**

- `output_path` (str, required): S3 output path for extracted DDL.
- `schema_name` (str, optional): Schema name(s) to extract (comma-separated).
- `table_names` (str, optional): Specific table names (comma-separated).
- `view_names` (str, optional): Specific view names (comma-separated).
- `secret_name` (str, optional): Database connection secret name.
- `extract_dependent_objects` (bool, optional): Include dependent objects. Default: `True`.

**Raises:**

- Exception if required parameters missing or extraction fails.

---

### execute_query()

**Purpose:**
Executes the query using the arguments provided.

**Args:**

- `query` (str): the query to execute.
- `secret_name` (str): Database connection secret.

**Raises:**

- Exception if query fails.

## Output Format

Extracted DDL statements are organized as follows:

1. **Directory Structure:**
   ```
   s3://output-path/
   ├── schema_1/
   │   ├── tables.sql
   │   └── views.sql
   ├── schema_2/
   │   ├── tables.sql
   │   └── views.sql
   └── extracted_ddl.zip
   ```

2. **SQL Files:** Each schema has separate files for tables and views.
3. **ZIP Archive:** All SQL files packaged into a single ZIP for easy download.

