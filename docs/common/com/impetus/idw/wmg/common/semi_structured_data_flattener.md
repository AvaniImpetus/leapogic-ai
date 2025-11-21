# semi_structured_data_flattener.py

## Overview

This module provides utilities for flattening semi-structured data (XML, JSON) into relational tables. It extracts data
from files, flattens nested structures, generates SQL queries for inserting flattened data, and creates readme
documentation files.

## Function Index

1. `add_value_to_set()`
2. `is_int()`
3. `generate_readme()`
4. Additional helper functions

---

## Function

### process_files

Main function for the files to extract and flatten data from.

```
process_files(spark, etl_job_name, input_path, output_path, db_name, tbl_data_storage_path=None,
                  save_read_me_query_file=True, file_stage_tbl_suffix='_file_stg',
                  flatten_data_tbl_suffix='_flatten_data')
```

**Args:**

- `spark`: The spark session object.
- `etl_job_name`: The ETL job name.
- `input_path`: Path to input file.
- `output_path`: Path to output location.
- `db_name`: DB Name.

#### Optional Args

- `tble_data_storage_path`
- `save_read_me_query_file`
- `file_stage_tbl_suffix`
- `flatten_data_tbl_suffix`

---

### generate_readme()

**Purpose:**  
Generates a SQL query that extracts flattened data from a stage table and creates a readable data extraction query.
Saves the query to S3 as a text file.

**Args:**

- `s3_bucket_name` (str): S3 bucket name.
- `s3_path` (str): S3 output path prefix.
- `distinctColumns` (set): Set of column names from flattened data.
- `etl_job_name` (str): ETL job name (used for table naming).
- `readmeRequired` (bool): If `True`, write readme query to S3.
- `db_name` (str): Database name for fully qualified table names.
- `file_stage_tbl_suffix` (str): Suffix for staging table name.

**Returns:**

- None. Uploads file to S3 as side effect.

**Raises:**

- Exception if S3 upload fails.

**Example:**

```python
columns = {'customer_id', 'order_date', 'amount'}
generate_readme('my-bucket', 'output/flatten/', columns, 'json_flattener', True, 'analytics', '_stage')
# Generates: my-bucket/output/flatten/json_flattener_readme.txt
```

---

**Generated Query Pattern:**
The function generates a complex SQL query that:

1. Uses positional matching to extract values from key-value pairs.
2. Handles pipe-delimited column format.
3. Creates columns dynamically for each distinct column name.
4. Returns ROW_ID, FILE_NAME, and extracted column values.

---

## Module Parameters

When used as a PySpark transformation:

```python
"""
Input Parameters:
    input_path (str, required): S3 input path to XML/JSON files
    output_path (str, required): S3 output path
    flatten_data_db_name (str, optional): Database name for flattened tables
    flatten_data_storage_path (str, optional): S3 location for flattened data (defaults to output_path)
    save_read_me_query_file (bool, optional): Write select query file. Default: True
    file_stage_tbl_suffix (str, optional): File stage table suffix. Default varies
    flatten_data_tbl_suffix (str, optional): Flattened data table suffix. Default varies
"""
```

## XML/JSON Processing

The module uses:

- `flatten_json`: Python library for flattening nested JSON structures.
- `xmltodict`: Python library for converting XML to dictionaries.
- PySpark for distributed data processing.

---

## Output Format

Flattened data is stored as:

1. **Stage Table**: Intermediate table with raw flattened key-value pairs.
2. **Flattened Table**: Final table with columns extracted from distinct keys.
3. **Readme Query**: SQL file with helper queries for data extraction.

___
