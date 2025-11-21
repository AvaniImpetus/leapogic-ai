## data_migration.py

This document is a detailed guide to the `DataMigration` class defined in the `data_migration.py` module. It covers
purpose, how the class works, method-by-method documentation, configuration details, examples, common edge cases, and
suggested improvements.

---

## Overview

`DataMigration` is a Glue ELT step extending `GlueELTStep` (imported
as `from com.impetus.idw.wmg.elt.glue_elt_step import GlueELTStep`). It orchestrates data migration operations driven by
a CSV configuration file. The class reads a CSV that describes source and target connection/table metadata and then
migrates data according to a specified `load_type` (append, overwrite, incremental).

This class is intended to run inside the project's Glue/ELT framework where an `executor` object provides database /
Spark-like operations (readFile, executeQuery, saveDataframeToTable, etc.). It uses a CSV config file with rows
describing migration tasks; each row is validated and executed.

---

## Purpose

- Read a configuration CSV from S3 (or other location supported by executor.readFile). The CSV should contain rows with
  information about source/target connections and options.
- For each valid config row, read source data with an SQL-style query (constructed from source_table, source_columns,
  and source_filter), then write to the specified target using the configured load type.
- Support three load types:
    - `append` (default) — append rows to target
    - `overwrite` — overwrite target (delegated to executor.saveDataframeToTable via options `{ 'mode': load_type }`)
    - `incremental` — reads watermark column from the target to determine which rows to copy

The class logs progress and collects lists of successful and failed rows.

---

## Configuration (job args and CSV format)

Job arguments (expected by the step):

- `config_path` (required) : S3 or file path to the configuration CSV.
- `config_file_sep` (optional) : Separator used in the config CSV; default is `#` in the code.

CSV configuration file format (columns expected per row):

- `source_name` : Source database type or name (used as dbType when running queries).
- `source_table` : Source table (fully qualified e.g. `db.schema.table` or `db.table`) or any SQL-able identifier.
- `source_connection` : Named connection to use for the source.
- `source_columns` : (Optional) Comma-separated columns to select. If absent, `*` is used.
- `source_filter` : (Optional) WHERE clause filter (without the `WHERE` keyword) applied to the source query.
- `target_name` : Target database type or name (used as dbType when running queries/writes).
- `target_table` : Target table (fully qualified `db.schema.table` or `db.table`).
- `target_connection` : Named connection to use for the target.
- `load_type` : (Optional, default `append`) One of `append`, `overwrite`, or `incremental`.
- `incremental_load_column` : Required if `load_type` is `incremental` — column name used as watermark.

Notes:

- The code expects rows accessible via `row.get('<column_name>')`. This typically works with dict-like rows (or pyspark
  Row objects) returned by the CSV reader.

---

## Class: DataMigration

Inheritance: `GlueELTStep` (project-specific base class)

Class-level attribute:

- `log` : standard Python logger for the module (`logging.getLogger(__name__)`)

Public entry point:

- `executeFlow(self, executor, *args, **kwargs)` — main method invoked by the framework to run the step. It:
    - Reads job parameters using `self.get_param_value` (e.g., `config_path`, `config_file_sep`).
    - Reads the config CSV into a dataframe using `read_config_file`.
    - Iterates rows, validates them and runs migration per row.

Detail: this method expects `self.get_param_value(...)` to be provided by `GlueELTStep`.

---

## Methods (detailed)

All private methods are named with leading double-underscores — they are used internally by the step.

### executeFlow(self, executor, *args, **kwargs)

- Purpose: orchestrates the overall migration flow.
- Parameters:
    - `executor` — an object providing runtime methods used by the implementation (see Executor interface section
      below).
    - `*args, **kwargs` — supported but unused in this implementation.
- Behavior:
    - Reads config path and separator.
    - Calls `read_config_file` to get a dataframe of configurations.
    - Iterates over each configuration row, validates it via `__validate_row__` and migrates it via `__migrate_data__`.
    - Collects `success_rows` and `failed_rows` lists and logs summary.
- Exceptions: raises Exception if `config_path` is not provided.

## Expected Executor interface (what the `executor` must provide)

This class delegates I/O to `self.executor`. The following methods and behaviors are required:

- `executor.readFile(filetype, path, temporary_table, options)` — returns a dataframe-like object representing the CSV (
  header-aware). Used by `read_config_file`.
- `executor.executeQuery(query, temporary_table, dbType, connection_name)` — executes a query and returns an object
  with `.resultObject` that behaves like a dataframe and supports `.count()` and `.collect()`.
- `executor.saveDataframeToTable(source_df, target_schema, target_table, options, dbType, connection_name)` — writes a
  dataframe to target table using the given options.

Note: The `GlueELTStep` base class probably sets `self.executor` and provides `self.get_param_value(...)`. Ensure the
runtime framework wires those in when running the step.

---

## Example config CSV

Assume separator is `,` (CSV) or `#` depending on job arg `config_file_sep`.

Example rows (comma separated):

source_name,source_table,source_connection,source_columns,source_filter,target_name,target_table,target_connection,load_type,incremental_load_column
postgres,public.orders,pg_conn,id,order_date >= '2024-01-01',redshift,analytics.public.orders,rs_conn,append,

Incremental example:

postgres,public.orders,pg_conn,id,order_date >= '
2024-01-01',redshift,analytics.public.orders,rs_conn,incremental,order_date

Notes for CSV:

- If `source_columns` is blank, all columns are selected (`*`).
- `source_filter` should be an expression suitable for SQL `WHERE` clause (no `WHERE` prefix).

---

## Quick usage example (conceptual)

The step is intended to be invoked inside the project's framework. In the file's `__main__` it instantiates and
calls `start()`:

```python
if __name__ == '__main__':
    step = DataMigration()
    step.start()
```

To run manually (framework dependent):

- Ensure the job is configured with `config_path` and optionally `config_file_sep`.
- `config_path` must be accessible to `executor.readFile` (S3 path or local file supported by executor).

Example (conceptual):

- Set job param `config_path` to `s3://my-bucket/configs/data_migration.csv` and `config_file_sep` to `,`.

Because this module depends on the project's `GlueELTStep` and `executor` runtime, a direct stand-alone run may fail;
it's recommended to run via the project's job runner or test harness that provides `executor` and param resolution.

---

## Edge cases and error handling

- Missing `config_path`: `executeFlow` raises `Exception('Need config CSV file path to proceed.')`.
- Invalid rows: `__validate_row__` returns `False` for invalid rows; such rows are skipped and logged.
- Exceptions during migration of a row: the row is added to `failed_rows` and logged; migration continues for other
  rows.
- Incremental load: if `__get_max_value__` returns no rows, an exception is raised and that migration row will be marked
  failed.

---