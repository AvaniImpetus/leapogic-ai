# td_latest_ts_extractor.py

## Overview

This module provides the `TeradataLatestTimestampExtractor` class for extracting latest timestamp/date values from
Teradata tables. Supports parallel extraction using thread pools and generates Excel reports with extracted metadata.

## Classes

### TeradataLatestTimestampExtractor

**Purpose:**  
Extracts latest timestamp and date column values from Teradata tables, identifies and processes only tables with
temporal columns, and generates summary reports.

**Class Attributes:**

- `filter_column_types`: List of column types to filter on (`['timestamp', 'date']`).

---

## Methods

### executeFlow()

**Purpose:**  
Main execution orchestrator. Reads table mapping, extracts timestamp details (parallel or sequential), and saves results
to report.

**Args:**

- `executor` (object): Glue executor.
- `*args`: Variable positional arguments.
- `**kwargs`: Variable keyword arguments.

**Input Parameters:**

- `teradata_secret` (str, required): Teradata connection secret name.
- `parallel_threads` (int, optional): Number of parallel threads. Default: `10`. Set to 0 for sequential.

**Returns:**

- None. Generates report as side effect.

**Raises:**

- `Exception`: If secret name not provided.

**Workflow:**

1. Reads mapping file to get view-to-table mappings.
2. Executes extraction in parallel (if threads > 0) or sequentially.
3. Logs failed tables.
4. Saves results to Excel report via `save_report()`.

---

### execute_parallel()

**Purpose:**  
Executes table extraction in parallel using ThreadPoolExecutor.

**Args:**

- `parallel_threads` (int): Number of worker threads.
- `view_table_mapping` (dict): Mapping of view names to table names.
- `secret_name` (str): Teradata secret name.
- `failed_tables` (list): List to collect failed extractions (updated in place).
- `summary_rows` (list): List to collect summary data (updated in place).
- `output_rows` (list): List to collect extracted results (updated in place).

**Returns:**

- None. Updates output lists as side effect.

**Raises:**

- Exception if any thread fails.

**Notes:**

- Uses `concurrent.futures.ThreadPoolExecutor` for parallel execution.
- Waits for all tasks to complete before returning.
- Re-raises any thread exceptions.

---

### save_report()

**Purpose:**  
Saves extraction results to an Excel file with timestamp metadata.

**Args:**

- `output_rows` (list): List of extracted data rows.
- `summary_rows` (list): List of summary data rows.

**Returns:**

- None. Writes file as side effect.

**Raises:**

- Exception if file write fails.

---