# job_stats_extractor.py

## File Overview
This module provides the `GlueJobStatsExtractor` class for extracting and reporting AWS Glue job execution statistics (start time, end time, execution duration, capacity allocation, worker details, DPU consumption). Supports single job analysis or bulk analysis of multiple jobs.

## Class Index
1. `GlueJobStatsExtractor`

---

## Classes

### GlueJobStatsExtractor
**Purpose:**  
Extracts execution statistics from Glue job runs and generates reports in Excel format. Provides analysis for single or multiple jobs.

---

## Methods

### extract()
**Purpose:**  
Main extraction method. Analyzes Glue job(s) and retrieves their execution statistics. Supports no arguments (current job), single job name, comma-separated list, or list/set of job names.

**Args:**
- `job_name` (str, list, set, optional): Job name(s) to analyze. If `None`, analyzes the current job. Can be:
  - Single job name (str)
  - Comma-separated job names (str)
  - List or set of job names

**Returns:**
- None. Calls underlying methods and logs results.

**Raises:**
- `Exception`: If job name or run ID cannot be determined.

**Example:**
```python
extractor = GlueJobStatsExtractor()
extractor.extract()  # Extracts stats for current job
extractor.extract('my-etl-job')  # Single job
extractor.extract('job1,job2,job3')  # Multiple jobs
```

---

### get_jobs_detail()
**Purpose:**  
Retrieves execution details for multiple jobs. Finds the last successful run for each job and extracts statistics.

**Args:**
- `glue_client` (boto3 client): Glue service client.
- `job_names` (list): List of job names to analyze.
- `output_path` (str, optional): S3 or local path where Excel report will be saved. Default: `None`.

**Returns:**
- None. Logs progress and optionally saves report.

**Raises:**
- Exception from Glue API if jobs cannot be retrieved.

**Example:**
```python
extractor.get_jobs_detail(glue_client, ['job1', 'job2', 'job3'], output_path='/tmp/stats')
```

---

### save_report()
**Purpose:**  
Saves job statistics to an Excel file with a timestamped filename.

**Args:**
- `rows` (list): List of dictionaries containing job stats (one dict per job).
- `output_path` (str): Directory path where the Excel file will be saved.

**Returns:**
- None. Creates Excel file as side effect.

**Raises:**
- Exception if file write fails (propagated from pandas).

**Example:**
```python
rows = [{'Job Name': 'job1', 'Execution Time': 120, ...}, ...]
save_report(rows, '/tmp/output')
# Creates file: /tmp/output/data_2023_12_15_14_30_22.xlsx
```

**Notes:**
- File is named with timestamp: `data_{YYYY}_{MM}_{DD}_{HH}_{MM}_{SS}.xlsx`.
- Uses pandas DataFrame to write Excel file.

---

### get_job_last_success_run()
**Purpose:**  
Finds the most recent successful job run for a given job.

**Args:**
- `glue_client` (boto3 client): Glue service client.
- `job_name` (str): Name of the job.

**Returns:**
- `dict` or `None`: Job run details (including `'Id'` field) if successful run found; `None` if no successful runs exist.

**Raises:**
- Exception if Glue API call fails.

**Example:**
```python
run = extractor.get_job_last_success_run(glue_client, 'my-job')
if run:
    print(f"Last success run ID: {run['Id']}")
```

---

### get_job_details()
**Purpose:**  
Extracts detailed statistics for a specific job run. Prints formatted output to console and returns a dictionary of stats.

**Args:**
- `glue_client` (boto3 client): Glue service client.
- `job_name` (str): Name of the job.
- `job_run_id` (str): Run ID of the specific job execution.

**Returns:**
- `dict`: Dictionary with keys:
  - `'Job Name'`: Job name
  - `'Last Run ID'`: Job run ID
  - `'Start Time'`: Start timestamp
  - `'End Time'`: End timestamp
  - `'Execution Time'`: Duration in seconds
  - `'Allocated Capacity'`: Allocated DPU/capacity
  - `'Max Capacity'`: Max allocated capacity
  - `'Worker Type'`: Type of workers (e.g., G.2X, G.1X)
  - `'Number Of Workers'`: Worker count
  - `'DPU Seconds'`: Total DPU-seconds consumed

**Raises:**
- Exception if job run cannot be retrieved.

**Example:**
```python
stats = extractor.get_job_details(glue_client, 'my-job', 'jr-12345')
print(stats['Execution Time'])  # Output: 250 (seconds)
```

---

### get_glue_client()
**Purpose:**  
Creates and returns a Glue service boto3 client.

**Args:**
- `region_name` (str, optional): AWS region name. If not provided, uses `glue_utils.get_aws_region()`.

**Returns:**
- boto3 Glue client.

**Raises:**
- Exception if Glue client cannot be created.

**Example:**
```python
client = GlueJobStatsExtractor.get_glue_client('us-east-1')
```

