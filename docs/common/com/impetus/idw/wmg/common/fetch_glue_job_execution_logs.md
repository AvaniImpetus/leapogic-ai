# fetch_glue_job_execution_logs.py

## Overview

This module provides utilities to fetch Glue job execution logs from CloudWatch, store them in S3, and manage retention
of historical logs. It retrieves both output and error logs for a specified Glue job run and archives them with
timestamped filenames for easy tracking and cleanup.

## Function Index

1. `get_latest_glue_run()`
2. `get_log_streams()`
3. `fetch_all_logs()`
4. `upload_logs_to_s3()`
5. `cleanup_old_runs()`
6. `delete_s3_folder()`
7. `main()`

---

## Module-Level Configuration

```python
GLUE_JOB_NAME = gu.get_value_for_job_argument('GLUE_JOB_NAME')
S3_BUCKET_NAME = gu.get_value_for_job_argument('S3_BUCKET_NAME')
KEEP_LAST_N_RUNS = 5  # Keep only the latest N job runs
```

**AWS Clients Initialized:**

- `glue_client`: Boto3 Glue client for job operations.
- `logs_client`: Boto3 CloudWatch Logs client for log retrieval.
- `s3_client`: Boto3 S3 client for file operations.

**CloudWatch Log Groups:**

- `OUTPUT_LOG_GROUP = "/aws-glue/jobs/output"`
- `ERROR_LOG_GROUP = "/aws-glue/jobs/error"`

---

## Functions

### get_latest_glue_run()

**Purpose:**  
Fetches the most recent Glue job run (including failed runs) and returns its metadata.

**Args:**

- None.

**Returns:**

- `dict` or `None`: A dictionary containing job run details (including "Id" field) if a run exists; `None` if no runs
  are found or if the latest run lacks an "Id" field.

**Raises:**

- No explicit exceptions; boto3 errors propagate.

**Example:**

```python
latest_run = get_latest_glue_run()
if latest_run:
    job_run_id = latest_run["Id"]
    print(f"Latest run: {job_run_id}")
```

**Notes:**

- Uses `glue_client.get_job_runs()` with `MaxResults=1` to fetch only the latest run.
- Returns the run as-is without filtering on job status (includes failed/succeeded runs).

---

### get_log_streams(log_group, job_run_id)

**Purpose:**  
Retrieves all log stream names from a CloudWatch log group that match a specific job run ID.

**Args:**

- `log_group` (str): CloudWatch log group name (e.g., `/aws-glue/jobs/output`).
- `job_run_id` (str): The Glue job run ID to filter by.

**Returns:**

- `list`: List of log stream names matching the filter. Empty list if none found or on error.

**Raises:**

- No exceptions raised; errors are caught and logged to console.

**Example:**

```python
streams = get_log_streams("/aws-glue/jobs/output", "jr-12345")
print(f"Found {len(streams)} output log streams")
```

**Notes:**

- Uses `logStreamNamePrefix` to filter log streams by job run ID.
- Prints count of found streams and any errors to console.
- Returns empty list on exception.

---

### fetch_all_logs(log_group, log_streams)

**Purpose:**  
Retrieves all log events (messages) from a list of CloudWatch log streams.

**Args:**

- `log_group` (str): CloudWatch log group name.
- `log_streams` (list): List of log stream names to fetch from.

**Returns:**

- `list`: List of log message strings (one per event).

**Raises:**

- No explicit exceptions; boto3 errors propagate.

**Example:**

```python
logs = fetch_all_logs("/aws-glue/jobs/output", ["stream-1", "stream-2"])
print(f"Fetched {len(logs)} log messages")
```

**Notes:**

- Iterates through each log stream and calls `get_log_events()` with `startFromHead=True` to retrieve events in
  chronological order.
- Extracts the "message" field from each event.
- Aggregates all messages into a single list.

---

### upload_logs_to_s3(logs_data, s3_folder, log_type)

**Purpose:**  
Uploads a collection of log messages to S3 as a timestamped log file.

**Args:**

- `logs_data` (list): List of log message strings to upload.
- `s3_folder` (str): S3 folder path (prefix) where the log file will be stored (e.g., `glue-logs/job-name/run-id/`).
- `log_type` (str): Type of log (e.g., "output", "error") used in the filename for differentiation.

**Returns:**

- None. Uploads file as a side effect.

**Raises:**

- No exceptions raised; errors are caught and logged to console.

**Example:**

```python
upload_logs_to_s3(output_logs, "glue-logs/my-job/jr-12345/", "output")
# Uploads to s3://bucket/glue-logs/my-job/jr-12345/output_20231215_143022.log
```

**Notes:**

- Skips upload if `logs_data` is empty and prints a skip message.
- Generates timestamped filename: `{log_type}_{YYYYMMDD_HHMMSS}.log`.
- Joins all log messages with newlines before uploading.
- Prints the S3 path and any errors to console.
- Errors are caught but not raised.

---

### cleanup_old_runs()

**Purpose:**  
Deletes old Glue job run folders from S3, keeping only the latest N runs (defined by `KEEP_LAST_N_RUNS`).

**Args:**

- None.

**Returns:**

- None. Deletes S3 folders as a side effect.

**Raises:**

- No explicit exceptions; S3 errors may propagate.

**Example:**

```python
cleanup_old_runs()
# Keeps the latest 5 runs; deletes older folders
```

**Notes:**

- Lists S3 prefixes (folders) under `glue-logs/{GLUE_JOB_NAME}/`.
- Sorts folders in reverse chronological order (newest first).
- Deletes folders beyond the `KEEP_LAST_N_RUNS` limit.
- Calls `delete_s3_folder()` for each old folder.
- Prints deletion messages to console.

---

### delete_s3_folder(folder_prefix)

**Purpose:**  
Recursively deletes all objects within an S3 folder (prefix).

**Args:**

- `folder_prefix` (str): S3 folder path (prefix) to delete, including trailing slash if applicable.

**Returns:**

- None. Deletes objects as a side effect.

**Raises:**

- No explicit exceptions; S3 errors may propagate.

**Example:**

```python
delete_s3_folder("glue-logs/my-job/old-run/")
```

**Notes:**

- Lists all objects under the prefix using `list_objects_v2()`.
- Deletes all objects in one batch using `delete_objects()`.
- Prints deletion confirmation to console.
- Silent if folder is empty (no "Contents" key in response).

---

### main()

**Purpose:**  
Main orchestration function that coordinates log fetching and cleanup. Performs cleanup of old runs, fetches the latest
job run's logs, retrieves output and error streams, and uploads all logs to S3.

**Args:**

- None.

**Returns:**

- None (or implicitly exits if no runs are found).

**Raises:**

- No explicit exceptions; errors from called functions may propagate.

**Example:**

```python
main()
# Fetches latest Glue job logs and archives them to S3
```

**Notes:**

- Execution flow:
    1. Calls `cleanup_old_runs()` to delete old run folders.
    2. Calls `get_latest_glue_run()` to get the latest job run.
    3. If no run found, prints message and returns early.
    4. Calls `get_log_streams()` for both output and error log groups.
    5. Calls `fetch_all_logs()` for both groups.
    6. Defines S3 folder structure: `glue-logs/{GLUE_JOB_NAME}/{job_run_id}/`.
    7. Calls `upload_logs_to_s3()` for both output and error logs.
- Useful as an entry point for scheduled tasks or Glue job post-processing.

---
