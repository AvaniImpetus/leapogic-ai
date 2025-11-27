# GlueJobStatsExtractor Class

## Overview
The `GlueJobStatsExtractor` class is a utility for extracting and reporting execution statistics from AWS Glue jobs. It retrieves details such as start/end times, execution duration, capacity allocation, worker configurations, and DPU consumption from the last successful run of specified jobs. The class supports analysis of single or multiple jobs and can generate Excel reports. It does not inherit from any base class but uses Glue utilities for client creation and parameter retrieval.

## What It Does
- Analyzes AWS Glue job runs to extract performance and execution metrics.
- Identifies the last successful run for each job and pulls detailed statistics.
- Supports processing of the current job (when no arguments provided), a single job, or multiple jobs (via comma-separated string or list).
- Prints formatted statistics to the console and optionally saves them to an Excel file.
- Provides insights into job efficiency, resource usage, and execution times for monitoring and optimization.

## Arguments
The primary method `extract()` accepts the following argument:

- `job_name` (str, list, set, optional): Specifies the job(s) to analyze. 
  - If `None` (default), analyzes the current Glue job using parameters from the job arguments.
  - If a string, can be a single job name or comma-separated job names.
  - If a list or set, contains multiple job names.
  No other arguments are required, as the class handles Glue client creation and region detection internally.

## Usage
The `GlueJobStatsExtractor` class is used for post-execution analysis of Glue jobs to gather metrics for reporting, troubleshooting, or performance tuning. It is particularly useful in ETL pipelines where job statistics need to be logged or exported for compliance or optimization purposes. The output includes key metrics that help assess job performance and resource utilization.

### Key Features
- **Flexible Job Selection**: Supports current job, single job, or batch processing of multiple jobs.
- **Last Successful Run Focus**: Automatically targets the most recent successful execution for accurate metrics.
- **Comprehensive Metrics**: Extracts detailed stats including execution time, DPU seconds, worker types, and capacities.
- **Console and File Output**: Prints stats to console and optionally generates timestamped Excel reports.
- **AWS Integration**: Uses boto3 for Glue API interactions and integrates with Glue utilities for seamless operation.

## Exceptions
- **Exception**: Raised if the job name cannot be determined from job arguments when `job_name` is `None`.
- **Exception**: Raised if the job run ID cannot be found for the current job.
- **boto3 Exceptions**: May occur during Glue API calls, such as `ClientError` for access denied, invalid job names, or network issues.
- **pandas Exceptions**: If saving reports, exceptions from DataFrame operations (e.g., file write failures) may propagate.
- General exceptions from Glue client creation or region detection if AWS configurations are incorrect.

## How to Use It
1. Instantiate the `GlueJobStatsExtractor` class.
2. Call the `extract()` method with optional `job_name`:
   - For current job: `extractor.extract()`
   - For a single job: `extractor.extract('my-glue-job')`
   - For multiple jobs: `extractor.extract(['job1', 'job2'])` or `extractor.extract('job1,job2')`
3. View console output for statistics; optionally provide an `output_path` to `get_jobs_detail()` for Excel export.
4. Ensure AWS credentials are configured for Glue access, and run within a Glue environment for automatic parameter resolution.

The class is designed for integration into Glue workflows or standalone scripts for job monitoring.

