# WMG Documentation

This is the comprehensive documentation for the **WMG** module. This documentation provides detailed guides for all Python modules within the `com.impetus.idw.wmg` package, including classes, methods, usage examples, and best practices.

---

## Documentation Structure

The WMG module is organized into several functional packages:

- **[Common Utilities](#-common-utilities)** — Core utilities for AWS, Spark, data processing, and framework operations.
- **[Data Access Layer (DAL)](#-data-access-layer-dal)** — Executor and database access patterns.
- **[ELT Processing](#-elt-processing)** — Extract-Load-Transform job orchestration and execution.
- **[Exception Handling](#-exception-handling)** — Exception management and restart mechanisms.
- **[Performance Optimization](#-performance-optimization)** — Spark configuration and table caching strategies.

---

## Common Utilities

Core utilities used across the WMG framework for AWS integration, Spark operations, data validation, and framework initialization.

### AWS & Cloud Services

| Module                                                                        | Purpose |
|-------------------------------------------------------------------------------|---------|
| [aws_secret_manager.md](./com/impetus/idw/wmg/common/aws_secret_manager.md)   | Secure retrieval of secrets from AWS Secrets Manager |
| [aws_utils.md](./com/impetus/idw/wmg/common/aws_utils.md)                                         | General AWS operations and utilities |
| [aws_sqs_utils.md](./com/impetus/idw/wmg/common/aws_sqs_utils.md)                                 | AWS SQS queue operations and message handling |
| [s3_utils.md](./com/impetus/idw/wmg/common/s3_utils.md)                                           | S3 bucket operations, file uploads, downloads, and merges |
| [download_s3_folder.md](./com/impetus/idw/wmg/common/download_s3_folder.md)                       | Bulk download utilities for S3 folders |
| [glue_utils.md](./com/impetus/idw/wmg/common/glue_utils.md)                                       | AWS Glue job management and execution |
| [fetch_glue_job_execution_logs.md](./com/impetus/idw/wmg/common/fetch_glue_job_execution_logs.md) | Retrieve and process Glue job execution logs |

### Database & Data Processing

| Module | Purpose |
|--------|---------|
| [jdbc_utils.md](./com/impetus/idw/wmg/common/jdbc_utils.md) | JDBC connections and database queries |
| [hdfs_utils.md](./com/impetus/idw/wmg/common/hdfs_utils.md) | Hadoop Distributed File System operations |
| [data_migration.md](./com/impetus/idw/wmg/common/data_migration.md) | Data migration utilities and patterns |
| [postgres_cross_account_data_migration.md](./com/impetus/idw/wmg/common/postgres_cross_account_data_migration.md) | PostgreSQL data migration across AWS accounts |
| [redshift_cross_account_data_migration.md](./com/impetus/idw/wmg/common/redshift_cross_account_data_migration.md) | Amazon Redshift data migration across accounts |
| [teradata_object_extraction_util.md](./com/impetus/idw/wmg/common/teradata_object_extraction_util.md) | Teradata database object extraction utilities |
| [postgres_object_extrator_util.md](./com/impetus/idw/wmg/common/postgres_object_extrator_util.md) | PostgreSQL object extraction and schema analysis |
| [redshift_object_extraction_util.md](./com/impetus/idw/wmg/common/redshift_object_extraction_util.md) | Redshift object extraction utilities |

### DDL & Schema Validation

| Module | Purpose |
|--------|---------|
| [ddl_validator.md](./com/impetus/idw/wmg/common/ddl_validator.md) | Validate DDL (Data Definition Language) statements |
| [redshift_ddl_validator.md](./com/impetus/idw/wmg/common/redshift_ddl_validator.md) | Redshift-specific DDL validation |
| [ddl_comparator.md](./com/impetus/idw/wmg/common/ddl_comparator.md) | Compare DDL schemas between source and target systems |
| [td_rs_ddl_comparator.md](./com/impetus/idw/wmg/common/td_rs_ddl_comparator.md) | Compare Teradata and Redshift DDL schemas |

### Data Quality & Transformation

| Module | Purpose |
|--------|---------|
| [codebase_comparator.md](./com/impetus/idw/wmg/common/codebase_comparator.md) | Compare and validate code between environments |
| [semi_structured_data_flattener.md](./com/impetus/idw/wmg/common/semi_structured_data_flattener.md) | Flatten JSON/nested data structures |
| [domain_doc_validator.md](./com/impetus/idw/wmg/common/domain_doc_validator.md) | Validate domain-level documentation |
| [custom_udf.md](./com/impetus/idw/wmg/common/custom_udf.md) | Custom User Defined Functions for data transformation |
| [td_latest_ts_extractor.md](./com/impetus/idw/wmg/common/td_latest_ts_extractor.md) | Extract latest timestamp data from Teradata |
| [test_metdata_queries.md](./com/impetus/idw/wmg/common/test_metdata_queries.md) | Test metadata and query patterns |
| [time_based_job_stats_extractor.md](./com/impetus/idw/wmg/common/time_based_job_stats_extractor.md) | Extract job statistics based on time windows |
| [job_stats_extractor.md](./com/impetus/idw/wmg/common/job_stats_extractor.md) | Extract and analyze job execution statistics |

### Framework Core

| Module | Purpose |
|--------|---------|
| [framework_base.md](./com/impetus/idw/wmg/common/framework_base.md) | Base class for framework components |
| [framework_configurations.md](./com/impetus/idw/wmg/common/framework_configurations.md) | Framework configuration management |
| [parameter_resolver.md](./com/impetus/idw/wmg/common/parameter_resolver.md) | Resolve runtime parameters and variables |
| [execution_manager.md](./com/impetus/idw/wmg/common/execution_manager.md) | Manage query execution and execution context |
| [execution_command.md](./com/impetus/idw/wmg/common/execution_command.md) | Command execution and lifecycle management |
| [execution_task.md](./com/impetus/idw/wmg/common/execution_task.md) | Task definition and execution units |
| [utils.md](./com/impetus/idw/wmg/common/utils.md) | General utility functions and helpers |
| [zip_large_files.md](./com/impetus/idw/wmg/common/zip_large_files.md) | Compress and manage large file archives |

### Spark Data Format Utilities

| Module | Purpose |
|--------|---------|
| [spark/csv_utility.md](./com/impetus/idw/wmg/common/spark/csv_utility.md) | Read and write CSV files with Spark |
| [spark/excel_utility.md](./com/impetus/idw/wmg/common/spark/excel_utility.md) | Read Excel files with Spark DataFrames |
| [spark/fixed_width_utility.md](./com/impetus/idw/wmg/common/spark/fixed_width_utility.md) | Parse fixed-width format files |
| [spark/text_utility.md](./com/impetus/idw/wmg/common/spark/text_utility.md) | Text file processing utilities |
| [spark/xml_utility.md](./com/impetus/idw/wmg/common/spark/xml_utility.md) | Read and write XML files with Spark |

---

## Data Access Layer (DAL)

Provides abstraction and factory patterns for database executors and query execution.

| Module | Purpose |
|--------|---------|
| [executor_dal.md](./dal/executor_dal.md) | Executor data access patterns and interfaces |
| [glue_executor_dal.md](./dal/glue_executor_dal.md) | AWS Glue-based executor implementation |

---

## ELT Processing

Core modules for orchestrating Extract-Load-Transform jobs and data pipelines.

| Module | Purpose |
|--------|---------|
| [elt_process.md](./elt/elt_process.md) | Main ELT orchestration and workflow engine |
| [data_processing_step.md](./elt/data_processing_step.md) | Individual data processing step definitions |
| [glue_elt_step.md](./elt/glue_elt_step.md) | AWS Glue-specific ELT step implementations |

---

## Exception Handling

Comprehensive exception management and restart mechanisms for resilient job execution.

| Module | Purpose |
|--------|---------|
| [eltjob_exception.md](./exception/eltjob_exception.md) | Base ELT job exception class |
| [restartable_exception.md](./exception/restartable_exception.md) | Exception wrapper that enables job restart at specific steps |
| [exception_persistence_handler.md](./exception/exception_persistence_handler.md) | Abstract base class for exception persistence strategies |
| [exception_persistence_handler_factory.md](./exception/exception_persistence_handler_factory.md) | Factory for creating persistence handler instances |
| [local_persistence_handler.md](./exception/local_persistence_handler.md) | Local JSON file-based exception persistence |
| [hdfspersistence_handler.md](./exception/hdfspersistence_handler.md) | HDFS-based exception persistence |
| [spark_based_persistence_handler.md](./exception/spark_based_persistence_handler.md) | Spark DataFrame-based exception persistence |

---

## Performance Optimization

Configuration management for Spark optimization and intelligent table caching.

| Module | Purpose |
|--------|---------|
| [performance.md](./performance.md) | Spark configuration management and table caching strategies |

---
