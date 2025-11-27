# glue_utils.py

## Overview

This module provides utility functions for AWS Glue operations, including client creation, AWS secret retrieval,
database authentication, S3 file operations, Glue job execution, and workflow parameter handling. It abstracts common
Glue and AWS service interactions into reusable helper functions.

## Function Index
1. `generate_db_auth_token()`
2. `get_secret_values()`
3. `get_secret_values_from_cross_account()`
4. `read_file_from_s3()`
5. `get_current_job_attached_iam_role()`
6. `get_current_job_attached_connections()`
7. `get_value_for_job_argument()`
8. `get_job_timeout_value()`
9. `get_connection_detail()`
10. `copy_file_from_local_to_s3()`
11. `copy_file_from_s3_to_local()`
12. `get_current_glue_workflow_run_id()`
13. `get_current_glue_workflow_name()`
14. `get_value_for_workflow_argument()`
15. `execute_glue_job()`

---

## Functions

### generate_db_auth_token()

**Purpose:**  
Generates an AWS RDS database authentication token for IAM-based DB authentication (temporary credentials).

**Args:**

- `executor` (object): The executor object.
- `host_name` (str): RDS database hostname.
- `port` (int): Database port number.
- `user` (str): Database username.
- `aws_region` (str): AWS region where the RDS instance is located.

**Returns:**

- `str`: An authentication token (valid for 15 minutes).

**Raises:**

- Exception from boto3 RDS client if token generation fails.

**Example:**

```python
token = generate_db_auth_token(executor, "mydb.c12345.us-east-1.rds.amazonaws.com", 5432, "admin", "us-east-1")
```

---

### get_secret_values()

**Purpose:**  
Retrieves secrets from AWS Secrets Manager and returns a dictionary of connection details indexed
by `{DB_TYPE}_{SECRET_NAME}`.

**Args:**

- `executor` (object): The executor object.
- `secrets` (list): List of dictionaries with keys `'Secret'` (secret name) and `'dbType'` (database type).
- `aws_region` (str, optional): AWS region for Secrets Manager.

**Returns:**

- `dict`: Dictionary mapping `{DB_TYPE}_{SECRET_NAME}` to connection details (parsed JSON).

**Raises:**

- None explicitly; errors are logged but exceptions propagate.

**Example:**

```python
secrets = [{'Secret': 'prod/postgres/creds', 'dbType': 'postgresql'}]
conn_dict = get_secret_values(executor, secrets)
# Output: {'POSTGRESQL_prod/postgres/creds': {'username': 'user', 'password': 'pwd', ...}}
```

---

### get_secret_values_from_cross_account()

**Purpose:**  
Retrieves secrets from a cross-account AWS Secrets Manager using role assumption (STS).

**Args:**

- `executor` (object): The executor object.
- `secrets` (list): List of dictionaries with `'Secret'` and `'dbType'` keys.
- `role_arn` (str): ARN of the IAM role to assume in the other account.
- `aws_region` (str, optional): AWS region.

**Returns:**

- `dict`: Dictionary mapping `{DB_TYPE}_{SECRET_NAME}` to connection details.

**Raises:**

- Exception from STS or Secrets Manager if role assumption or secret retrieval fails.

**Example:**

```python
role_arn = "arn:aws:iam::123456789012:role/CrossAccountRole"
secrets = [{'Secret': 'remote/db/creds', 'dbType': 'redshift'}]
conn_dict = get_secret_values_from_cross_account(executor, secrets, role_arn)
```

---

### read_file_from_s3()

**Purpose:**  
Reads the contents of a file from S3 and returns it as a string.

**Args:**

- `executor` (object): The executor object.
- `file_path` (str): S3 file path (e.g., `"s3://bucket-name/path/to/file.txt"`).

**Returns:**

- `str`: The file contents as a string.

**Raises:**

- Exception from S3 client if file does not exist or cannot be read.

**Example:**

```python
content = read_file_from_s3(executor, "s3://my-bucket/config.json")
```

---

### get_current_job_attached_iam_role()

**Purpose:**  
Retrieves the IAM role attached to the current Glue job.

**Args:**

- `executor` (object): The executor object.

**Returns:**

- `str`: IAM role ARN attached to the job.

**Raises:**

- Exception if job name cannot be retrieved or Glue API call fails.

**Example:**

```python
role = get_current_job_attached_iam_role(executor)
print(role)  # Output: "arn:aws:iam::123456789012:role/GlueJobRole"
```

---

### get_current_job_attached_connections()

**Purpose:**  
Retrieves a list of Glue connections attached to the current Glue job.

**Args:**

- `executor` (object): The executor object.

**Returns:**

- `list`: List of connection names attached to the job.

**Raises:**

- Exception if job information cannot be retrieved.

**Example:**

```python
connections = get_current_job_attached_connections(executor)
print(connections)  # Output: ['postgres_conn', 'redshift_conn']
```

---

### get_value_for_job_argument()

**Purpose:**  
Retrieves a Glue job argument by name. Supports both built-in Glue arguments and custom arguments.

**Args:**

- `arg_name` (str): The argument name to retrieve.
- `is_glue_inbuilt_arg` (bool, optional): If `True`, treats as built-in Glue argument; if `False`, custom argument.
  Default: `False`.
- `default_value` (any, optional): Default value if argument not found. Default: `None`.

**Returns:**

- The argument value if found; otherwise, the default value.

**Raises:**

- No explicit exceptions raised; returns default if not found.

**Example:**

```python
job_name = get_value_for_job_argument('JOB_NAME')
output_path = get_value_for_job_argument('output_path', default_value='/tmp/output')
```

---

### get_job_timeout_value()

**Purpose:**  
Retrieves the timeout value for the current Glue job run (in minutes).

**Args:**

- `executor` (object): The executor object.

**Returns:**

- `int`: Timeout in minutes. Default: `480` (8 hours) if not explicitly set.

**Raises:**

- Exception if job name or run ID cannot be retrieved.

**Example:**

```python
timeout = get_job_timeout_value(executor)
print(timeout)  # Output: 480
```

---

### get_connection_detail()

**Purpose:**  
Retrieves detailed connection information from a Glue connection, including handling secrets stored in Secrets Manager.

**Args:**

- `executor` (object): The executor object.
- `connection_name` (str): Name of the Glue connection.
- `db_type` (str): Database type for secret lookup.
- `secret_values` (dict): Pre-loaded secret values dictionary.

**Returns:**

- `dict`: Connection details with keys `'url'`, `'user'`, `'password'`.

**Raises:**

- Exception if connection not found or secret retrieval fails.

**Example:**

```python
conn = get_connection_detail(executor, 'my_postgres_conn', 'postgresql', secret_values)
print(conn['url'])  # Output: "jdbc:postgresql://host:5432/db"
```

---

### copy_file_from_local_to_s3()

**Purpose:**  
Uploads/Copy a file from the local filesystem to S3.

**Args:**

- `executor` (object): The executor object.
- `local_path` (str): Local file path to upload.
- `s3_location` (str): S3 location (e.g., `"s3://bucket-name/path/file.txt"`).

**Returns:**

- None.

**Raises:**

- Exception from S3 client if upload fails.

**Example:**

```python
copy_file_from_local_to_s3(executor, "/tmp/data.csv", "s3://my-bucket/uploads/data.csv")
```

---

### copy_file_from_s3_to_local()

**Purpose:**  
Downloads/Copy a file from S3 to the local filesystem.

**Args:**

- `executor` (object): The executor object.
- `s3_location` (str): S3 file location (e.g., `"s3://bucket-name/path/file.txt"`).
- `local_path` (str): Local destination path.

**Returns:**

- None.

**Raises:**

- Exception from S3 client if download fails.

**Example:**

```python
copy_file_from_s3_to_local(executor, "s3://my-bucket/data.csv", "/tmp/data.csv")
```

---

### get_current_glue_workflow_run_id()

**Purpose:**  
Retrieves the current Glue workflow run ID from job arguments.

**Args:**

- None.

**Returns:**

- `str` or `None`: The workflow run ID if available; `None` otherwise.

**Raises:**

- No explicit exceptions.

**Example:**

```python
workflow_run_id = get_current_glue_workflow_run_id()
```

---

### get_current_glue_workflow_name()

**Purpose:**  
Retrieves the current Glue workflow name from job arguments.

**Args:**

- None.

**Returns:**

- `str` or `None`: The workflow name if available; `None` otherwise.

**Raises:**

- No explicit exceptions.

**Example:**

```python
workflow_name = get_current_glue_workflow_name()
```

---

### get_value_for_workflow_argument()

**Purpose:**  
Retrieves a workflow parameter value from the current Glue workflow run properties.

**Args:**

- `executor` (object): The executor object.
- `arg_name` (str): The parameter name to retrieve.
- `default_value` (any, optional): Default value if parameter not found. Default: `None`.

**Returns:**

- The parameter value if found; otherwise, the default value.

**Raises:**

- No explicit exceptions; returns default if workflow info unavailable.

**Example:**

```python
schema = get_value_for_workflow_argument(executor, 'schema_name', default_value='public')
```

---

### execute_glue_job()

**Purpose:**  
Starts a Glue job and optionally waits for it to complete. Returns the final job state.

**Args:**

- `executor` (object): The executor object.
- `job_name` (str): Name of the Glue job to execute.
- `job_args` (dict, optional): Dictionary of job arguments to pass. Default: `{}`.
- `wait_for_end` (bool, optional): If `True`, polls until job completes. Default: `True`.
- `poll_time` (int, optional): Polling interval in seconds. Default: `10`.

**Returns:**

- `str` or `None`: Final job state if `wait_for_end=True` (e.g., `'SUCCEEDED'`, `'FAILED'`); `None`
  if `wait_for_end=False`.

**Raises:**

- Exception from Glue client if job cannot be started.

**Example:**

```python
state = execute_glue_job(executor, 'my-etl-job', job_args={'--input': 's3://bucket/data'}, wait_for_end=True)
print(state)  # Output: 'SUCCEEDED'
```

**Notes:**

- Possible job states: `'SUCCEEDED'`, `'FAILED'`, `'STOPPED'`, `'TIMEOUT'`, `'RUNNING'`, etc.
- Polling continues until one of the terminal states is reached.

___
