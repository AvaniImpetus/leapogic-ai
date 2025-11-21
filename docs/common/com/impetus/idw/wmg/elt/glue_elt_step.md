# glue_elt_step.py

## Overview

The `glue_elt_step.py` module provides `GlueELTStep`, a concrete subclass of `DataProcessingStep` tailored for AWS
Glue-based ELT jobs. It contains Glue-specific initialization, parameter resolution, secret management, timeout
handling, execution orchestration, and utility helpers such as parameter merging, mail notifications, and job resource
analysis.

This class is intended to be subclassed by Glue-specific steps and often serves as the primary execution entry when
running Glue jobs within this framework.

---

## Method Index (brief)

- `execute(executor_dal=None, restart_hierarchy=None, **kwargs)` - Glue-specific orchestrator that prepares parameters,
  initializes secrets, sets timeouts, and runs `executeFlow()`
- `executeFlow(executor, *args, **kwargs)` - Abstract method (override in subclass)
- `send_mail(from_email, to_email, subject, body, attachment_path=None, msg_type='plain')` - Send email via configured
  SMTP
- `merge_params(existing_params, new_params)` - Merge JSON/string dicts into a single JSON string
- `get_param_value(key, default_value=None, transformation_name=None)` - Resolve parameter with precedence (runtime ->
  argv -> resolver -> context -> default)
- `extract_from_json_params(key, default_value=None, json_param_key='json_param')` - Extract key from JSON payload
  parameter
- `start(**kwargs)` - Helper to execute with start semantics and resource analysis

---

## Detailed Method Documentation

### execute(executor_dal=None, restart_hierarchy=None, **kwargs)

Purpose:

Main Glue-specific orchestrator. It sets up logging, parameter defaults (
like `max_workers`, `default_timezone`, `s3_temp_location`), initializes the Glue executor if not provided, fetches
secrets, handles timeout logic for start jobs, executes the ETL flow via `executeFlow()`, and persists executed queries
if debug is enabled.

Args:
executor_dal (object or None): executor_dal or glue_executor_dal instance or None. If None, `GlueExecutorDAL` is
instantiated using kwargs.
restart_hierarchy (list): Restart information for recovery (passed to child execution).
**kwargs: Additional run-time options:

- `start_job` (bool): If True, enables timeout enforcement and signals
- `enableDebug` (str): 'YES'/'NO' to force debug mode
- `max_workers` (int): Maximum worker count for Glue
- `default_timezone` (str)
- `s3_temp_location` (str)
- `redshift_iam_role` (str)
- `enable_cross_acc_conn` (bool)
- `cross_acc_conn_role`, `cross_acc_conn_db_type` (if cross-account enabled)

Returns:
None

Raises:
RestartableException: If restart semantics are configured and an exception occurs
Exception: Underlying exceptions are re-raised if restart not configured

Example:

```python
step = MyGlueStep()
step.execute(executor_dal=None, start_job=True, parameter_module='my.app.params')
```

Notes:

- When `start_job=True`, the method can set an alarm using `signal.alarm()` to enforce a maximum runtime;
  the `handle_timeout` static method raises an exception when triggered.
- After successful execution, if executor has `enable_debug` and executed queries recorded, queries are written to S3
  path configured by `executed_queries_path`.

---

### executeFlow(executor, *args, **kwargs)

Purpose:

Abstract placeholder for the main ETL flow; `GlueELTStep` provides a default `pass`. Subclasses should implement their
Glue-specific processing here.

Args:
executor: Glue executor instance
*args, **kwargs: Step-specific parameters

Returns:
None

Raises:
Any exception thrown by subclass logic

Notes:

- Typical implementations will use `self.get_param_value` and `self.executor` to perform reads/writes.

---

### send_mail(from_email, to_email, subject, body, attachment_path=None, msg_type='plain')

Purpose:

Convenience wrapper that delegates to `utils.send_mail` using SMTP server details resolved from parameters. Supports
attachments and message type (plain/html).

Args:
from_email (str): Sender email address
to_email (str or list): Recipient(s)
subject (str): Email subject
body (str): Message body
attachment_path (str, optional): Path to file to attach
msg_type (str): `'plain'` or `'html'`

Returns:
None

Raises:
Any exception raised by `utils.send_mail` or SMTP operations

Example:

```python
step.send_mail('noreply@company.com', 'ops@company.com', 'Job failed', 'See attached log', '/tmp/log.txt')
```

Notes:

- SMTP host/port are resolved via `get_param_value('smtp_server')` and `get_param_value('smtp_port')`.

---

### merge_params(existing_params, new_params)

Purpose:

Merge two parameter payloads that may be provided as JSON strings or dicts. Returns a JSON string representation of the
merged params.

Args:
existing_params (str or dict or None): Existing parameter payload
new_params (str or dict or None): New parameter payload to merge in

Returns:
str: JSON string of merged parameters

Raises:
json.JSONDecodeError if provided strings are not valid JSON

Example:

```python
merged = GlueELTStep.merge_params('{"a":1}', '{"b":2}')  # returns '{"a":1,"b":2}'
```

Notes:

- If inputs are `None`, they are treated as empty dicts.
- Uses `json.loads` for string inputs.

---

### get_param_value(key, default_value=None, transformation_name=None)

Purpose:

Resolve a parameter by precedence:

1. runtime parameters (`self.runtime_params`)
2. job arguments from command line via `get_value_for_job_argument`
3. Parameter resolver (`self.param_resolver`)
4. `self.context_params_dict` (from imported parameter module)
5. `default_value`

Args:
key (str): Parameter key
default_value (any): Default fallback
transformation_name (str, optional): Transformation name for resolver lookups

Returns:
Any: Resolved parameter value or default

Raises:
None

Example:

```python
val = step.get_param_value('max_workers', default_value=4)
```

Notes:

- If value is falsy and executor debug enabled, a warning is logged.

---

### extract_from_json_params(key, default_value=None, json_param_key='json_param')

Purpose:

Extract a key from a JSON-encoded parameter payload stored under another parameter (default: `'json_param'`).

Args:
key (str): Key to extract from JSON
default_value (any): Value to return if key missing
json_param_key (str): Name of parameter containing JSON payload

Returns:
any: Extracted value or default

Example:

```python
v = step.extract_from_json_params('fileName', default_value='NA')
```

Notes:

- Uses `json.loads` on the JSON payload returned by `get_param_value`.

---

### start(**kwargs)

Purpose:

Convenience wrapper to call `execute()` with `start_job=True` and run `analyze_job_resources()` afterwards. Converts
raised exceptions to a compact custom message.

Args:
**kwargs: forwarded to `execute()`

Returns:
None

Raises:
Exception: Reraises a compacted message if an error occurs

Example:

```python
step.start(parameter_module='my.params', start_job=True)
```

Notes:

- Ensures `executor.closeConnection()` is invoked in `finally` if executor is present.

---

## Common Use Cases

- Subclass `GlueELTStep` to implement `executeFlow()` with Glue-specific logic (reading from S3, writing to
  Iceberg/Redshift, etc.)
- Use `init_step_params()` to bootstrap parameters in Glue jobs
- Use `merge_params()` to combine JSON-run parameters
- Use `setMappVars`/`getMappVars` for dynamic mapping variables persisted in metadata table

---
