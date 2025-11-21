# data_processing_step.py

## Overview

The `data_processing_step.py` module provides the `DataProcessingStep` class which implements a reusable,
framework-aware step for ELT (Extract-Load-Transform) pipelines. It extends the class `ELTProcess` and adds execution
orchestration, restart/resume support, parameter resolution, and utility wrappers to run one or more job steps.

This class is designed to be subclassed by concrete data processing steps (mapping jobs). A subclass should
implement `executeFlow()` which contains the business logic of the step and can call helper methods
like `executeJobSteps()` to run composed jobs.

### Key responsibilities:

- Resolve runtime and workflow parameters
- Initialize an executor (e.g., Spark-on-Hive) via `ExecutorFactory`
- Support restartable execution and recovery using `restart_hierarchy`
- Provide utilities for tracking/advancing a labeled index for multi-part steps
- Execute single or parallel job tasks using `ExecutionManager` and `ExecutionCommand`
- Provide a `start()` helper for standalone execution

---

## Method Index (brief)

- `execute(executor_dal=None, restart_hierarchy=None, **kwargs)` - Main entry to execute the step; sets up executor,
  parameters, and calls `executeFlow()`
- `start(executorDAL='SparkOnHive', parameter_module=None)` - Convenience method to run the step standalone (typical for
  local/testing)

---

## Detailed Method Documentation

### execute(executor_dal=None, restart_hierarchy=None, **kwargs)

Purpose:
Main entry point to run this step. This method prepares parameters, resolves parameter modules, instantiates or obtains
an executor (via `ExecutorFactory`), applies restart handling logic, and triggers `executeFlow()` implemented by
subclasses.

Args:
executor_dal (str or object, optional): Either a DAL identifier string (e.g. `'SparkOnHive'`) to obtain an executor
via `ExecutorFactory`, or a DAL object with a `newInstance()` method. If not provided, the executor is created from
kwargs.

    restart_hierarchy (list, optional): A list of `(step_name, restart_index)` tuples describing which steps failed and at which index. Used to drive restart/resume behavior.

    **kwargs: Additional runtime parameters and job arguments. Common keys:
        - `workflowName` (str): Name of the workflow
        - `sessionName` (str): Name of the session
        - `mappingName` (str): Name of mapping or step
        - `parameter_module` (str): Fully qualified module name containing parameter definitions
        - `executor` (object): Explicit executor instance (overrides factory)
        - `executorDAL` (str): DAL name used by ExecutorFactory
        - `enableDebug` (str): 'YES'/'NO' string used by framework for debug toggles

Returns:
None

Raises:
RestartableException: If an exception occurs and a restart level is configured (the exception is wrapped to support
restart semantics)
Exception: Re-raises underlying exceptions if restart semantics are not enabled

Example:

```python
# Typical call from orchestration code
step.execute(executor_dal='SparkOnHive', workflowName='DailyWorkflow', sessionName='S1')
```

Notes:

- `execute()` first looks for `parameter_module` as a job argument (via `get_value_for_job_argument`) and then via
  kwargs. If provided, it's imported via `utils.import_param_module` and stored in `self.context_params`.
- The method sets `self.executor` either from kwargs (if provided) or from `ExecutorFactory.get_instance()`.
- If `restart_hierarchy` is provided and `restartLevel()` returns `'index'`, the step attempts to recover `label_index`
  from `getRestartIndex()`.
- On exception, if `restartLevel()` is not `None`, the exception is wrapped and re-raised as `RestartableException` to
  allow orchestrators to persist restart state.

---

### start(executorDAL='SparkOnHive', parameter_module=None)

Purpose:
Convenience entry point to run the step as a standalone script (useful for testing). It sets up basic logging,
calls `setConf()` and `execute()` and logs success/failure.

Args:
executorDAL (str, optional): DAL name to pass to `execute()` or to let `ExecutorFactory` create an executor.
Default: `'SparkOnHive'`.

    parameter_module (str, optional): Fully-qualified parameter module to import and use as context parameters.

Returns:
None

Raises:
Any exception raised by `execute()` will be caught and printed via `traceback.print_exc()`; the method does not
re-raise.

Example:

```python
if __name__ == '__main__':
    step = MyConcreteDataProcessingStep()
    step.start(executorDAL='SparkOnHive', parameter_module='my_app.params')
```

Notes:

- This method configures basic logging using `logging.basicConfig(...)` before calling `execute()`.
- It logs success and failure messages to help during manual runs.

---

## Usage Pattern (Example Concrete Step)

```python
from com.impetus.idw.wmg.elt.data_processing_step import DataProcessingStep

class MyStep(DataProcessingStep):
    def executeFlow(self, executor, **kwargs):
        # Example: read parquet, transform, write
        spark = executor.spark_session
        df = spark.read.parquet('s3://input-bucket/data/')
        # do processing
        processed = df.filter('status = "ACTIVE"')
        processed.write.mode('overwrite').parquet('s3://output-bucket/processed')

if __name__ == '__main__':
    MyStep().start(executorDAL='SparkOnHive', parameter_module='my_app.params')
```

---
