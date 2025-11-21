# spark_based_persistence_handler.py

## Overview

A persistence handler that uses Spark DataFrame JSON read/write operations to persist and load restart details. This is
designed for environments where a distributed store (accessible by Spark) is used, for example writing to HDFS or a
cloud storage location supported by Spark.

Class index

- SparkBasedPersistenceHandler

Method index

- persist_exception_details(restartable_exception)
- load_exception_details()

---

## SparkBasedPersistenceHandler

### Purpose

Persist the restart hierarchy by constructing a tiny Spark DataFrame and writing it as JSON. When loading, read the JSON
back into a DataFrame and collect entries.

### Methods

- persist_exception_details(restartable_exception)
    - Purpose: Convert restart hierarchy into a list of dicts and write the data as JSON using Spark's DataFrame writer
      in `mode('overwrite')`.
    - Args:
        - restartable_exception: RestartableException instance.
    - Returns: None
    - Notes: The method times the write and logs the stored details. It creates a DataFrame by parallelizing
      the `details` in the Spark driver and calling `spark.read.json` on that RDD, then writes
      with `df.write.format('json').mode('overwrite').save(self.path)`.

- load_exception_details()
    - Purpose: Read JSON from `self.path` using Spark, collect rows and translate them into
      tuples `(scriptClass, index)` for each entry.
    - Returns: A list of tuples with script class and index, or an empty list on errors.
    - Behavior: After reading, it calls `__SaveDetails('[{"scriptClass":"nonexistent","index":0}]')` which writes a
      sentinel row to the path — this is used to mark the store as cleared/initialized.
    - Exceptions: If Spark read fails due to missing path, the code logs and may raise; other errors are logged and
      re-raised.

### Example:

handler = SparkBasedPersistenceHandler(executor=executor, exceptionPersistencePath='/tmp/app/exceptionDetails')
handler.persist_exception_details(restartable_exception)
items = handler.load_exception_details()

### Notes:

- This handler requires an active SparkSession and access to the configured storage path (HDFS, S3, ADLS, etc.).
- Because the implementation uses Spark to read JSON directly, the underlying storage should contain files that Spark's
  json reader can parse. The write operation uses `mode('overwrite')`, which replaces existing content.
- After loading, the handler writes a sentinel row to avoid reprocessing the same entries. Be aware of this if you
  examine the target path.
- Consider switching to Parquet for more robust schema handling and performance if you expect larger payloads.
