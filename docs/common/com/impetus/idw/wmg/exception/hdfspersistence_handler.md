## hdfspersistence_handler.py

Overview

A concrete ExceptionPersistenceHandler that writes and reads restart details to HDFS using the Hadoop FileSystem API
accessed through the Spark JVM bridge. This handler is intended for environments where application metadata is stored on
HDFS (or any Hadoop-supported filesystem).

Class index

- HDFSPersistencehandler

Method index

- persist_exception_details(restartableException)
- load_exception_details()

---

### HDFSPersistencehandler

Purpose

Store and retrieve a small JSON representation of the restart hierarchy on HDFS. The JSON contains a list of entries
with keys like `scriptClass`, `index`, and `runtimeStepConfig`.

Methods

- persist_exception_details(restartableException)
    - Purpose: Convert the RestartableException.restart_hierarchy into a small JSON-friendly structure and persist it to
      HDFS by delegating to `__SaveDetails`.
    - Args:
        - restartableException: instance of RestartableException. The handler verifies the instance before processing.
    - Returns: None
    - Notes: For each hierarchy element the code extracts `scriptClass`, `index`, and `runtime_step_config` (if present)
      and logs what it stores.

- load_exception_details()
    - Purpose: Read the persisted JSON file from HDFS, parse it and convert each entry into a tuple (scriptClass, index,
      runtimeStepConfig). The file is deleted after reading to avoid reusing stale details.
    - Args: None
    - Returns: A list of tuples (scriptClass, index, runtimeStepConfig) or an empty list on error / if file is absent.
    - Notes: The method reads bytes via the Hadoop InputStream and constructs a Python string by reading integer values
      and converting to chars. It logs timings and errors.

Example usage

- Typical construction inside an executor environment:

  handler = HDFSPersistencehandler(exceptionPersistencePath='/user/app/exceptionDetails', executor=executor)
  handler.persist_exception_details(restartable_exception)
  restart_items = handler.load_exception_details()

Notes

- This handler depends on the Spark session and access to the Spark JVM (org.apache.hadoop.*). It will fail outside of a
  Spark runtime.
- The code swallows many exceptions but logs errors; ensure logging is configured to capture failures.
- Because it deletes the file after reading, make sure no other process expects to read the same file later.
- Byte handling uses a simple read loop; large files could be slow. This handler expects small JSON payloads only (a
  handful of entries).
