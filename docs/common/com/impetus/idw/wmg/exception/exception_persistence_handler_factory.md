## exception_persistence_handler_factory.py

Overview

A minimal factory for registering and retrieving persistence handler classes by name. The project registers concrete
handlers (e.g., local, HDFS, Spark-based) and then obtains instances via this factory.

Class index

- ExceptionPersistenceHandlerFactory

Method index

- register_handler(name, clazz)
- get_instance(name, *args, **kwargs)

---

### ExceptionPersistenceHandlerFactory

Purpose

Provide a central registry for exception persistence handler classes so other parts of the application can request an
instance by name without hard-coded imports.

Methods

- register_handler(name, clazz)
    - Purpose: Register a handler class under a string key.
    - Args:
        - name (str): The key to register the handler under.
        - clazz (class): The handler class (not an instance). It should be instantiable with the arguments passed later
          to get_instance.
    - Returns: None
    - Notes: This stores the mapping in an internal dictionary.

- get_instance(name, *args, **kwargs)
    - Purpose: Return a new instance of the handler registered under `name`.
    - Args:
        - name (str): Registered key.
        - *args, **kwargs: Forwarded to the handler's constructor.
    - Returns: A new instance of the registered handler class.
    - Raises: KeyError if `name` is not registered (or a stack trace depending on Utils.get_instance behavior).
    - Notes: This method uses `com.impetus.idw.wmg.common.utils.get_instance` to create the instance. That helper
      handles instantiation semantics and may provide logging or special argument handling.

Example

- Registering a handler:

  ExceptionPersistenceHandlerFactory.register_handler('local', LocalPersistencehandler)

- Retrieving an instance:

  handler = ExceptionPersistenceHandlerFactory.get_instance('local', exceptionPersistencePath='/tmp', executor=executor)

Notes

- This factory is intentionally simple. It does not perform any type verification when registering. Good practice:
  register only classes that subclass ExceptionPersistenceHandler.
