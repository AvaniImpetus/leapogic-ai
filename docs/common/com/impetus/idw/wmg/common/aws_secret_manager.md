# AWS Secret Manager Module Documentation

## Overview
The `AwsSecretManager` class provides a convenient interface for retrieving database connection credentials from AWS Secrets Manager. It implements caching to avoid repeated API calls and supports multiple database types.


### `get_value_for_secret(secret_info)`
**Purpose:** Retrieves database connection information for a specific secret.

**Parameters:**
- `secret_info` (dict, required): Dictionary containing:
  - `'dbType'` (str): Database type (e.g., 'POSTGRES', 'REDSHIFT', 'MYSQL')
  - `'Secret'` (str): Name of the secret in AWS Secrets Manager

**Returns:** Dictionary containing connection options:
- `'url'` (str): Database connection URL
- `'user'` (str): Database username
- `'password'` (str): Database password
- `'driver'` (str): JDBC driver class name
- `'jars'` (str): JDBC driver JAR files

**Exceptions Raised:**
- `KeyError`: If 'Secret' key is missing from secret_info
- `Exception`: Any exception from AWS Secrets Manager API (re-raised)
- `EnvironmentError`: If AWS_DEFAULT_REGION environment variable is not set

**Usage:**
```python
secret_manager = AwsSecretManager()
connection_info = secret_manager.get_value_for_secret({
    'dbType': 'POSTGRES',
    'Secret': 'prod/postgres/credentials'
})
# Returns: {'url': 'jdbc:postgresql://...', 'user': 'dbuser', 'password': 'dbpass', ...}
```

**Description:** Checks cache first, fetches from AWS Secrets Manager if not cached, and returns connection options for database access.

---

### `initialize_secret_connections(secrets, region)`
**Purpose:** Pre-loads multiple secret connections into the cache for improved performance.

**Parameters:**
- `secrets` (list, required): List of dictionaries, each containing:
  - `'Secret'` (str): Name of the secret in AWS Secrets Manager
  - `'dbType'` (str): Database type (e.g., 'POSTGRES', 'REDSHIFT')
- `region` (str, required): AWS region name (e.g., 'us-east-1')

**Returns:** None

**Exceptions Raised:**
- `KeyError`: If any secret dictionary is missing 'Secret' or 'dbType' keys
- `Exception`: Any exception from AWS Secrets Manager API (re-raised)

**Usage:**
```python
secret_manager = AwsSecretManager()
secrets_list = [
    {'dbType': 'POSTGRES', 'Secret': 'prod/postgres/main'},
    {'dbType': 'REDSHIFT', 'Secret': 'prod/redshift/data'}
]
secret_manager.initialize_secret_connections(secrets_list, 'us-east-1')
```

**Description:** Iterates through the list of secrets and caches their connection information to avoid repeated API calls during subsequent `extract_value_for_secret` calls.

---

### `extract_value_for_secret(db_type, region, secret_name)`
**Purpose:** Fetches a secret value from AWS Secrets Manager and caches it internally.

**Parameters:**
- `db_type` (str, required): Database type identifier (automatically uppercased)
- `region` (str, required): AWS region name
- `secret_name` (str, required): Name of the secret in AWS Secrets Manager

**Returns:** None (result is cached internally)

**Exceptions Raised:**
- `Exception`: Any exception from AWS Secrets Manager API (re-raised)
- `json.JSONDecodeError`: If the secret string is not valid JSON
- `KeyError`: If required fields are missing from the secret JSON

**Usage:**
```python
secret_manager = AwsSecretManager()
secret_manager.extract_value_for_secret('POSTGRES', 'us-east-1', 'prod/database/creds')
# Secret is now cached and can be retrieved via get_value_for_secret
```

**Description:** Creates a boto3 client, retrieves the secret value, parses the JSON, extracts connection details, and stores them in the internal cache using a composite key of `{db_type}_{secret_name}`.

---

## Expected Secret JSON Format
AWS Secrets Manager secrets should contain JSON with the following structure:

```json
{
  "username": "database_user",
  "password": "database_password",
  "hosturl": "jdbc:postgresql://hostname:5432/database",
  "driver": "org.postgresql.Driver",
  "jars": "/path/to/postgresql.jar"
}
```

**Alternative field names supported:**
- `user` (can be used instead of `username`)
- `url` (can be used instead of `hosturl`)

---

