# aws_sqs_utils.py — SQS Utility Reference

## Overview

This module contains utility functions to interact with AWS SQS (Simple Queue Service). It provides helpers to create an
SQS client, read messages, send single or batched messages, get queue attributes and list queues, delete/purge messages,
and retrieve queue URLs. The functions are designed to work within the project's Glue executor context (they accept
an `executor` parameter used for locking/compatibility) and support optional explicit AWS credentials and region
overrides.

## Methods (brief index)

- `read_message(executor, queue_url, region=None, max_messages=1, wait_time=0, visibility_timeout=None, message_attributes=None, delete_after_read=False, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None)` —
  Read (receive) messages from a queue; optionally delete after read.
- `send_message(executor, queue_url, message_body, region=None, delay_seconds=0, message_attributes=None, message_group_id=None, message_deduplication_id=None, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None)` —
  Send one message to a queue (supports FIFO parameters).
- `get_queue_attrs(executor, queue_url, region=None, attribute_names=None, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None)` —
  Retrieve queue attributes and normalize numeric attributes to ints when possible.
- `get_queue_list(executor, region=None, queue_name_prefix=None, max_results=1000, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None)` —
  List queue URLs (optionally filter by prefix) and return structured info.
- `delete_message(executor, queue_url, receipt_handle, region=None, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None)` —
  Delete a message by receipt handle.
- `purge_queue(executor, queue_url, region=None, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None)` —
  Purge all messages from a queue (may take up to 60s to complete on SQS side).
- `send_message_batch(executor, queue_url, messages, region=None, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None)` —
  Send up to many messages in batches of 10 (SQS limit); messages parameter is list of dicts.
- `get_queue_url(executor, queue_name, region=None, queue_owner_aws_account_id=None, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None)` —
  Get queue URL by name (optionally for cross-account queues).

---

## Detailed method reference

### `read_message()`

read_message(executor, queue_url, region=None, max_messages=1, wait_time=0, visibility_timeout=None,
message_attributes=None, delete_after_read=False, aws_access_key_id=None,
aws_secret_access_key=None, aws_session_token=None)

Purpose:

- Receive messages from an SQS queue, parse each message body as JSON when possible, and return a normalized list of
  messages. Optionally deletes messages after reading.

Args:

- executor: object — Glue executor context
- queue_url (str): URL of the SQS queue
- region (str): AWS region (optional)
- max_messages (int): Max messages to fetch (1-10). Values outside are clamped to [1,10].
- wait_time (int): Long-poll wait time in seconds (0-20). Values outside are clamped.
- visibility_timeout (int): Visibility timeout to apply when receiving (seconds)
- message_attributes (list): List of attribute names to request (None means 'All')
- delete_after_read (bool): If True, delete messages from the queue after reading
- aws_access_key_id, aws_secret_access_key, aws_session_token: optional explicit credentials

Returns: dict — shape example:

```
    {
        'Messages': [
            {
                'MessageId': '...',
                'ReceiptHandle': '...',
                'Body': 'raw body string',
                'ParsedBody': {...} or None,
                'MD5OfBody': '...',
                'MessageAttributes': {...},
                'Attributes': {...}
            },
            ...
        ],
        'MessageCount': int,
        'QueueUrl': queue_url,
        'Success': True|False,
        'Error': '...'  # present when Success is False
    }
```

Raises:

- botocore.exceptions.ClientError: If SQS returns an error (caught and returned in result)
- botocore.exceptions.NoCredentialsError: If AWS credentials cannot be found (caught and returned)

Example:

```python    
res = read_message(executor, 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue', max_messages=5)
for m in res['Messages']:
    ...     
    print(m['ParsedBody'])  # parsed JSON or None
```

Notes:

- The function attempts to parse the message body as JSON and stores a `ParsedBody` field.
- If `delete_after_read` is True the function will call `delete_message` for each message; deletion failures are logged
  but do not stop processing.
- On exception the function returns a dict with Success=False and an Error string rather than raising.

---

### `send_message()`

send_message(executor, queue_url, message_body, region=None, delay_seconds=0, message_attributes=None,
message_group_id=None, message_deduplication_id=None, aws_access_key_id=None, aws_secret_access_key=None,
aws_session_token=None)

Purpose:

- Send a single message to an SQS queue. Accepts either a string body or a dict (which will be JSON-encoded). Supports
  FIFO queue fields `MessageGroupId` and `MessageDeduplicationId`.

Args:

- executor: object — Glue executor context
- queue_url (str): URL of the SQS queue
- message_body (str|dict): Message payload; dicts are JSON-encoded
- region (str): AWS region (optional)
- delay_seconds (int): Delay before message becomes visible (0-900)
- message_attributes (dict): Attributes mapping name -> value (helper converts to SQS format)
- message_group_id (str): FIFO queue group id (optional)
- message_deduplication_id (str): FIFO dedupe id (optional)
- aws_access_key_id, aws_secret_access_key, aws_session_token: optional explicit credentials

Returns:
dict — shape example:

```
    {
        'MessageId': '...',
        'MD5OfBody': '...',
        'MD5OfMessageAttributes': '...',
        'SequenceNumber': '...',  # for FIFO
        'QueueUrl': queue_url,
        'Success': True|False,
        'Error': '...'  # present when Success is False
    }
```

Raises:

- ClientError: If SQS send fails (caught and returned)
- NoCredentialsError: If credentials missing (caught and returned)

Example:

```python   
send_message(executor, queue_url, {'event': 'user_signup', 'id': 123})
```

Notes:

- `message_attributes` accepts Python primitives and will convert them into SQS `MessageAttributes` values with
  appropriate DataType.
- For binary attributes, pass a `bytes` value — it will be sent as `BinaryValue`.
- For non-string, non-numeric values the helper JSON-encodes them and sends as a string attribute.

---

### `get_queue_attrs()`

get_queue_attrs(executor, queue_url, region=None, attribute_names=None,
aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None)

Purpose:

- Retrieve queue attributes and convert commonly numeric attributes to Python ints when possible.

Args:

- executor: object — Glue executor context
- queue_url (str): URL of the queue
- region (str): AWS region (optional)
- attribute_names (list): Specific attribute names to fetch (defaults to ['All'])
- aws_access_key_id, aws_secret_access_key, aws_session_token: optional credentials

Returns:
dict — example:

```
    {
        'Attributes': { 'ApproximateNumberOfMessages': 5, 'VisibilityTimeout': 30, ... },
        'QueueUrl': queue_url,
        'AttributeCount': int,
        'Success': True|False,
        'Error': '...'  # when Success False
    }
```

Raises:

- ClientError: If SQS returns an error (caught and returned)
- NoCredentialsError: If credentials missing (caught and returned)

Example:

```python
    attrs = get_queue_attrs(executor, queue_url)
    print(attrs['Attributes']['ApproximateNumberOfMessages'])
```

Notes:

- A list of `numeric_attrs` is used to convert those values to ints; if conversion fails the original string is
  returned.
- This function always returns a normalized dict with Success flag; it does not raise directly for AWS errors.

---

### `get_queue_list()`

get_queue_list(executor, region=None, queue_name_prefix=None, max_results=1000,
aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None)

Purpose:

- List SQS queues in the account (optionally filtered by name prefix). Returns a list of queue URLs and a `QueueInfo`
  list with extracted names.

Args:

- executor: object — Glue executor context
- region (str): AWS region (optional)
- queue_name_prefix (str): Filter queues by name prefix (optional)
- max_results (int): Maximum number of results (clamped to 1000)
- aws_access_key_id, aws_secret_access_key, aws_session_token: optional credentials

Returns:

```
    dict — example:
    {
        'Queues': [{'QueueName': '...', 'QueueUrl': '...', 'Region': '...'}, ...],
        'QueueUrls': [...],
        'QueueCount': int,
        'Region': '...' ,
        'Success': True|False,
        'Error': '...'  # when Success False
    }
```

Raises:

- ClientError: If SQS list operation fails (caught and returned)
- NoCredentialsError: If credentials missing (caught and returned)

Example:

```python
qinfo = get_queue_list(executor, queue_name_prefix='prod-')
for q in qinfo['Queues']:
    ...     
    print(q['QueueName'], q['QueueUrl'])
```

Notes:

- The function clamps `max_results` to 1000 because the SQS API supports at most 1000 results in one call.
- QueueName is derived from the last path segment of each QueueUrl.

---

### `delete_message()`

delete_message(executor, queue_url, receipt_handle, region=None,
aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None)

Purpose:

- Delete a single message from a queue using its receipt handle.

Args:

- executor: object — Glue executor context
- queue_url (str): URL of the queue
- receipt_handle (str): Receipt handle obtained from receive_message
- region, aws_access_key_id, aws_secret_access_key, aws_session_token: optional

Returns:
dict — example:

```
{ 
    'QueueUrl': queue_url, 
    'ReceiptHandle': receipt_handle, 
    'Success': True 
} 
```

Raises:

- ClientError: If SQS deletion fails (caught and returned)
- NoCredentialsError: If credentials missing (caught and returned)

Example:

```python
delete_message(executor, queue_url, receipt_handle)
```

Notes:

- The function logs success and returns a consistent dict; errors are returned rather than raised.

---

### `purge_queue()`

purge_queue(executor, queue_url, region=None,
aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None)

Purpose:

- Purge all messages from an SQS queue. Note: AWS may take up to 60 seconds to fully clear the queue.

Args:

- executor: object — Glue executor context
- queue_url (str): URL of the queue to purge
- region, aws_access_key_id, aws_secret_access_key, aws_session_token: optional

Returns:
dict — example:

```
{ 
    'QueueUrl': queue_url, 
    'Success': True, 
    'Message': 'Queue purge initiated...' 
}
```

Raises:

- ClientError: If purge fails (caught and returned)
- NoCredentialsError: If credentials missing (caught and returned)

Example:

```
purge_queue(executor, queue_url)
```

Notes:

- Purge is a destructive operation; use with caution. Purging a queue that has a redrive policy or is FIFO still removes
  messages.
- AWS throttles purge operations; you can only purge a queue once every 60 seconds.

---

### `send_message_batch()`

send_message_batch(executor, queue_url, messages, region=None,
aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None)

Purpose:

- Send a list of messages to SQS in batches of up to 10 messages (SQS API limit per batch). Each message in `messages`
  must be a dict with at least `MessageBody` and an `Id` or the function will assign one.

Args:

- executor: object — Glue executor context
- queue_url (str): URL of the SQS queue
- messages (list[dict]):
    - Example message dict:
    ```
    { 
        'Id': 'msg1', 
        'MessageBody': 'text' , 
        'DelaySeconds': 0, 
        'MessageAttributes': {...} 
    }
    ```
- region, aws_access_key_id, aws_secret_access_key, aws_session_token: optional

Returns:
dict — example:

```
    {
        'Successful': [...],
        'Failed': [...],
        'SuccessfulCount': int,
        'FailedCount': int,
        'QueueUrl': queue_url,
        'Success': True|False,
        'Error': '...'  # when Success False
    }
```

Raises:

- ClientError: If batch send fails (caught and returned)
- NoCredentialsError: If credentials missing (caught and returned)

Example:

```python
    batch = [
    ...   {'Id': '1', 'MessageBody': {'event': 'a'}},
    ...   {'Id': '2', 'MessageBody': 'plain text'}
    ... ]
    send_message_batch(executor, queue_url, batch)
```

Notes:

- The function automatically JSON-encodes dict message bodies.
- It collects `Successful` and `Failed` entries returned by SQS and returns aggregated counts.
- It handles messages in batches of 10.

---

### `get_queue_url()`

get_queue_url(executor, queue_name, region=None, queue_owner_aws_account_id=None,
aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None)

Purpose:

- Resolve an SQS queue name into its full QueueUrl. Useful when you only know the queue name and need the URL to act on
  it.

Args:

- executor: object — Glue executor context
- queue_name (str): Name of the SQS queue
- region (str): AWS region (optional)
- queue_owner_aws_account_id (str): Optional account ID when accessing a queue owned by another account
- aws_access_key_id, aws_secret_access_key, aws_session_token: optional

Returns:
dict — example:

```
    { 
        'QueueUrl': 'https://sqs....', 
        'QueueName': queue_name, 
        'Success': True 
    }
```

Raises:

- ClientError: If SQS get_queue_url fails (caught and returned)
- NoCredentialsError: If credentials missing (caught and returned)

Example:

```    
get_queue_url(executor, 'my-queue')
```

Notes:

- For cross-account queues provide `queue_owner_aws_account_id`.
- If the queue does not exist the function returns Success=False and an Error string rather than raising.

---