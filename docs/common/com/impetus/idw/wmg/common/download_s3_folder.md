# download_s3_folder.py

## File Overview

This module provides utility functions for downloading S3 folders, zipping directories, uploading files to S3, and batch
downloading/zipping S3 bucket contents. It uses AWS CLI commands through subprocess and boto3 for S3 operations.

## Method Index

1. `download_s3_folder()`
2. `zip_dir()`
3. `upload_file()`
4. `zip_and_download_bucket()`

---

## Methods

### 1. download_s3_folder()

**Purpose:**  
Downloads files from an S3 path to a local directory using AWS CLI (`aws s3 cp` command). Optionally supports recursive
download of entire folders.

**Args:**

- `s3_path` (str): The S3 path from which to download (e.g., `s3://bucket-name/folder/`).
- `local_dir` (str): The local directory path where files will be downloaded.
- `recursive` (bool, optional): If `True`, downloads recursively; if `False`, downloads only the specified path.
  Default: `True`.

**Returns:**

- None. Prints status messages to console.

**Raises:**

- No explicit exceptions raised; errors are printed to console via subprocess stderr.

**Example:**

```python
download_s3_folder('s3://my-bucket/data/', '/tmp/data')
# Output: "downloading s3 files from s3://my-bucket/data/ to /tmp/data recursively..."
# Output: "download complete..."
```

**Notes:**

- Uses subprocess to execute AWS CLI commands; requires AWS CLI to be installed and configured.
- Prints download error messages (if any) to console but does not raise exceptions.
- Output and error are printed for debugging purposes.

---

### 2. zip_dir()

**Purpose:**  
Recursively compresses a directory into a ZIP file using the system's `zip` command.

**Args:**

- `folder_path` (str): The absolute path to the folder to compress.
- `zip_file_path` (str): The target path where the ZIP file will be created.

**Returns:**

- None. Prints status messages to console.

**Raises:**

- No explicit exceptions raised; errors are printed via subprocess stderr.

**Example:**

```python
zip_dir('/tmp/my_folder', '/tmp/my_folder.zip')
# Output: "zipping files from /tmp/my_folder to /tmp/my_folder.zip recursively..."
# Output: "zip command complete..."
```

**Notes:**

- Uses the system `zip` command (Unix/Linux/macOS).
- Recursively includes all files and subdirectories.
- Error messages are printed but not raised as exceptions.

---

### 3. upload_file()

**Purpose:**  
Uploads a local file to an S3 bucket using AWS CLI (`aws s3 cp` command).

**Args:**

- `local_file_path` (str): The absolute path to the local file to upload.
- `bucket_name` (str): The name of the S3 bucket (without `s3://` prefix).
- `s3_key_name` (str): The key (path) in S3 where the file will be stored.

**Returns:**

- None. Prints status messages to console.

**Raises:**

- No explicit exceptions raised; errors are printed via subprocess stderr.

**Example:**

```python
upload_file('/tmp/my_folder.zip', 'my-bucket', 'uploads/my_folder.zip')
# Output: "uploading s3 file from /tmp/my_folder.zip to s3://my-bucket/uploads/my_folder.zip recursively..."
# Output: "upload complete..."
```

**Notes:**

- Uses AWS CLI; requires AWS credentials to be configured.
- Output and errors are printed for debugging.

---

### 4. zip_and_download_bucket()

**Purpose:**  
Downloads specified or all folders from an S3 bucket, zips them together, and uploads the resulting ZIP file back to S3.
This is the main orchestration function.

**Args:**

- `bucket_name` (str): The S3 bucket name to download from.
- `include_folders` (list, optional): List of specific folder prefixes to include in the download. If `None` or empty,
  all folders are included. Default: `None`.

**Returns:**

- None. Downloads, zips, and uploads the result to S3.

**Raises:**

- No explicit exceptions caught; boto3 or subprocess errors propagate.

**Example:**

```python
zip_and_download_bucket('my-bucket', include_folders=['folder1/', 'folder2/'])
# Downloads folder1/ and folder2/, zips them, and uploads the ZIP to my-bucket/my-bucket.zip
```

**Notes:**

- Uses boto3 to list S3 objects and boto3/AWS CLI to download and upload.
- Temporary files are stored in `/tmp/` directory.
- ZIP file is named after the bucket (e.g., `my-bucket.zip`).
- Skips folders not in `include_folders` list (if provided); logs which folders are skipped.
- **Known Issue:** Function references `include_folder_names` (undefined) instead of `include_folders` parameter in loop
  condition. This may cause runtime error.

---

## Module-Level Code

The module contains a try-except block at the bottom that
calls `zip_and_download_bucket('bcbs-poc-oregon', include_folders=[])`:

```python
try:
    print('started....')
    include_folder_names = []
    zip_and_download_bucket('bcbs-poc-oregon', include_folders=include_folder_names)
    print('completed...')
except Exception as e:
    print(f"error while running download process: {e}")
    traceback.print_exc()
```

This is the entry point when the script is run directly. It processes the `bcbs-poc-oregon` bucket with no specific
folder filters.

