# download_s3_folder.py

### zip_and_download_bucket()

**What It Does:**  
This function orchestrates the process of downloading specified or all top-level folders from an S3 bucket, compressing them into a single ZIP archive, and uploading the ZIP file back to the same S3 bucket. It is designed for batch archiving or backup of S3 bucket contents, allowing selective inclusion of folders to reduce download size and time.

**Arguments:**  
- `bucket_name` (str, required): The name of the S3 bucket to process. The function lists top-level folders (prefixes) in this bucket.
- `include_folders` (list of str, optional): A list of folder prefixes to include in the download and ZIP. If `None` or empty, all top-level folders are included. Default: `None`.

**Usage:**  
The function is used for creating compressed backups of S3 bucket folders. It first lists all top-level folders using boto3, filters them based on `include_folders`, downloads each included folder recursively to a local temporary directory (`/tmp/{bucket_name}/`), zips the entire downloaded structure, and uploads the resulting ZIP file to the bucket root. This is useful for periodic archiving, data migration, or compliance backups where only certain folders need to be preserved.

**Exceptions:**  
- **boto3 Exceptions**: May occur during S3 operations, such as `ClientError` for access denied, invalid bucket, or network issues when listing objects or accessing the bucket.
- **subprocess Exceptions**: Raised if AWS CLI commands (`aws s3 cp`) fail, e.g., due to missing AWS credentials, permissions, or command execution errors.
- **OSError/FileNotFoundError**: If local directories cannot be created or accessed (e.g., permissions on `/tmp`).
- **Zip Command Errors**: If the `zip` command fails (e.g., disk space issues or invalid paths).
- General exceptions are not explicitly caught in the function; errors propagate and are handled at the module level in the try-except block.

**How to Use It:**  
1. Ensure AWS CLI is installed and configured with appropriate credentials for S3 access.
2. Call the function with the bucket name and optionally a list of folders to include:
   ```python
   zip_and_download_bucket('my-bucket', include_folders=['data/', 'logs/'])
   ```
   This downloads only 'data/' and 'logs/' folders, zips them, and uploads `my-bucket.zip` to `s3://my-bucket/my-bucket.zip`.
3. For all folders, omit `include_folders`:
   ```python
   zip_and_download_bucket('my-bucket')
   ```
4. Run the module directly for the hardcoded bucket:
   ```bash
   python download_s3_folder.py
   ```
   This processes 'bcbs-poc-oregon' with no folder filters.

**Notes:**  
- Temporary files are stored in `/tmp/`, which may require sufficient disk space for large buckets.
- The ZIP file is named `{bucket_name}.zip` and uploaded to the bucket root.
- Skipped folders are logged to console.
- Known issue: The function has a bug where it references `include_folder_names` instead of `include_folders` in the loop; this may cause a NameError if not defined externally.

---