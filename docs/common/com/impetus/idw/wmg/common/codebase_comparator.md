# CodebaseComparator

## Overview
The `CodebaseComparator` class compares codebases between Git repositories (stored as ZIP files in S3) and S3 directories, generating detailed Excel reports of differences, missing files, and line-by-line mismatches.

## Class Hierarchy
```
GlueELTStep
  └── CodebaseComparator
```

## Key Methods

### executeFlow(executor, *args, **kwargs)
**Purpose:** Main orchestration method that coordinates the entire comparison workflow.

**Parameters:**
- `executor`: Glue executor instance for S3 operations and logging
- `*args`: Variable positional arguments (unused)
- `**kwargs`: Keyword arguments containing:
  - `git_zip_path` (str, required): S3 path to Git repository ZIP file
  - `s3_codebase_path` (str, required): S3 path to deployed codebase directory
  - `output_path` (str, required): S3 path where comparison report will be uploaded
  - `redshift_zip_s3_path` (str, optional): S3 path to Redshift DDL ZIP for schema comparison
  - `redshift_secret` (str, optional): AWS Secrets Manager secret name for Redshift credentials
  - `table_names_csv_path` (str, optional): S3 path to CSV file mapping table names to target databases

**What it does:**
1. Validates required input parameters
2. Downloads and extracts Git ZIP file from S3
3. Downloads S3 codebase directory to local storage
4. Compares all files between the two codebases
5. Generates Excel report with multiple sheets
6. Optionally compares DDL schemas using DDLComparator
7. Uploads final report back to S3

### compare_directories(git_dir, s3_dir, output_excel)
**Purpose:** Compares two directory structures and generates Excel report.

**Parameters:**
- `git_dir` (str): Local path to extracted Git codebase
- `s3_dir` (str): Local path to downloaded S3 codebase
- `output_excel` (str): Local path where Excel report will be created

**What it does:**
- Identifies files present in both, only in Git, or only in S3
- Compares matching files line-by-line using difflib
- Calculates match ratios and identifies mismatches
- Creates Excel report with four worksheets:
  - **Summary**: Match ratios, line counts, mismatch statistics
  - **Detail**: Line-by-line differences with color highlighting
  - **Missing Files**: Files present in one location but not the other
  - **Failed Files**: Files that couldn't be compared due to errors

### compare_files(git_file, s3_file)
**Purpose:** Compares two individual files and returns comparison statistics.

**Parameters:**
- `git_file` (str): Path to file in Git codebase
- `s3_file` (str): Path to corresponding file in S3 codebase

**Returns:** Tuple containing:
- Match ratio (float): Similarity score between 0.0 and 1.0
- Fully matched (bool): True if files are identical
- Git line count (int): Number of lines in Git file
- S3 line count (int): Number of lines in S3 file
- Mismatches (list): List of (line_number, git_line, s3_line) tuples for differences

### highlight_differences(line1, line2)
**Purpose:** Highlights character-level differences between two text lines.

**Parameters:**
- `line1` (str): First line of text
- `line2` (str): Second line of text

**Returns:** Tuple of formatted lists for Excel rich text formatting, with tags indicating matched/mismatched sections.

## Usage Examples

### Basic Code Comparison
```python
from com.impetus.idw.wmg.common.codebase_comparator import CodebaseComparator

comparator = CodebaseComparator()
comparator.executeFlow(
    executor=glue_executor,
    git_zip_path="s3://my-bucket/git-repo.zip",
    s3_codebase_path="s3://my-bucket/codebase/domain1",
    output_path="s3://my-bucket/reports"
)
```

### Full Comparison with DDL Validation
```python
comparator = CodebaseComparator()
comparator.executeFlow(
    executor=glue_executor,
    git_zip_path="s3://my-bucket/git-repo.zip",
    s3_codebase_path="s3://my-bucket/codebase/finance",
    output_path="s3://my-bucket/reports",
    redshift_zip_s3_path="s3://my-bucket/redshift-ddl.zip",
    redshift_secret="prod/redshift/credentials",
    table_names_csv_path="s3://my-bucket/table-mapping.csv"
)
```

## Output
- **Excel Report**: Multi-sheet workbook uploaded to S3 with filename `code_comparison_{domain_name}.xlsx`
- **Log Output**: Detailed logging of comparison progress and any errors
- **Success/Failure**: Returns success status; failures are logged but don't stop processing

## Dependencies
- `difflib`: For text comparison and diff generation
- `xlsxwriter`: For Excel report creation with formatting
- `zipfile`: For extracting Git repository ZIP files
- `subprocess`: For AWS CLI S3 operations
- `DDLComparator`: For optional database schema comparison
- AWS S3, Glue utilities, and Secrets Manager access

## Error Handling
- Validates all required parameters at startup
- Continues processing other files if individual file comparisons fail
- Logs DDL comparison errors but doesn't fail entire workflow
- Captures failed file comparisons in separate Excel sheet