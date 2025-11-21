# utils.py

## Overview

The `utils.py` module provides a comprehensive set of utility functions and helper classes for file operations, string
manipulation, command execution, argument parsing, email sending, and data processing. It is designed to support data
engineering workflows, especially in distributed environments using Spark, S3, and Unix-like systems. The file includes
methods for reading/writing files, handling dependencies, executing shell commands, flattening dictionaries, extracting
fields, and more. Many functions are tailored for use in ETL pipelines and job orchestration.

## List of Methods and Briefs

| Method                           | Brief Description                                                      |
|----------------------------------|------------------------------------------------------------------------|
| get_instance                     | Dynamically creates an instance of a class from its name and arguments |
| get_class_details                | Parses a class name string to extract class, module, and package       |
| extract_source_tables_from_query | Extracts table names from SQL queries                                  |
| remove_sql_comments              | Removes comments from SQL queries, preserving string literals          |
| read_dependency_file             | Reads a dependency file and parses its contents                        |
| str_to_bool                      | Converts a string to a boolean value                                   |
| toBool                           | Converts a value to boolean, with error handling                       |
| caller_name                      | Returns the caller's name and hierarchy from the stack                 |
| prepare_arguments                | Prepares and parses command-line arguments                             |
| getConfigurations                | Builds a configuration dictionary from parsed arguments                |
| readExecutionProperties          | Reads and parses a JSON properties file                                |
| executeUnixCommandTimeout        | Executes a Unix command with a timeout                                 |
| executeUnixCommand               | Executes a Unix command and returns output                             |
| get_module                       | Dynamically imports a module                                           |
| import_param_module              | Imports a parameter module                                             |
| get_module_attribute             | Gets an attribute from a module                                        |
| import_global_property           | Imports global property module based on environment                    |
| import_spark_global_property     | Imports Spark global property module                                   |
| get_command_line_arg_value       | Gets a command-line argument value                                     |
| sendMail                         | Sends an email using Unix mail command                                 |
| send_mail                        | Sends an email using SMTP with attachments                             |
| flatten_dict                     | Flattens a nested dictionary                                           |
| executeMethod                    | Executes a list of methods on a job object                             |
| executeClass                     | Executes a list of classes by name                                     |
| create_splitted_files            | Splits a DataFrame into multiple files and writes to S3                |
| unzipFile                        | Unzips a file to a destination folder                                  |
| deleteDirectoryContent           | Deletes all files in a directory                                       |
| renameFile                       | Renames a file                                                         |
| deleteLocation                   | Deletes a file                                                         |
| getExecutionOrder                | Gets execution order from a priority file                              |
| list_files                       | Lists files in a directory with pattern matching                       |
| move_file                        | Moves files matching a pattern to a target directory                   |
| copy_file                        | Copies a file from source to target                                    |
| rename_file                      | Renames a file from source to target                                   |
| convert_to_list                  | Converts a string to a list by splitting lines                         |
| get_process_id                   | Gets the current process ID                                            |
| delete_file                      | Deletes a file                                                         |
| read_file                        | Reads the content of a file                                            |
| read_file_lines                  | Reads lines from a file                                                |
| write_file                       | Writes content to a file                                               |
| grep_lines_from_file             | Greps lines containing text from a file                                |
| grep_lines                       | Greps lines containing text from a list                                |
| change_dir                       | Changes the current working directory                                  |
| truncate                         | Removes lines containing a character from a file                       |
| sed_command                      | Applies sed-like scripts to files                                      |
| sed_lines                        | Applies sed-like scripts to lines                                      |
| extract_nth_field                | Extracts nth field from lines in a file                                |
| extract_nth_field_from_lines     | Extracts nth field from a list of lines                                |
| word_count                       | Counts lines, words, characters, or bytes in text                      |
| awk_from_file                    | Applies awk-like operations to a file or lines                         |
| handle_printf                    | Handles printf operations in awk-like scripts                          |
| handle_begin_operations          | Handles BEGIN operations in awk-like scripts                           |
| handle_iter_operations           | Handles iteration operations in awk-like scripts                       |
| handle_dollar                    | Handles $n field access in awk-like scripts                            |
| handle_functions                 | Handles built-in functions in awk-like scripts                         |
| is_int                           | Checks if a value is integer                                           |

---

## Method Details

### get_instance(clazz, *instance_args, **instance_kwargs)

**Purpose:** Dynamically creates an instance of a class from its name and arguments.

**Description:**
This method takes a class name (as a string or object), parses its module and package, imports the module, and creates
an instance of the class with the provided arguments.

**Args:**

- clazz (str): Class name or fully qualified class name (e.g., 'module.ClassName')
- *instance_args: Positional arguments for the class constructor
- **instance_kwargs: Keyword arguments for the class constructor

**Returns:**

- object: Instance of the specified class

**Raises:**

- ImportError: If the module or class cannot be imported
- AttributeError: If the class is not found in the module

**Example:**

```python
my_obj = get_instance('com.impetus.idw.wmg.common.MyClass', arg1, arg2, key='value')
```

**Notes:**

- Useful for dynamic class instantiation in plugin architectures

---

### get_class_details(name)

**Purpose:** Parses a class name string to extract class, module, and package.

**Description:**
Splits a fully qualified class name into its class, module, and package components.

**Args:**

- name (str): Fully qualified class name (e.g., 'package.module.ClassName')

**Returns:**

- tuple: (class, module, package)

**Example:**

```python
clazz, module, package = get_class_details('com.impetus.idw.wmg.common.MyClass')
```

---

### extract_source_tables_from_query(sql)

**Purpose:** Extracts table names from SQL queries.

**Description:**
Parses SQL to find all table names used in FROM and JOIN clauses, removes duplicates, and returns them in uppercase.

**Args:**

- sql (str): SQL query string

**Returns:**

- list: List of table names

**Example:**

```python
tables = extract_source_tables_from_query('SELECT * FROM my_table JOIN other_table ON ...')
```

---

### remove_sql_comments(sql_query)

**Purpose:** Removes comments from SQL queries, preserving string literals.

**Description:**
Masks string literals, removes inline and multiline comments, and normalizes whitespace.

**Args:**

- sql_query (str): SQL query string

**Returns:**

- str: SQL query without comments

**Example:**

```python
clean_sql = remove_sql_comments('SELECT * FROM table -- comment')
```

---

### read_dependency_file(location, is_parallel)

**Purpose:** Reads a dependency file and parses its contents.

**Description:**
Reads a file containing job dependencies and returns a dictionary mapping priorities to file names.

**Args:**

- location (str): Path to dependency file
- is_parallel (bool): Whether to parse in parallel mode

**Returns:**

- dict: Parsed dependencies

**Example:**

```python
deps = read_dependency_file('file_dependency.txt', True)
```

---

### str_to_bool(flag_str)

**Purpose:** Converts a string to a boolean value.

**Description:**
Returns True if the string is 'true' (case-insensitive), otherwise False.

**Args:**

- flag_str (str): String to convert

**Returns:**

- bool

**Example:**

```python
flag = str_to_bool('True')  # True
```

---

### toBool(value, handle_error=False)

**Purpose:** Converts a value to boolean, with error handling.

**Description:**
Uses distutils to convert a value to boolean. If conversion fails and handle_error is True, returns None.

**Args:**

- value: Value to convert
- handle_error (bool): Whether to handle errors silently

**Returns:**

- bool or None

**Example:**

```python
flag = toBool('yes')  # True
```

---

### caller_name(depth=20)

**Purpose:** Returns the caller's name and hierarchy from the stack.

**Description:**
Inspects the call stack to find the calling function/module/class hierarchy.

**Args:**

- depth (int): Stack depth to inspect

**Returns:**

- str or None: Caller hierarchy string

**Example:**

```python
hierarchy = caller_name(5)
```

---

### prepare_arguments()

**Purpose:** Prepares and parses command-line arguments.

**Description:**
Defines and parses command-line arguments for job execution, including locations, properties, and workflow names.

**Args:**

- None (uses sys.argv)

**Returns:**

- argparse.Namespace: Parsed arguments

**Example:**

```python
args = prepare_arguments()
```

---

### getConfigurations(args, executorDAL='SparkOnHive')

**Purpose:** Builds a configuration dictionary from parsed arguments.

**Description:**
Creates a dictionary of configuration values for job execution, including paths, handlers, and properties.

**Args:**

- args (argparse.Namespace): Parsed arguments
- executorDAL (str): Executor type

**Returns:**

- dict: Configuration dictionary

**Example:**

```python
configs = getConfigurations(args)
```

---

### readExecutionProperties(location)

**Purpose:** Reads and parses a JSON properties file.

**Description:**
Opens and loads a JSON file containing execution properties.

**Args:**

- location (str): Path to JSON file

**Returns:**

- dict: Parsed JSON properties

**Example:**

```python
props = readExecutionProperties('executor_properties.json')
```

---

### executeUnixCommandTimeout(command, maxTime)

**Purpose:** Executes a Unix command with a timeout.

**Description:**
Runs a shell command, waits for completion or timeout, and returns output, error, and return code.

**Args:**

- command (str): Command to execute
- maxTime (float): Maximum time in seconds

**Returns:**

- tuple: (stdout, stderr, return_code)

**Raises:**

- Exception: If timeout or execution error occurs

**Example:**

```python
out, err, code = executeUnixCommandTimeout('ls -l', 10)
```

---

### executeUnixCommand(command)

**Purpose:** Executes a Unix command and returns output.

**Description:**
Runs a shell command and returns output, error, and return code.

**Args:**

- command (str): Command to execute

**Returns:**

- tuple: (stdout, stderr, return_code)

**Raises:**

- Exception: If execution error occurs

**Example:**

```python
out, err, code = executeUnixCommand('ls -l')
```

---

### get_module(module)

**Purpose:** Dynamically imports a module.

**Description:**
Imports a module by name, handling package/module separation.

**Args:**

- module (str): Module name

**Returns:**

- module: Imported module

**Example:**

```python
mod = get_module('com.impetus.idw.wmg.common.utils')
```

---

### import_param_module(module)

**Purpose:** Imports a parameter module.

**Description:**
Wrapper for get_module().

**Args:**

- module (str): Module name

**Returns:**

- module: Imported module

**Example:**

```python
mod = import_param_module('com.impetus.idw.wmg.common.utils')
```

---

### get_module_attribute(module, attribute)

**Purpose:** Gets an attribute from a module.

**Description:**
Imports a module and retrieves the specified attribute.

**Args:**

- module (str): Module name
- attribute (str): Attribute name

**Returns:**

- object: Attribute value

**Example:**

```python
attr = get_module_attribute('os', 'path')
```

---

### import_global_property(file_name=None, env_name=None, env_key_name='eng_app_environment')

**Purpose:** Imports global property module based on environment.

**Description:**
Imports a global property module for the specified environment.

**Args:**

- file_name (str, optional): Base file name
- env_name (str, optional): Environment name
- env_key_name (str): Key for environment argument

**Returns:**

- module: Imported module

**Example:**

```python
mod = import_global_property('global_property', 'dev')
```

---

### import_spark_global_property(file_name=None, env_name=None, env_key_name='environment')

**Purpose:** Imports Spark global property module.

**Description:**
Imports a Spark global property module for the specified environment.

**Args:**

- file_name (str, optional): Base file name
- env_name (str, optional): Environment name
- env_key_name (str): Key for environment argument

**Returns:**

- module: Imported module

**Example:**

```python
mod = import_spark_global_property('global_property', 'prod')
```

---

### get_command_line_arg_value(arg_name, default_value=None)

**Purpose:** Gets a command-line argument value.

**Description:**
Searches sys.argv for the specified argument and returns its value.

**Args:**

- arg_name (str): Argument name
- default_value: Default value if not found

**Returns:**

- str or default_value

**Example:**

```python
val = get_command_line_arg_value('location', '/tmp')
```

---

### sendMail(sender, receiver, subject, body, attachment, carbonCopy=None, blindCarbonCopy=None)

**Purpose:** Sends an email using Unix mail command.

**Description:**
Constructs and executes a mail command to send an email with optional attachments and CC/BCC.

**Args:**

- sender (str): Sender email address
- receiver (str): Receiver email address
- subject (str): Email subject
- body (str): Email body
- attachment (str): Attachment file path(s)
- carbonCopy (str, optional): CC email address(es)
- blindCarbonCopy (str, optional): BCC email address(es)

**Returns:**

- None

**Raises:**

- Exception: If sender or receiver is missing

**Example:**

```python
sendMail('me@example.com', 'you@example.com', 'Subject', 'Body', '/tmp/file.txt')
```

---

### send_mail(executor, from_email, to_email, subject, body, smtp_server=None, smtp_port=None, attachment_path=None, msg_type='plain')

**Purpose:** Sends an email using SMTP with attachments.

**Description:**
Creates and sends an email using SMTP, with support for attachments and message types.

**Args:**

- executor: Executor object for S3 downloads
- from_email (str): Sender email address
- to_email (str or list): Receiver email address(es)
- subject (str): Email subject
- body (str): Email body
- smtp_server (str, optional): SMTP server address
- smtp_port (int, optional): SMTP server port
- attachment_path (str or list, optional): Attachment file path(s)
- msg_type (str): Message type ('plain' or 'html')

**Returns:**

- None

**Example:**

```python
send_mail(executor, 'me@example.com', 'you@example.com', 'Subject', 'Body', 'smtp.example.com', 587, '/tmp/file.txt')
```

---

### flatten_dict(param_dict, new_param_dict, parent_key=None)

**Purpose:** Flattens a nested dictionary.

**Description:**
Recursively flattens a nested dictionary into a single-level dictionary with dot-separated keys.

**Args:**

- param_dict (dict): Dictionary to flatten
- new_param_dict (dict): Output dictionary
- parent_key (str, optional): Parent key prefix

**Returns:**

- None

**Example:**

```python
flat = {}
flatten_dict({'a': {'b': 1}}, flat)
```

---

### create_splitted_files(executor, df, records_per_file, options, s3_file_path, s3_target_dir=None, filename_count_separator="")

**Purpose:** Splits a DataFrame into multiple files and writes to S3.

**Description:**
Splits a Spark DataFrame into multiple CSV files, each containing a specified number of records, and writes them to S3.
Optionally copies files to a target directory.

**Args:**

- executor: Object with write_single_file method
- df: Spark DataFrame
- records_per_file (int): Number of records per file
- options (dict): Write options
- s3_file_path (str): Base S3 file path
- s3_target_dir (str, optional): Target S3 directory
- filename_count_separator (str, optional): Separator for file numbering

**Returns:**

- list: List of file paths created

**Example:**

```python
files = create_splitted_files(executor, df, 1000, options, 's3://bucket/data.csv')
```

---

### unzipFile(source, destination)

**Purpose:** Extracts all files from a zip archive to the specified destination directory.

**Args:**

- source (str): Path to zip file
- destination (str): Destination directory

**Returns:**

- int: 1 if successful

**Example:**

```python
unzipFile('archive.zip', '/tmp/extracted')
```

---

### deleteDirectoryContent(source)

**Purpose:** Deletes all files in a directory.

**Args:**

- source (str): Directory path

**Returns:**

- None

**Example:**

```python
deleteDirectoryContent('/tmp/data')
```

---

### renameFile(fileName, newFileName)

**Purpose:** Renames a file.

**Description:**
Renames a file from fileName to newFileName if it exists.

**Args:**

- fileName (str): Original file name
- newFileName (str): New file name

**Returns:**

- None

**Example:**

```python
renameFile('old.txt', 'new.txt')
```

---

### deleteLocation(source)

**Purpose:** Deletes a file.

**Description:**
Removes the specified file if it exists.

**Args:**

- source (str): File path

**Returns:**

- None

**Example:**

```python
deleteLocation('file.txt')
```

---

### getExecutionOrder(location)

**Purpose:** Gets execution order from a priority file.

**Description:**
Loads job priorities from a file using TaskPriority and returns job names by priority.

**Args:**

- location (str): Path to priority file

**Returns:**

- dict: Job names by priority

**Example:**

```python
order = getExecutionOrder('priority.json')
```

---

### list_files(input_file_path, recursive=False, include_file_patterns=None, exclude_file_pattern=None, list_file_type=None)

**Purpose:** Lists files in a directory with pattern matching.

**Description:**
Lists files in a directory, optionally recursively, and filters by include/exclude patterns.

**Args:**

- input_file_path (str): Directory path
- recursive (bool, optional): List files recursively
- include_file_patterns (list, optional): Patterns to include
- exclude_file_pattern (str, optional): Pattern to exclude
- list_file_type (str, optional): Type of items to list

**Returns:**

- list: List of file paths

**Example:**

```python
files = list_files('/tmp', recursive=True, include_file_patterns=['*.csv'])
```

---

### move_file(src, tgt)

**Purpose:** Moves files matching a pattern to a target directory.

**Description:**
Moves files from src to tgt, supporting wildcards.

**Args:**

- src (str): Source file path or pattern
- tgt (str): Target directory

**Returns:**

- None

**Example:**

```python
move_file('/tmp/*.txt', '/data')
```

---

### copy_file(source_path, target_path)

**Purpose:** Copies a file from source to target.

**Args:**

- source_path (str): Source file path
- target_path (str): Target file path

**Returns:**

- None

**Example:**

```python
copy_file('a.txt', 'b.txt')
```

---

### rename_file(source_path, target_path)

**Purpose:** Renames a file from source to target.

**Args:**

- source_path (str): Original file path
- target_path (str): New file path

**Returns:**

- None

**Example:**

```python
rename_file('old.txt', 'new.txt')
```

---

### convert_to_list(obj)

**Purpose:** Converts a string to a list by splitting lines.

**Description:**
Splits a string into a list of lines, or returns the object if already a list.

**Args:**

- obj (str or list): Object to convert

**Returns:**

- list

**Example:**

```python
lst = convert_to_list('a\nb\nc')
```

---

### get_process_id()

**Purpose:** Gets the current process ID.

**Args:**

- None

**Returns:**

- int: Process ID

**Example:**

```python
pid = get_process_id()
```

---

### delete_file(file_path, **kwargs)

**Purpose:** Deletes the specified file.

**Args:**

- file_path (str): File path
- **kwargs: Additional arguments

**Returns:**

- None

**Example:**

```python
delete_file('file.txt')
```

---

### read_file(file_path, encode='utf-8')

**Purpose:** Reads the entire content of a file as a string.

**Args:**

- file_path (str): File path
- encode (str, optional): Encoding

**Returns:**

- str: File content

**Example:**

```python
content = read_file('file.txt')
```

---

### read_file_lines(file_path, encode='utf-8')

**Purpose:** Reads all lines from a file and returns them as a list.

**Args:**

- file_path (str): File path
- encode (str, optional): Encoding

**Returns:**

- list: Lines from file

**Example:**

```python
lines = read_file_lines('file.txt')
```

---

### write_file(content, file_path, mode='append')

**Purpose:** Writes content to a file, appending or overwriting as specified.

**Args:**

- content (str): Content to write
- file_path (str): File path
- mode (str, optional): 'append' or 'overwrite'

**Returns:**

- None

**Example:**

```python
write_file('Hello', 'file.txt', mode='overwrite')
```

---

### grep_lines_from_file(file_path, text)

**Purpose:** Greps lines containing text from a file. Reads lines from a file and returns those containing the specified
text.

**Args:**

- file_path (str): File path
- text (str): Text to search for

**Returns:**

- list: Matching lines

**Example:**

```python
matches = grep_lines_from_file('file.txt', 'error')
```

---

### grep_lines(lines, text)

**Purpose:** Greps lines containing text from a list. Returns lines from a list that contain the specified text.

**Args:**

- lines (list): List of lines
- text (str): Text to search for

**Returns:**

- list: Matching lines

**Example:**

```python
matches = grep_lines(['a', 'b', 'error'], 'error')
```

---

### change_dir(file_path)

**Purpose:** Changes the working directory to the specified path.

**Args:**

- file_path (str): Directory path

**Returns:**

- None

**Example:**

```python
change_dir('/tmp')
```

---

### truncate(input_path, output_path, character)

**Purpose:** Removes lines containing a character from a file.

**Description:**
Reads lines from input_path, removes those containing the character, and writes to output_path.

**Args:**

- input_path (str): Input file path
- output_path (str): Output file path
- character (str): Character to filter out

**Returns:**

- None

**Example:**

```python
truncate('input.txt', 'output.txt', '#')
```

---

### sed_command(scripts, file_path)

**Purpose:** Applies sed-like transformation scripts to files matching a pattern.

**Args:**

- scripts (list): List of sed scripts
- file_path (str): File path or pattern

**Returns:**

- list: Modified lines

**Example:**

```python
lines = sed_command(['s/foo/bar/g'], 'file.txt')
```

---

### sed_lines(scripts, lines)

**Purpose:** Applies sed-like transformation scripts to a list of lines.

**Args:**

- scripts (list): List of sed scripts
- lines (list): List of lines

**Returns:**

- list: Modified lines

**Example:**

```python
new_lines = sed_lines(['s/foo/bar/g'], ['foo', 'baz'])
```

---

### extract_nth_field(file_path, option, indexes, complement=False, delimiter="\t")

**Purpose:** Extracts specified fields or characters from each line in a file.

**Args:**

- file_path (str): File path
- option (str): 'char' for character, else field
- indexes (str): Indexes to extract (e.g., '1,2')
- complement (bool, optional): Whether to exclude specified indexes
- delimiter (str, optional): Field delimiter

**Returns:**

- list: Extracted fields

**Example:**

```python
fields = extract_nth_field('file.txt', 'field', '1,2')
```

---

### word_count(text, option=None)

**Purpose:** Counts lines, words, characters, bytes, or max line length in the input text.

**Args:**

- text (str or list): Text to count
- option (str, optional): '-l', '-w', '-m', '-c', '-L'

**Returns:**

- int: Count result

**Example:**

```python
count = word_count('a b c', '-w')
```

---

### awk_from_file(input_file, seperator, begin_operations=None, iter_operations=None, end_operations=None)

**Purpose:** Applies awk-like operations to a file or lines.

**Description:**
Processes a file or list of lines using awk-like BEGIN, iteration, and END operations.

**Args:**

- input_file (str or list): File path or lines
- seperator (str): Field separator
- begin_operations (str, optional): BEGIN operations
- iter_operations (str, optional): Iteration operations
- end_operations (str, optional): END operations

**Returns:**

- str: Result of awk operations

**Example:**

```python
result = awk_from_file('file.txt', '\t', begin_operations='printf "Start"', iter_operations='print $1', end_operations='printf "End"')
```

---

### is_int(text)

**Purpose:** Checks if a value is integer. Returns True if text can be converted to integer, else False.

**Args:**

- text: Value to check

**Returns:**

- bool

**Example:**

```python
flag = is_int('123')  # True
```
