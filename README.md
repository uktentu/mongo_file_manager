# MongoDB File Manager

A robust Python utility for managing file uploads and downloads with MongoDB, featuring intelligent storage selection and data integrity verification.

## Features

- **Intelligent Storage**: Automatically chooses between **BSON Document** storage (for small files) and **GridFS** (for large files) based on a configurable threshold.
- **Data Integrity**: Calculates and validates **SHA-256 checksums** for every file to ensure uploaded and downloaded data is identical.
- **Deduplication Ready**: Indexed checksums allow for easy implementation of file deduplication.
- **Metadata Tracking**: Stores original filename, upload date, file size, and checksum.

## Prerequisites

- Python 3.x
- MongoDB instance (Local or Atlas)

## Installation

1. Clone the repository.
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## CLI Usage

You can use the `main.py` script to interact with the file manager directly from the terminal.

### Upload a File
```bash
python main.py upload <path_to_file>
```
Example: `python main.py upload requirements.txt`

### Download a File
```bash
python main.py download <filename> [--destination <output_dir>]
```
Example (uses default `./downloaded` folder): 
`python main.py download requirements.txt`

Example (custom destination):
`python main.py download requirements.txt --destination ./my_downloads`

### Options
- `--threshold <MB>`: Set the GridFS threshold (default: 15MB).
- `--db-url <url>`: MongoDB connection string.
- `--db-name <name>`: Database name.

## Programmatic Usage

### Initialization
```python
from mongo_file_manager import MongoFileManager

# Initialize with connection details and threshold (in MB)
manager = MongoFileManager(
    db_url='mongodb://localhost:27017', 
    db_name='fileStorageDemo', 
    gridfs_threshold_mb=15
)
```

### Uploading a File
```python
file_id, method = manager.upload_file("path/to/my_file.txt")
print(f"Uploaded via {method} with ID: {file_id}")
```

### Downloading a File
```python
output_path = manager.download_file("my_file.txt", "download_dir/")
print(f"File saved to: {output_path}")
```
