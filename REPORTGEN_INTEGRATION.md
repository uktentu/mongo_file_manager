# ReportGen Integration Guide

> How to integrate the MongoDB Document Seeder with ReportGen for direct
> file access — no export step, no temp files, maximum speed.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Integration Methods](#integration-methods)
3. [Method 1: Direct MongoDB (Recommended)](#method-1-direct-mongodb)
4. [Method 2: HTTP API](#method-2-http-api)
5. [File Flow](#file-flow)
6. [Setup Instructions](#setup-instructions)
7. [API Reference](#api-reference)
8. [Example: Full ReportGen Workflow](#example-full-reportgen-workflow)
9. [Error Handling](#error-handling)
10. [Performance Tips](#performance-tips)

---

## Architecture Overview

```
┌──────────────────┐     seed.yaml      ┌──────────────────────┐
│  Regulation Repo │ ─────────────────→ │  MongoDB Document    │
│  (CI/CD pipeline)│   POST /api/seed   │  Seeder              │
└──────────────────┘                    │  ┌────────────────┐  │
                                        │  │  MongoDB        │  │
                                        │  │  ┌──────────┐  │  │
                                        │  │  │ metadata  │  │  │
                                        │  │  │ collection│  │  │
                                        │  │  └──────────┘  │  │
                                        │  │  ┌──────────┐  │  │
                                        │  │  │ GridFS    │  │  │
                                        │  │  │ (files)   │  │  │
                                        │  │  └──────────┘  │  │
                                        │  └────────────────┘  │
                                        └──────────┬───────────┘
                                                   │
                              ┌─────────────────────┤
                              │                     │
                    Direct MongoDB           HTTP API
                    (recommended)          (cross-network)
                              │                     │
                              ▼                     ▼
                    ┌──────────────────────────────────┐
                    │         ReportGen                 │
                    │                                   │
                    │  1. Get JSON config (in memory)   │
                    │  2. Get SQL query (in memory)     │
                    │  3. Execute SQL against data DB   │
                    │  4. Apply template (if any)       │
                    │  5. Generate report               │
                    └──────────────────────────────────┘
```

**Key insight**: ReportGen reads files **directly from MongoDB** — there is
no export-to-disk step. Files flow from GridFS → memory → processing.

---

## Integration Methods

| Method | When to Use | Latency | Reliability |
|--------|------------|---------|-------------|
| **Direct MongoDB** | Same network / VPC | ~5ms per file | ⭐⭐⭐⭐⭐ |
| **HTTP API** | Different networks | ~50-200ms per file | ⭐⭐⭐⭐ |

---

## Method 1: Direct MongoDB (Recommended)

### Installation

In your ReportGen project, install the Seeder SDK:

```bash
# Option A: Install as a package dependency (recommended)
pip install pymongo

# Option B: Copy the SDK file directly
cp /path/to/mongo-seeder/src/sdk/client.py /path/to/reportgen/lib/seeder_client.py
```

### Configuration

ReportGen needs the **same MongoDB connection string** as the Seeder.
Set these environment variables:

```bash
# .env or environment config
MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/
MONGO_DB_NAME=doc_management
MONGO_METADATA_COLLECTION=metadata    # optional, default: metadata
MONGO_GRIDFS_BUCKET=fs                # optional, default: fs
```

### Usage

```python
from src.sdk.client import ReportGenClient

# Initialize (reads from environment)
client = ReportGenClient.from_env()

# ── Get files directly in memory (NO disk I/O) ────────────────

# 1. Get the JSON config as a Python dict
config = client.get_json_config("report-uuid-here")
print(config["report"]["name"])  # → "MAS TRM Report"

# 2. Get the SQL query as a string
sql = client.get_sql_query("report-uuid-here")
print(sql)  # → "SELECT * FROM trm_events WHERE ..."

# 3. Get template (returns None if not set)
template = client.get_template("report-uuid-here")

# 4. Get raw bytes for any file
raw_bytes = client.get_file_bytes("report-uuid-here", "json_config")

# ── Query records ──────────────────────────────────────────────

# List all active records for a regulation
records = client.list_records(regulation="MAS-TRM")
for r in records:
    print(f"  {r['report_id']} - {r['name']} (v{r['version']})")

# Get specific record metadata
record = client.get_record("report-uuid-here")

# ── Cleanup ────────────────────────────────────────────────────
client.close()
```

### Context Manager

```python
with ReportGenClient.from_env() as client:
    config = client.get_json_config("report-uuid-here")
    sql = client.get_sql_query("report-uuid-here")
    # ... process ...
# Connection automatically closed
```

---

## Method 2: HTTP API

### Configuration

```bash
SEEDER_BASE_URL=http://seeder-host:8000
SEEDER_API_KEY=your-api-key
```

### Usage

```python
from src.sdk.client import ReportGenHTTPClient

client = ReportGenHTTPClient(
    base_url="http://seeder-host:8000",
    api_key="your-api-key",
)

# Same interface as Direct MongoDB client
config = client.get_json_config("report-uuid-here")
sql = client.get_sql_query("report-uuid-here")

# List available files
files_info = client.list_record_files("report-uuid-here")
print(files_info)
# → {"report_id": "...", "version": 1, "files": {"json_config": {...}, "sql_file": {...}}}

client.close()
```

### REST Endpoints for Direct Access

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/records/{id}/files` | GET | List available files with metadata |
| `/api/records/{id}/files/json_config` | GET | Stream JSON config bytes |
| `/api/records/{id}/files/sql_file` | GET | Stream SQL file bytes |
| `/api/records/{id}/files/template` | GET | Stream template bytes |

```bash
# curl examples
curl -H "X-API-Key: your-key" \
  http://seeder:8000/api/records/UUID/files/json_config \
  -o config.json

curl -H "X-API-Key: your-key" \
  http://seeder:8000/api/records/UUID/files/sql_file \
  -o query.sql
```

---

## File Flow

```
Regulation Repo                    MongoDB Seeder                    ReportGen
─────────────────                  ──────────────                    ──────────

seed.yaml ───────── POST ─────────→ Validates files
configs/*.json                       Uploads to GridFS
sql/*.sql                            Stores metadata
templates/*                          Assigns report_id
                                         │
                                         │ (files stored in GridFS)
                                         │
                                         ├────── Direct MongoDB ─────→ get_json_config()
                                         │       (recommended)          get_sql_query()
                                         │       No temp files          get_template()
                                         │       ~5ms per file          ↓
                                         │                           Process report
                                         │
                                         └────── HTTP API ───────────→ GET /files/json_config
                                                 (cross-network)       GET /files/sql_file
                                                 ~50-200ms per file    ↓
                                                                    Process report
```

---

## Setup Instructions

### Step 1: Ensure MongoDB Seeder is Running

```bash
# Using Docker
docker-compose up -d

# Or locally
pip install -r requirements.txt
python -m uvicorn src.api:app --host 0.0.0.0 --port 8000
```

### Step 2: Seed Your Regulation Data

```bash
# Via CLI
python -m src.cli seed seeds/seed.yaml

# Via API (from CI/CD)
python integration/seed_caller.py manifest seeds/seed.yaml
```

### Step 3: Add SDK to ReportGen

```bash
# In your ReportGen project
pip install pymongo

# Copy the SDK
cp src/sdk/client.py /path/to/reportgen/lib/seeder_client.py
```

### Step 4: Configure ReportGen Environment

```bash
# Same MongoDB connection as the Seeder
MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/
MONGO_DB_NAME=doc_management
```

### Step 5: Use in ReportGen Code

```python
# reportgen/main.py
from lib.seeder_client import ReportGenClient

def generate_report(report_id: str):
    with ReportGenClient.from_env() as seeder:
        # Get config and SQL directly from MongoDB
        config = seeder.get_json_config(report_id)
        sql_query = seeder.get_sql_query(report_id)
        template = seeder.get_template(report_id)

        # Execute SQL against your data database
        data = execute_query(sql_query, params={
            "start_date": "2026-01-01",
            "end_date": "2026-03-31",
        })

        # Render report
        if template:
            output = render_template(template, data=data, config=config)
        else:
            output = generate_default_report(data, config)

        return output
```

---

## API Reference

### ReportGenClient (Direct MongoDB)

| Method | Returns | Description |
|--------|---------|-------------|
| `from_env()` | `ReportGenClient` | Create from MONGO_URI env var |
| `from_uri(uri, db_name)` | `ReportGenClient` | Create with explicit URI |
| `get_record(report_id, version=None)` | `dict` | Get record metadata |
| `list_records(**filters)` | `list[dict]` | Query records |
| `get_file_bytes(report_id, file_key)` | `bytes` | Raw file content |
| `get_json_config(report_id)` | `dict` | Parsed JSON config |
| `get_sql_query(report_id)` | `str` | SQL query text |
| `get_template(report_id)` | `str \| None` | Template text or None |
| `get_all_files(report_id)` | `dict[str, bytes]` | All files as bytes |
| `export_file(report_id, file_key, dir)` | `Path` | Write file to disk |
| `close()` | None | Close connection |

### ReportGenHTTPClient (HTTP API)

| Method | Returns | Description |
|--------|---------|-------------|
| `get_record(report_id)` | `dict` | Get record via API |
| `list_records(**filters)` | `dict` | Query records via API |
| `get_file_bytes(report_id, file_key)` | `bytes` | Stream file via API |
| `get_json_config(report_id)` | `dict` | Parsed JSON config |
| `get_sql_query(report_id)` | `str` | SQL query text |
| `list_record_files(report_id)` | `dict` | List available files |

---

## Error Handling

```python
from src.sdk.client import ReportGenClient

client = ReportGenClient.from_env()

try:
    config = client.get_json_config("some-report-id")
except ValueError as e:
    # Record not found or file not available
    print(f"Not found: {e}")
except RuntimeError as e:
    # Checksum mismatch (data corruption)
    print(f"Data integrity error: {e}")
except Exception as e:
    # Connection error, etc.
    print(f"Connection error: {e}")
```

---

## Performance Tips

1. **Reuse the client** — Don't create a new `ReportGenClient` for each request.
   Create once at app startup and reuse.

2. **Use Direct MongoDB** — HTTP adds ~50-200ms overhead per file. Direct
   MongoDB is ~5ms.

3. **Don't export to disk** — Use `get_json_config()` and `get_sql_query()`
   which return data directly in memory. Avoid `export_file()` unless you
   truly need disk files.

4. **Batch processing** — Use `get_all_files()` if you need all three files
   for a single report.

5. **Connection pooling** — The SDK uses PyMongo's built-in connection pool.
   One client instance handles multiple concurrent requests efficiently.
