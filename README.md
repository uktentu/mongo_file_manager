# MongoDB Document Seeder

Seeds, versions, and manages regulatory document bundles in MongoDB. A "bundle" is a set of files that belong together:

1. **JSON Config** — metadata about a report (name, schedule, parameters)
2. **SQL File** — the database query that generates the report
3. **Template** — (optional) a formatting template

The app stores these in MongoDB with full **version history**, **SHA-256 checksums**, and **audit trails**. When a bundle changes, the old version is deactivated (never deleted) and a new version is appended.

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure MongoDB connection
cp .env.example .env
# Edit .env with your MongoDB URI

# 3. Seed bundles from manifest
python -m src.cli seed seeds/seed.yaml

# 4. Or start the REST API
uvicorn src.api:app --reload --port 8000
```

### Environment Variables

```env
MONGO_URI=mongodb://localhost:27017
MONGO_DB_NAME=doc_management
LOG_LEVEL=INFO
```

---

## Project Structure

```
├── .env                    ← MongoDB connection settings
├── requirements.txt        ← Python dependencies
├── seeds/                  ← Input bundles to seed
│   ├── seed.yaml           ← Manifest listing all bundles
│   ├── configs/            ← JSON config files
│   ├── sql/                ← SQL query files
│   └── templates/          ← Template files
├── src/
│   ├── cli.py              ← CLI entry point (Click)
│   ├── api.py              ← REST API (FastAPI)
│   ├── config/
│   │   └── database.py     ← MongoDB connection manager
│   ├── models/
│   │   └── schemas.py      ← Pydantic data models
│   ├── services/
│   │   ├── seed_service.py     ← Core seeding logic
│   │   ├── fetch_service.py    ← Query operations
│   │   ├── gridfs_service.py   ← GridFS file storage
│   │   ├── cleanup_service.py  ← Retention / purging
│   │   ├── export_service.py   ← Reconstruct files to disk
│   │   └── audit_service.py    ← Audit log entries
│   ├── errors/
│   │   └── exceptions.py   ← Custom exception hierarchy
│   └── utils/
│       ├── validator.py     ← Input validation
│       ├── unique_id.py     ← Deterministic ID builder
│       ├── checksum.py      ← SHA-256 checksums
│       └── retry.py         ← Retry with exponential backoff
└── tests/
    └── unit/               ← 47 unit tests
```

---

## How It Works

### 1. Database Connection

On startup, `DatabaseManager` connects to MongoDB, pings the server, and:

- **Detects transaction support** — checks if MongoDB is a replica set or standalone. If standalone, operations proceed without transactions (with a warning).
- **Creates indexes** — a partial unique index on `(unique_id)` where `active=true` ensures only one active record per bundle. Secondary indexes on `csi_id`, `region`, `regulation` for fast lookups.

### 2. Seeding Bundles

Prepare a `seed.yaml` manifest:

```yaml
bundles:
  - csi_id: "CSI-001"
    region: "APAC"
    regulation: "MAS-TRM"
    json_config: "configs/mas_trm_report.json"
    template: "templates/mas_trm_template.txt"
    sql_file: "sql/mas_trm_query.sql"
```

Run:
```bash
python -m src.cli seed seeds/seed.yaml
```

For each bundle, the seeder:

1. **Validates** the bundle keys and JSON config (must have `name` and `outFileName`)
2. **Builds a unique ID** from `regulation_name_outFileName_region` (normalized, lowercase)
3. **Checks for existing active record:**
   - No existing → **CREATE** new record (version 1)
   - Exists but checksums match → **SKIP** (idempotent)
   - Exists but checksums differ → **MODIFY** (deactivate old, create new version)

### 3. Data Storage

| Storage | What | Why |
|---|---|---|
| `metadata` collection | Record metadata, version, checksums, file references, audit log | Fast queries, version tracking |
| `configs` collection | JSON config content as embedded documents | JSON is small, no need for GridFS |
| GridFS `sqlfiles` bucket | SQL files as binary blobs | Handles large files with chunking |
| GridFS `templates` bucket | Template files as binary blobs | Same reason |

A metadata document:

```json
{
  "unique_id": "mas-trm_mas_trm_compliance_report_mas_trm_output_apac",
  "csi_id": "CSI-001",
  "region": "APAC",
  "regulation": "MAS-TRM",
  "name": "MAS TRM Compliance Report",
  "out_file_name": "mas_trm_output",
  "version": 1,
  "active": true,
  "original_files": {
    "json_config": "mas_trm_report.json",
    "template": "mas_trm_template.txt",
    "sql_file": "mas_trm_query.sql"
  },
  "file_references": {
    "json_config_id": "ObjectId(...)",
    "template_gridfs_id": "ObjectId(...)",
    "sql_gridfs_id": "ObjectId(...)"
  },
  "checksums": {
    "json_config": "sha256:abc123...",
    "template": "sha256:def456...",
    "sql_file": "sha256:789ghi..."
  },
  "file_sizes": { "json_config": 393, "template": 651, "sql_file": 636 },
  "uploaded_at": "2026-02-18T08:00:00Z",
  "audit_log": [
    { "action": "CREATED", "timestamp": "...", "details": "Initial seed" }
  ]
}
```

### 4. Append-Only Versioning

When a bundle is modified, the old version is **deactivated** (`active: false`) and a new version is **appended**. Old versions are never deleted automatically — use the `cleanup` command to purge them.

If MongoDB supports transactions (replica set), the deactivation and new insert happen atomically. On standalone, operations run sequentially with a logged warning.

### 5. Safety Mechanisms

**Orphan Cleanup** — If uploading files to GridFS succeeds but inserting the metadata document fails, `GridFSOrphanTracker` rolls back by deleting the orphaned GridFS files:

```
upload SQL ✅ → upload template ✅ → insert metadata ❌
                                          ↓
                              tracker.cleanup() → deletes SQL & template from GridFS
```

**Retry with Backoff** — GridFS uploads/downloads are wrapped with `@retry_on_failure`. On transient errors (network timeouts, auto-reconnects), the operation retries up to 3 times with exponential delays (0.5s → 1s → 2s).

**Checksum Verification** — Every file gets a `sha256:...` checksum at upload time. During export, checksums are re-verified. If a mismatch is detected:

- The corrupted file is **deleted from disk**
- A `ChecksumMismatchError` is raised
- The export **aborts**

Use `--force` to export anyway, or `--no-verify` to skip verification.

---

## CLI Commands

```bash
# Seed all bundles from manifest
python -m src.cli seed seeds/seed.yaml

# Create a single record
python -m src.cli create --csi-id CSI-003 --region US --regulation SOX \
  --config path/to/config.json --sql path/to/query.sql

# Modify an existing record
python -m src.cli modify --unique-id "sox_report_sox_output_us" --sql new_query.sql

# List active records
python -m src.cli list
python -m src.cli list --all    # include inactive

# Show version history
python -m src.cli history --unique-id "mas-trm_mas_trm_compliance_report_mas_trm_output_apac"

# Fetch a specific record
python -m src.cli fetch --unique-id "..."
python -m src.cli fetch --region APAC
python -m src.cli fetch --csi-id CSI-001

# Export bundle files back to disk
python -m src.cli export --unique-id "..." -o ./exported/
python -m src.cli export --unique-id "..." -V 2 -o ./exported/       # specific version
python -m src.cli export --unique-id "..." -o ./exported/ --force     # export even if checksums fail
python -m src.cli export --unique-id "..." -o ./exported/ --no-verify # skip verification

# Cleanup old versions
python -m src.cli cleanup --unique-id "..." --keep 3 --dry-run
python -m src.cli cleanup --all --keep 3
python -m src.cli cleanup --max-age-days 90

# Verbose mode (debug logging)
python -m src.cli -v seed seeds/seed.yaml
```

---

## REST API

```bash
uvicorn src.api:app --reload --port 8000
```

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/health` | Health check + transaction support status |
| GET | `/api/records` | List records (filters: `region`, `csi_id`, `regulation`, `active_only`) |
| GET | `/api/records/{id}` | Fetch by unique_id (optional `?version=N`) |
| GET | `/api/records/{id}/history` | Full version history |
| GET | `/api/records/{id}/export` | Download bundle as ZIP |
| POST | `/api/cleanup` | Run retention cleanup (JSON body) |

---

## Running Tests

```bash
python -m pytest tests/unit -v
```

47 unit tests covering checksums, unique ID generation, validation, retry logic, and orphan tracking.
