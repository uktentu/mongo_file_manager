# MongoDB Document Seeder

A **standalone seeder engine** for regulatory document bundles. Regulation repos call this service (via HTTP or CLI) to store, version, and retrieve their config files (JSON configs, SQL queries, templates) in MongoDB — with full audit trails, SHA-256 checksums, and append-only versioning.

---

## Architecture

```
┌────────────────────────────────────────┐
│      Central Seeder Engine (this repo) │
│                                        │
│  ┌──────────────┐  ┌────────────────┐  │
│  │  FastAPI API │  │  Click CLI     │  │
│  └──────┬───────┘  └──────┬─────────┘  │
│         │                 │            │
│  ┌──────▼─────────────────▼─────────┐  │
│  │   Services (seed / fetch /       │  │
│  │   export / cleanup)              │  │
│  └──────────────────────────────────┘  │
│  ┌──────────────────────────────────┐  │
│  │   MongoDB (metadata + GridFS)    │  │
│  └──────────────────────────────────┘  │
└────────────────────────────────────────┘
         ▲               ▲              ▲
  MAS-TRM repo     BASEL repo     DORA repo
  (seed.yaml /     (seed.yaml /   (seed.yaml /
  HTTP API)        CLI)           HTTP API)
```

External regulation repos **never write to MongoDB directly**. They push file content to this service via HTTP or CLI — the seeder handles storage, versioning, deduplication, and audit logging.

---

## Project Structure

```
├── src/
│   ├── api.py                 ← FastAPI REST endpoints
│   ├── cli.py                 ← Click + Rich CLI commands
│   ├── config/
│   │   ├── settings.py        ← All env vars (typed + validated via Pydantic)
│   │   ├── logging_config.py  ← Structured logging (text / JSON)
│   │   └── database.py        ← MongoDB connection + auto-reconnect + indexes
│   ├── models/
│   │   └── schemas.py         ← Pydantic data models (MetadataDocument etc.)
│   ├── services/
│   │   ├── seed_service.py    ← Bulk seeding + single create/modify (5-step flow)
│   │   ├── fetch_service.py   ← Query by report_id / composite key / region etc.
│   │   ├── export_service.py  ← Download bundle files from GridFS to disk
│   │   ├── cleanup_service.py ← Version retention + age-based purging
│   │   ├── gridfs_service.py  ← GridFS upload / download / delete + retry
│   │   └── audit_service.py   ← Audit log entry factory
│   ├── utils/
│   │   ├── checksum.py        ← SHA-256 hashing (file + bytes)
│   │   ├── report_id.py       ← UUID v4 internal ID generator
│   │   ├── validator.py       ← 6-layer validation (manifest → file → schema)
│   │   └── retry.py           ← Exponential backoff decorator for MongoDB ops
│   └── errors/
│       └── exceptions.py      ← Custom exception hierarchy
├── integration/
│   └── seed_caller.py         ← Drop-in HTTP caller for external regulation repos
├── seeds/
│   └── seed.yaml              ← Manifest template
├── .env.example               ← All supported environment variables with docs
├── Dockerfile
├── docker-compose.yml
└── entrypoint.sh
```

---

## Quick Start

```bash
cp .env.example .env          # Set MONGO_URI and optionally API_KEY

# Run with Docker
docker-compose up -d --build

# Or run manually
pip install -r requirements.txt
uvicorn src.api:app --reload --port 8000
```

---

## Environment Variables

| Variable | Default | Required in Prod | Description |
|---|---|---|---|
| `MONGO_URI` | `mongodb://localhost:27017` | ✅ | Full MongoDB connection string |
| `MONGO_DB_NAME` | `doc_management` | — | Target database name |
| `MONGO_METADATA_COLLECTION` | `metadata` | — | Metadata collection name |
| `MONGO_GRIDFS_BUCKET` | `fs` | — | GridFS bucket name |
| `MONGO_MAX_POOL_SIZE` | `50` | — | Connection pool ceiling |
| `MONGO_CONNECT_TIMEOUT_MS` | `5000` | — | Connection timeout |
| `MONGO_SERVER_TIMEOUT_MS` | `5000` | — | Server selection timeout |
| `API_KEY` | `""` (auth off) | ✅ | X-API-Key header value |
| `API_HOST` | `0.0.0.0` | — | Bind address |
| `API_PORT` | `8000` | — | Bind port |
| `API_WORKERS` | `2` | — | Gunicorn worker count |
| `LOG_LEVEL` | `INFO` | — | `DEBUG/INFO/WARNING/ERROR` |
| `LOG_FORMAT` | `text` | — | `text` or `json` |
| `ENVIRONMENT` | `development` | — | `development/staging/production` |

> **Production guard:** `ENVIRONMENT=production` without `API_KEY` → boot fails immediately.

---

## seed.yaml Format

```yaml
bundles:
  # Routing is fully automatic — no report_id needed.
  # The composite key (csi_id + region + regulation + json_config filename) decides:
  #   No active record  → CREATE  (UUID assigned internally)
  #   Record found, unchanged  → SKIP  (idempotent re-run)
  #   Record found, file changed → MODIFY  (new version, delta-upload)

  - csi_id: "CSI-001"
    region: "APAC"
    regulation: "MAS-TRM"
    json_config: "configs/mas_trm_report.json"   # filename = lookup key
    sql_file:    "sql/mas_trm_query.sql"
    template:    "templates/mas_trm_template.txt"  # optional
```

---

## `report_id` — Internal UUID Identifier

Every record receives a **UUID v4** `report_id` (e.g. `a1b2c3d4-e5f6-7890-abcd-ef1234567890`) generated on first creation. You **never supply** this for seeding or modification — routing is automatic.

Use `report_id` only for targeted operations: `fetch`, `history`, `export`, `cleanup`, and the `PATCH /api/records/{report_id}` endpoint.

---

## Seeding Flow — 5 Steps

```
Step 1  Load YAML → validate manifest structure
Step 2  Pre-validate ALL bundles (collect errors before any DB write)
Step 3  For each valid bundle:
          a. Compute SHA-256 checksums for all files
          b. Resolve existing record by composite key
          c. No active record → CREATE  (upload all files, assign UUID)
          d. Checksums unchanged → SKIP  (idempotent, nothing written)
          e. Any checksum changed → MODIFY  (delta-upload only changed files, new version)
Step 4  Log per-bundle result + summary
Step 5  Return structured dict: created/updated/skipped/failed + details[]
```

---

## CLI Reference

```bash
# ── Seed ──────────────────────────────────────────────────────────────────────
python -m src.cli seed seeds/seed.yaml          # bulk seed from manifest
python -m src.cli -v seed seeds/seed.yaml       # verbose (DEBUG logging)

# ── Create ────────────────────────────────────────────────────────────────────
python -m src.cli create \
  --csi-id CSI-003 --region US --regulation SOX \
  --config configs/sox.json --sql sql/sox.sql \
  --template templates/sox.txt                  # optional

# ── Modify ────────────────────────────────────────────────────────────────────
# --config is always required (filename = composite key lookup)
python -m src.cli modify \
  --csi-id CSI-001 --region APAC --regulation MAS-TRM \
  --config configs/mas_trm_report.json \
  --sql sql/updated_query.sql \                 # optional
  --template templates/updated.txt              # optional

# ── List ──────────────────────────────────────────────────────────────────────
python -m src.cli list                          # all active records
python -m src.cli list --all                    # include inactive versions

# ── Fetch ─────────────────────────────────────────────────────────────────────
python -m src.cli fetch --report-id <UUID>      # by internal UUID
python -m src.cli fetch --csi-id CSI-001        # by CSI ID (shows list)
python -m src.cli fetch --region APAC           # by region
python -m src.cli fetch --regulation MAS-TRM    # by regulation

# ── History ───────────────────────────────────────────────────────────────────
python -m src.cli history --report-id <UUID>    # all versions + audit log

# ── Export ────────────────────────────────────────────────────────────────────
python -m src.cli export --report-id <UUID> -o ./out/          # all files
python -m src.cli export --report-id <UUID> -o ./out/ -V 2     # specific version
python -m src.cli export --report-id <UUID> -o ./out/ \
  --file sql_file                                # single file only
python -m src.cli export --report-id <UUID> -o ./out/ --force  # skip checksum abort

# ── Cleanup ───────────────────────────────────────────────────────────────────
python -m src.cli cleanup --report-id <UUID> --keep 3 --dry-run  # preview
python -m src.cli cleanup --report-id <UUID> --keep 3            # live
python -m src.cli cleanup --all --keep 3                         # all records
python -m src.cli cleanup --max-age-days 90                      # age-based
```

---

## REST API

All endpoints (except `/api/health`) require the `X-API-Key` header when `API_KEY` is set.

### Records

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | Health check — DB ping, transaction support, timestamp |
| `GET` | `/api/records` | List records with optional filters and pagination |
| `GET` | `/api/records/{report_id}` | Fetch a specific record (active version by default) |
| `GET` | `/api/records/{report_id}/history` | Full version history with audit log |
| `GET` | `/api/records/{report_id}/export` | Download bundle as ZIP (streams response) |
| `PATCH` | `/api/records/{report_id}` | Modify a record by UUID with base64-encoded files |

### Seeding

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/seed/bundle` | Seed a single bundle (base64 inline) |
| `POST` | `/api/seed/manifest` | Seed multiple bundles at once |

### Cleanup

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/cleanup` | Run retention cleanup (by report_id, global, or max age) |

Interactive docs: `http://localhost:8000/docs` (Swagger UI)

---

### `GET /api/records` — Query Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `active_only` | bool | `true` | When false, includes inactive versions |
| `region` | string | — | Filter by region |
| `regulation` | string | — | Filter by regulation |
| `csi_id` | string | — | Filter by CSI ID |
| `limit` | int | `100` (max 1000) | Page size |
| `skip` | int | `0` | Offset for pagination |

**Response:**
```json
{
  "records": [...],
  "total": 42,
  "limit": 100,
  "skip": 0
}
```

---

### `POST /api/seed/bundle` — Request Body

```json
{
  "csi_id": "CSI-001",
  "region": "APAC",
  "regulation": "MAS-TRM",
  "json_config_filename": "mas_trm_report.json",
  "json_config_content": "<base64-encoded bytes>",
  "sql_file_filename": "mas_trm_query.sql",
  "sql_file_content": "<base64-encoded bytes>",
  "template_filename": "mas_trm_template.txt",   // optional
  "template_content": "<base64-encoded bytes>"   // optional
}
```

**Response (201):**
```json
{ "status": "created", "report_id": "<UUID>", "version": 1, "reason": "Initial seed" }
```
`status` is one of: `"created"`, `"updated"`, `"skipped"`

---

### `PATCH /api/records/{report_id}` — Request Body

```json
{
  "json_config_filename": "mas_trm_report.json",
  "json_config_content": "<base64>",   // optional
  "sql_file_filename": "updated.sql",
  "sql_file_content": "<base64>",      // optional
  "template_filename": "new.txt",
  "template_content": "<base64>"       // optional
}
```
At least one file must be provided. `report_id` must be a valid UUID.

---

### `POST /api/cleanup` — Request Body

```json
{
  "report_id": "<UUID>",    // optional — target one record
  "purge_all": false,       // optional — sweep all records
  "keep_versions": 3,       // versions to retain (default: 3)
  "max_age_days": 90,       // purge inactive versions older than N days
  "dry_run": false          // preview without writing
}
```
Specify exactly one of: `report_id`, `purge_all: true`, or `max_age_days`.

---

## Service Methods

### `seed_service`

| Function | Description |
|---|---|
| `seed_from_manifest(manifest_path)` | Load YAML, validate all bundles, process each → CREATE / MODIFY / SKIP |
| `create_single_record(csi_id, region, regulation, json_config_path, sql_file_path, template_path?)` | Create one record via composite key (used by CLI `create`) |
| `modify_record_by_id(report_id, json_config_path?, sql_file_path?, template_path?)` | Modify by internal UUID; delta-uploads only changed files |
| `_process_bundle(bundle, config)` | Core router: SKIP / MODIFY / CREATE per bundle |
| `_create_record(bundle, config, db)` | Insert new metadata doc + upload all files to GridFS (transactional) |
| `_modify_record(report_id, ..., db)` | Deactivate old version, insert new version, delta-upload changed files |

### `fetch_service`

| Function | Description |
|---|---|
| `fetch_active_by_report_id(report_id)` | Return active record by UUID |
| `fetch_by_csi_id(csi_id, active_only, limit)` | List records matching CSI ID |
| `fetch_by_region(region, active_only, limit)` | List records matching region |
| `fetch_by_regulation(regulation, active_only, limit)` | List records matching regulation |
| `fetch_by_composite(filters, active_only, limit)` | Multi-field filter query |
| `fetch_version_history(report_id)` | All versions (active + inactive) sorted by version |
| `list_all_active(limit)` | Projection-only list of all active records |

### `export_service`

| Function | Description |
|---|---|
| `export_bundle(report_id, output_dir, version?, verify_checksums, force, files?)` | Download selected files from GridFS to disk. `files` set controls which file types to export (`json_config`, `sql_file`, `template`). Checksum verification on every download. |

### `cleanup_service`

| Function | Description |
|---|---|
| `purge_old_versions(report_id, keep_versions, dry_run)` | Purge old inactive versions for one record, keeping N most recent |
| `purge_all_old_versions(keep_versions, dry_run)` | Global sweep across all logical records |
| `purge_by_age(max_age_days, dry_run)` | Purge all inactive records older than N days |

### `gridfs_service`

| Function | Description |
|---|---|
| `upload_to_gridfs(bucket, file_path, original_filename, content_type, extra_metadata?, orphan_tracker?)` | Upload file with retry (3×), checksum metadata, orphan tracking |
| `download_from_gridfs(bucket, gridfs_id)` | Download bytes + metadata with retry (3×) |
| `delete_from_gridfs(bucket, gridfs_id)` | Delete a GridFS file by ObjectId |
| `GridFSOrphanTracker` | Context helper: tracks upload IDs and bulk-deletes them on failure |

### `database` (`DatabaseManager`)

| Method / Property | Description |
|---|---|
| `connect()` | Open MongoClient, ping, detect transaction support, create all indexes |
| `close()` | Close client, release pool |
| `start_session()` | Open a MongoDB client session (for transactions) |
| `.metadata_collection` | Returns the configured metadata collection handle |
| `.fs` | Returns the configured GridFS handle |
| `.supports_transactions` | `True` if connected to a replica set or mongos |
| `get_db()` | Module-level singleton: returns connected manager, reconnects on stale TCP |
| `create_db_manager(uri?, db_name?)` | Create and connect a new `DatabaseManager` |
| `set_db(instance)` | Override global singleton (testing) |
| `reset_db()` | Close and clear global singleton |

### `validator`

| Function | Description |
|---|---|
| `validate_manifest_structure(manifest, source)` | Ensures root is dict with non-empty `bundles` list |
| `validate_seed_bundle(bundle, base_dir, index)` | Required field check, token format, file existence, extension allowlist |
| `validate_json_config(path, index?)` | Valid JSON, root is dict, has non-empty `report.name` |
| `validate_sql_content(path, index?)` | UTF-8 readable, non-whitespace content |

### `retry`

| Function | Description |
|---|---|
| `retry_on_failure(max_retries, base_delay, max_delay, backoff_factor, retryable_exceptions)` | Decorator — retries on `AutoReconnect`, `ConnectionFailure`, `NetworkTimeout`, `ServerSelectionTimeoutError` with exponential backoff |

---

## Custom Exceptions

| Exception | HTTP Status | When raised |
|---|---|---|
| `SeederError` | 500 | Base class for all domain errors |
| `ValidationError` | 400 | Manifest / bundle / file / schema validation failure |
| `FileNotFoundError` | 400 | Referenced file path does not exist |
| `DuplicateRecordError` | 409 | Attempt to create a record with an existing composite key |
| `DatabaseError` | 500 | MongoDB connection or query failure |
| `GridFSError` | 500 | GridFS upload / download / delete failure |
| `ChecksumMismatchError` | 500 | Stored vs re-computed checksum mismatch on export |
| `RecordNotFoundError` | 404 | No record found for given report_id / composite key |

---

## Data Storage

```
MongoDB
├── metadata collection
│   ├── report_id               ← UUID v4 (internal, auto-generated)
│   ├── csi_id, region, regulation
│   ├── name                    ← from report.name in json_config
│   ├── original_files          ← {json_config, sql_file, template} filenames
│   ├── file_contents           ← {json_config_id, sql_file_id, template_id} GridFS ObjectIds
│   ├── checksums               ← SHA-256 per file
│   ├── file_sizes              ← byte sizes per file
│   ├── active: true/false      ← only one active version per composite key
│   ├── version: 1, 2, 3 ...
│   ├── uploaded_at             ← UTC datetime
│   └── audit_log: [
│         { action: "CREATED", reason: "...", timestamp, version },
│         { action: "MODIFIED", reason: "changed: sql_file", timestamp, version },
│         { action: "DEACTIVATED", reason: "Superseded by version N", timestamp }
│       ]
└── fs (GridFS)                 ← Binary storage (no size limit per file)
    ├── fs.files                ← GridFS file metadata
    └── fs.chunks               ← Binary data in 255KB chunks
```

**Indexes:**

| Index | Type | Purpose |
|---|---|---|
| `report_id + active` (partial, active=true) | Unique | One active version per report_id |
| `report_id + active` | Compound | Fast report_id + active lookups |
| `report_id + version` | Compound | Version history queries |
| `csi_id + regulation + region + original_files.json_config` | Compound | Composite key dedup |
| `csi_id + regulation + region + json_config` (partial, active=true) | Unique | One active per composite key |
| `csi_id`, `region`, `regulation`, `active` | Single-field | Filter queries |

---

## Safety Mechanisms

| Mechanism | Detail |
|---|---|
| **SHA-256 checksums** | Stored at upload; re-verified on export — detects GridFS corruption |
| **Delta uploads** | MODIFY re-uploads only changed files; unchanged files reuse existing GridFS ObjectIds |
| **Transaction support** | On replica sets: old-version deactivation + new-version insert are atomic |
| **Orphan tracking** | On standalone: `GridFSOrphanTracker` deletes uploaded files if metadata insert fails |
| **Pre-validation** | All bundles validated before any DB write — one bad bundle never blocks others |
| **Exponential retry** | GridFS ops retry 3× at 0.5s → 1s → 2s on transient network errors |
| **Auto-reconnect** | `get_db()` pings server; stale TCP connections are silently replaced |
| **Sentinel guard** | Counter sentinel doc `_id="report_id_seq"` excluded from all queries, purges, and API results |
| **Production guard** | `ENVIRONMENT=production` without `API_KEY` → process exits at startup |
| **Secure URI logging** | Full MongoDB URI never logged; only host is shown |
| **UUID format validation** | `PATCH /api/records/{report_id}` validates UUID format before DB lookup |

---

## How External Repos Call This Service

### Option A — HTTP API (deployed)

```bash
# From your regulation repo's CI/CD pipeline:
SEEDER_BASE_URL=https://seeder.internal \
SEEDER_API_KEY=your-secret-key \
  python integration/seed_caller.py manifest seeds/seed.yaml

# Single bundle inline:
SEEDER_BASE_URL=https://seeder.internal \
SEEDER_API_KEY=your-secret-key \
  python integration/seed_caller.py bundle \
    --csi-id CSI-001 --region APAC --regulation MAS-TRM \
    --config configs/report.json --sql sql/query.sql
```

### Option B — CLI (monorepo / local)

```bash
python -m src.cli seed /path/to/regulation-repo/seeds/seed.yaml
```

---

## Validation Rules

| Layer | What is checked |
|---|---|
| Manifest | Root is a dict, has `bundles` key, list is non-empty |
| Bundle fields | `csi_id`, `region`, `regulation`, `json_config`, `sql_file` — present, non-empty, `[A-Za-z0-9_\-.]` only |
| File existence | All referenced files exist, are regular files, and are non-empty |
| SQL extension | Must be `.sql` |
| Template extension | `.txt`, `.html`, `.jinja`, `.j2`, `.tmpl`, `.xml`, `.csv` |
| JSON config | Valid JSON, root object, has `report.name` (non-empty string) |
| SQL content | Valid UTF-8, contains non-whitespace content |
