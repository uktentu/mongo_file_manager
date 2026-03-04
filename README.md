# MongoDB Document Seeder

A **standalone seeder engine** for regulatory document bundles. Regulation repos call this service (via HTTP or CLI) to store, version, and retrieve their config files (JSON configs, SQL queries, templates) in MongoDB — with full audit trails, SHA-256 checksums, and append-only versioning.

---

## Architecture — Standalone Service Model

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
         │               │              │
  MAS-TRM repo     BASEL repo     DORA repo
  (seed.yaml +    (seed.yaml +   (seed.yaml +
  POST to API)    CLI call)       POST to API)
```

External regulation repos **never write to a database directly**. They call this service (HTTP or CLI) with their config files — the seeder handles storage, versioning, and deduplication.

---

## Project Structure

```
├── src/
│   ├── cli.py                 ← CLI entry point (Click + Rich)
│   ├── api.py                 ← REST API (FastAPI)
│   ├── config/
│   │   ├── settings.py        ← Central config (all env vars, typed + validated)
│   │   ├── logging_config.py  ← Centralized logging setup (text / JSON)
│   │   └── database.py        ← MongoDB connection manager
│   ├── models/
│   │   └── schemas.py         ← Pydantic data models
│   ├── services/
│   │   ├── seed_service.py    ← Create & modify records (5-step flow)
│   │   ├── fetch_service.py   ← Query records by report_id / composite key
│   │   ├── export_service.py  ← Export bundles from GridFS to disk
│   │   ├── cleanup_service.py ← Purge old versions
│   │   ├── gridfs_service.py  ← GridFS upload/download/delete + retry
│   │   └── audit_service.py   ← Audit log entry factory
│   ├── utils/
│   │   ├── checksum.py        ← SHA-256 file hashing
│   │   ├── report_id.py       ← Atomic 7-digit ID generator
│   │   ├── validator.py       ← Layered manifest/bundle/file/schema validation
│   │   └── retry.py           ← Retry decorator with exponential backoff
│   └── errors/
│       └── exceptions.py      ← Custom exception hierarchy
├── integration/
│   └── seed_caller.py         ← Drop-in caller script for external repos
├── seeds/
│   └── seed.yaml              ← Template manifest (copy to your regulation repo)
├── .env.example               ← All supported environment variables with docs
├── Dockerfile
├── docker-compose.yml
└── entrypoint.sh              ← Server-only startup (no auto-seeding)
```

---

## Quick Start

### 1. Configure Environment

```bash
cp .env.example .env
# Edit .env — at minimum set MONGO_URI and MONGO_DB_NAME
```

> See [Environment Variables](#environment-variables) for the full list.

### 2a. Run with Docker (recommended)

```bash
docker-compose up -d --build
```

### 2b. Run Manually (development)

```bash
pip install -r requirements.txt
uvicorn src.api:app --reload --port 8000
# or
gunicorn src.api:app --workers 2 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

The container **starts the API server only** — seeding is triggered on-demand by external regulation repos.

---

## Environment Variables

All config lives in `src/config/settings.py`. Every variable is documented in `.env.example`.

| Variable | Type | Default | Required in Prod |
|---|---|---|---|
| `MONGO_URI` | str | `mongodb://localhost:27017` | ✅ |
| `MONGO_DB_NAME` | str | `doc_management` | — |
| `MONGO_MAX_POOL_SIZE` | int | `50` | — |
| `MONGO_CONNECT_TIMEOUT_MS` | int | `5000` | — |
| `MONGO_SERVER_TIMEOUT_MS` | int | `5000` | — |
| `API_KEY` | str | `""` (auth off) | ✅ (enforced) |
| `API_HOST` | str | `0.0.0.0` | — |
| `API_PORT` | int | `8000` | — |
| `API_WORKERS` | int | `2` | — |
| `LOG_LEVEL` | str | `INFO` | — |
| `LOG_FORMAT` | str | `text` | — (`json` for aggregators) |
| `ENVIRONMENT` | str | `development` | — |

> **Production guard:** `ENVIRONMENT=production` with no `API_KEY` → startup fails immediately.

---

## How External Repos Call This Service

### Option A — HTTP API (deployed service)

Copy `integration/seed_caller.py` into your regulation repo:

```bash
# Install PyYAML in your regulation repo if using manifest mode
pip install pyyaml

# Seed from your seed.yaml
SEEDER_BASE_URL=https://seeder.internal \
SEEDER_API_KEY=your-secret-key \
  python integration/seed_caller.py manifest seeds/seed.yaml

# Or seed a single bundle inline
SEEDER_BASE_URL=https://seeder.internal \
SEEDER_API_KEY=your-secret-key \
  python integration/seed_caller.py bundle \
    --csi-id CSI-001 --region APAC --regulation MAS-TRM \
    --config configs/report.json --sql sql/query.sql
```

The script base64-encodes your files and POSTs to the seeder's `/api/seed/manifest` or `/api/seed/bundle`. Exits non-zero if any bundle fails — CI/CD pipeline friendly.

### Option B — CLI (seeder cloned locally / in monorepo)

```bash
python -m src.cli seed /path/to/regulation-repo/seeds/seed.yaml
```

---

## seed.yaml Format

Create a `seeds/seed.yaml` in your regulation repo. File paths are relative to the YAML file.

```yaml
bundles:
  # CREATE — no report_id → auto-generates 7-digit report_id on first run
  - csi_id: "CSI-001"
    region: "APAC"
    regulation: "MAS-TRM"
    json_config: "configs/mas_trm_report.json"
    sql_file:    "sql/mas_trm_query.sql"
    template:    "templates/mas_trm_template.txt"   # optional

  # MODIFY — supply the report_id from the first CREATED output to target a specific record
  - csi_id: "CSI-001"
    region: "APAC"
    regulation: "MAS-TRM"
    json_config: "configs/mas_trm_report_v2.json"
    sql_file:    "sql/mas_trm_query.sql"
    report_id:   "0000001"    # locks to this exact record
```

**Deduplication** (when `report_id` is not supplied): matched by `(csi_id + regulation + region)`.
- Checksums match → **SKIPPED**
- Any file changed → **UPDATED** (new version, only changed files re-uploaded)
- No existing record → **CREATED** (report_id printed)

---

## `report_id` — The Primary Identifier

Every record gets a **7-digit, zero-padded** `report_id` (e.g. `0000001`) generated atomically on first creation using a MongoDB counter.

- Use `report_id` for all subsequent operations: fetch, history, export, modify, cleanup
- Supply `report_id` in `seed.yaml` to explicitly target a record for modification
- The composite key `(csi_id + regulation + region)` is used for deduplication if `report_id` is omitted

---

## Seeding Flow (5 Steps)

```
seed.start   manifest=seeds/seed.yaml
seed.step1   Validating manifest structure
seed.step1   OK — 3 bundle(s) found
seed.step2   Pre-validating all bundles before database operations
seed.step2   [1/3] CSI-001 — fields/files OK
seed.step2   [2/3] CSI-002 — VALIDATION FAILED: sql_file not found
seed.step3   Processing 2 validated bundle(s)
seed.step3   ── Bundle [1/2] 'CSI-001' ──
seed.create  DONE report_id=0000001 csi_id=CSI-001 v1
seed.step3   'CSI-001' → CREATED  report_id=0000001 version=1
seed.done    total=3 created=1 updated=0 skipped=0 failed=2
```

All bundles are **pre-validated before any DB write**. Failures are collected and reported without rolling back successful bundles.

---

## Validation Rules

| Layer | What is checked |
|---|---|
| Manifest | Root is a dict, has `bundles` list, list is non-empty |
| Bundle fields | Required keys present, non-empty, no illegal characters |
| `report_id` (if supplied) | Must be exactly 7 digits, zero-padded |
| File existence | Each referenced file exists, is a regular file, non-empty |
| Extensions | SQL → `.sql`; template → `.txt/.html/.jinja/.j2/.tmpl/.xml/.csv` |
| JSON config | Valid JSON, root is a dict, has `name` and `outFileName` (non-empty strings) |
| SQL content | Valid UTF-8, contains non-whitespace content |

---

## CLI Reference

```bash
# Seed from manifest
python -m src.cli seed seeds/seed.yaml

# Create a single record
python -m src.cli create \
  --csi-id CSI-003 --region US --regulation SOX \
  --config path/to/config.json --sql path/to/query.sql

# Modify an existing record by report_id
python -m src.cli modify --report-id 0000001 --sql new_query.sql

# List all active records
python -m src.cli list
python -m src.cli list --all      # include inactive versions

# Show full version history for a record
python -m src.cli history --report-id 0000001

# Fetch a specific record
python -m src.cli fetch --report-id 0000001
python -m src.cli fetch --region APAC
python -m src.cli fetch --csi-id CSI-001

# Export bundle files back to disk (with checksum verification)
python -m src.cli export --report-id 0000001 -o ./exported/
python -m src.cli export --report-id 0000001 -V 2 -o ./exported/      # specific version
python -m src.cli export --report-id 0000001 -o ./exported/ --force   # ignore checksum failures

# Clean up old versions (keep N most recent)
python -m src.cli cleanup --report-id 0000001 --keep 3 --dry-run
python -m src.cli cleanup --all --keep 3
python -m src.cli cleanup --max-age-days 90

# Debug logging
python -m src.cli -v seed seeds/seed.yaml
```

---

## REST API

All endpoints (except `/api/health`) require `X-API-Key` header when `API_KEY` is set.

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | Health check, DB status, transaction support |
| `GET` | `/api/records` | List records (filters: `region`, `regulation`, `csi_id`, `active_only`) |
| `GET` | `/api/records/{report_id}` | Fetch active record (optional `?version=N`) |
| `GET` | `/api/records/{report_id}/history` | Full version history |
| `GET` | `/api/records/{report_id}/export` | Download bundle as ZIP |
| `PATCH` | `/api/records/{report_id}` | Modify record with inline base64-encoded files |
| `POST` | `/api/seed/bundle` | Seed a single bundle (base64 files in JSON body) |
| `POST` | `/api/seed/manifest` | Seed multiple bundles at once (same structure as seed.yaml) |
| `POST` | `/api/cleanup` | Run retention cleanup |

Interactive docs available at `/docs` (Swagger UI) when running locally.

---

## Data Storage

```
MongoDB
├── metadata collection          ← Record metadata, version, checksums, file refs, audit log
│   ├── report_id: "0000001"     ← 7-digit primary identifier
│   ├── csi_id, region, regulation
│   ├── name, out_file_name      ← from JSON config
│   ├── file_contents            ← GridFS IDs for each file
│   ├── checksums                ← SHA-256 per file
│   ├── file_sizes, original_files
│   ├── active: true/false       ← only one active version per (csi_id+regulation+region)
│   ├── version: 1, 2, 3...
│   └── audit_log: [...]         ← CREATED / MODIFIED / DEACTIVATED entries
└── fs (GridFS bucket)           ← Binary file storage
    ├── JSON config files
    ├── SQL query files
    └── Template files
```

**Indexes:**
- `report_id + active` (partial unique — enforces one active record per report_id)
- `report_id + version` (version history lookups)
- `csi_id + regulation + region + active` (composite dedup key)
- `csi_id`, `region`, `regulation`, `active` (individual filter indexes)

---

## Safety Mechanisms

| Mechanism | What it does |
|---|---|
| **SHA-256 checksums** | Stored at upload time, re-verified on export to detect corruption |
| **Retry + backoff** | GridFS ops retry 3× with exponential delay (0.5s→1s→2s) on transient errors |
| **Orphan tracking** | If metadata insert fails after file upload, `GridFSOrphanTracker` deletes the orphaned GridFS files |
| **Transaction support** | On replica sets: deactivate + insert happen atomically. On standalone: orphan tracking fallback |
| **Delta uploads** | On modify, only files whose checksum changed are re-uploaded — unchanged files reuse existing GridFS IDs |
| **Pre-validation** | All bundles validated before any DB write — one bad bundle doesn't block others |
| **Production guard** | `ENVIRONMENT=production` without `API_KEY` → boot fails with a clear error |
| **Secure logging** | MongoDB URI never logged (only host) — prevents credential leaks in logs |
