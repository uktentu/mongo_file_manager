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
│   │   └── database.py        ← MongoDB connection manager + auto-reconnect
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
│   │   ├── report_id.py       ← UUID-based internal ID generator
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

The script base64-encodes your files and POSTs to `/api/seed/manifest` or `/api/seed/bundle`. Exits non-zero if any bundle fails — CI/CD pipeline friendly.

### Option B — CLI (seeder cloned locally / in monorepo)

```bash
python -m src.cli seed /path/to/regulation-repo/seeds/seed.yaml
```

---

## seed.yaml Format

Create a `seeds/seed.yaml` in your regulation repo. File paths are relative to the YAML file.

```yaml
bundles:
  # Routing is fully automatic — no report_id needed, ever.
  #
  # The composite key (csi_id + region + regulation + json_config filename) determines:
  #   No active record found  → CREATE  (internal UUID assigned)
  #   Record found, unchanged → SKIP    (idempotent re-run, nothing written)
  #   Record found, changed   → MODIFY  (new version, only changed files re-uploaded)
  #
  # json_config is always required:
  #   Its filename is the lookup key AND its content is checksum-compared.
  #   If the content changed, it is updated as part of the modification.

  - csi_id: "CSI-001"
    region: "APAC"
    regulation: "MAS-TRM"
    json_config: "configs/mas_trm_report.json"   # filename = lookup key
    sql_file:    "sql/mas_trm_query.sql"
    template:    "templates/mas_trm_template.txt"   # optional
```

---

## `report_id` — The Internal Identifier

Every record gets a **UUID v4** `report_id` (e.g. `a1b2c3d4-e5f6-7890-abcd-ef1234567890`) generated automatically on creation. You never supply or manage this value for seeding or modification.

- Use `report_id` for targeted operations: `fetch`, `history`, `export`, `cleanup`, API `PATCH`
- The composite key `(csi_id + regulation + region + json_config filename)` is the **user-facing primary key**
- The seeder automatically routes to CREATE / MODIFY / SKIP — no explicit `report_id` needed

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
seed.create  DONE report_id=a1b2c3d4-... csi_id=CSI-001 v1
seed.step3   'CSI-001' → CREATED  report_id=a1b2c3d4-... version=1
seed.done    total=3 created=1 updated=0 skipped=0 failed=2
```

All bundles are **pre-validated before any DB write**. Failures are collected and reported without blocking successful bundles.

---

## Validation Rules

| Layer | What is checked |
|---|---|
| Manifest | Root is a dict, has `bundles` list, list is non-empty |
| Bundle fields | Required keys present, non-empty, no illegal characters |
| File existence | Each referenced file exists, is a regular file, non-empty |
| Extensions | SQL → `.sql`; template → `.txt/.html/.jinja/.j2/.tmpl/.xml/.csv` |
| JSON config | Valid JSON, root is a dict, has `report.name` (non-empty string) |
| SQL content | Valid UTF-8, contains non-whitespace content |

---

## CLI Reference

```bash
# Seed from manifest (auto CREATE / MODIFY / SKIP)
python -m src.cli seed seeds/seed.yaml

# Create a single record
python -m src.cli create \
  --csi-id CSI-003 --region US --regulation SOX \
  --config path/to/config.json --sql path/to/query.sql

# Modify an existing record by composite key
# --config is always required (filename = lookup key)
python -m src.cli modify \
  --csi-id CSI-001 --region APAC --regulation MAS-TRM \
  --config configs/mas_trm_report.json \
  --sql new_query.sql          # optional

# List all active records
python -m src.cli list
python -m src.cli list --all      # include inactive versions

# Show full version history for a record
python -m src.cli history --report-id <UUID>

# Fetch a specific record
python -m src.cli fetch --report-id <UUID>
python -m src.cli fetch --region APAC
python -m src.cli fetch --csi-id CSI-001

# Export bundle files back to disk (with checksum verification)
python -m src.cli export --report-id <UUID> -o ./exported/
python -m src.cli export --report-id <UUID> -V 2 -o ./exported/       # specific version
python -m src.cli export --report-id <UUID> -o ./exported/ --file sql_file  # single file
python -m src.cli export --report-id <UUID> -o ./exported/ --force    # ignore checksum fail

# Clean up old versions (keep N most recent)
python -m src.cli cleanup --report-id <UUID> --keep 3 --dry-run
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
| `POST` | `/api/seed/manifest` | Seed multiple bundles (same structure as seed.yaml) |
| `POST` | `/api/cleanup` | Run retention cleanup |

Interactive docs available at `/docs` (Swagger UI) when running locally.

---

## Data Storage

```
MongoDB
├── metadata collection          ← Record metadata, version, checksums, file refs, audit log
│   ├── report_id: "<UUID>"      ← Internal UUID primary identifier (auto-generated)
│   ├── csi_id, region, regulation
│   ├── name                     ← from report.name in JSON config
│   ├── original_files           ← original filenames (json_config, sql_file, template)
│   ├── file_contents            ← GridFS ObjectIds for each file
│   ├── checksums                ← SHA-256 per file
│   ├── file_sizes               ← byte sizes
│   ├── active: true/false       ← only one active version per composite key
│   ├── version: 1, 2, 3...
│   └── audit_log: [...]         ← CREATED / MODIFIED / DEACTIVATED entries
└── fs (GridFS bucket)           ← Binary file storage (SQL, template, json_config)
```

**Indexes:**
- `report_id + active` (partial unique — one active record per report_id)
- `report_id + version` (version history lookups)
- `csi_id + regulation + region + original_files.json_config + active` (composite dedup key)
- Partial unique index: one active record per composite key — enforced at DB level
- `csi_id`, `region`, `regulation`, `active` (individual filter indexes)

---

## Safety Mechanisms

| Mechanism | What it does |
|---|---|
| **SHA-256 checksums** | Stored at upload time, re-verified on export to detect corruption |
| **Retry + backoff** | GridFS ops retry 3× with exponential delay (0.5s→1s→2s) on transient errors |
| **Orphan tracking** | If metadata insert fails after file upload, `GridFSOrphanTracker` deletes orphaned GridFS files |
| **Transaction support** | On replica sets: deactivate + insert happen atomically. On standalone: orphan tracking fallback |
| **Delta uploads** | On modify, only files whose checksum changed are re-uploaded — unchanged files reuse existing GridFS IDs |
| **Pre-validation** | All bundles validated before any DB write — one bad bundle doesn't block others |
| **Auto-reconnect** | `get_db()` pings the server on every call; stale TCP connections are automatically re-established |
| **Sentinel guard** | Counter sentinel document excluded from all aggregate, purge, list, and API queries |
| **Production guard** | `ENVIRONMENT=production` without `API_KEY` → boot fails with a clear error |
| **Secure logging** | MongoDB URI never logged (only host) — prevents credential leaks in logs |
