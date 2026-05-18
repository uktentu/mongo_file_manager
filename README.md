# MongoDB Document Seeder

> **Full interactive architecture (diagrams, flows, WHY reasoning):** open [`docs/index.html`](./docs/index.html) in a browser.
>
> **Presentation deck:** [`docs/mongo_seeder_presentation.pptx`](./docs/mongo_seeder_presentation.pptx)
>
> **Seed config reference:** [`SEED_CONFIG_GUIDE.md`](./SEED_CONFIG_GUIDE.md)

A **standalone seeder engine** for regulatory document bundles. Regulation repos call this service — via HTTP API or CLI — to store, version, deduplicate, and retrieve their configuration files (JSON configs, SQL queries, templates) in MongoDB, with full audit trails, SHA-256 checksums, and append-only versioning.

---

## Table of Contents

1. [What This Service Does](#what-this-service-does)
2. [Quick Start](#quick-start)
3. [Running with run.sh](#running-with-runsh)
4. [Architecture](#architecture)
5. [Project Structure](#project-structure)
6. [Environment Variables](#environment-variables)
7. [Composite report\_id](#composite-report_id)
8. [Seeding Flow — 5 Steps](#seeding-flow--5-steps)
9. [Routing Logic — CREATE / MODIFY / SKIP](#routing-logic--create--modify--skip)
10. [6-Layer Validation Pipeline](#6-layer-validation-pipeline)
11. [seed.yaml Format](#seedyaml-format)
12. [CLI Reference](#cli-reference)
13. [REST API Reference](#rest-api-reference)
14. [API Endpoint Details](#api-endpoint-details)
15. [Service Layer Reference](#service-layer-reference)
16. [Custom Exceptions](#custom-exceptions)
17. [MongoDB Data Schema](#mongodb-data-schema)
18. [Indexes](#indexes)
19. [GridFS Binary Storage](#gridfs-binary-storage)
20. [Safety Mechanisms](#safety-mechanisms)
21. [ReportGen Integration](#reportgen-integration)
22. [Technology Decisions](#technology-decisions)
23. [Troubleshooting](#troubleshooting)

---

## What This Service Does

Regulation repos (MAS-TRM, GDPR, DORA, Basel, SOX, etc.) each have a set of files that define how their reports are generated:

- A **JSON config** — report parameters, column definitions, filters
- A **SQL query** — the data-fetch template
- An optional **template** — HTML/CSV/Jinja2 output structure

Without a central store these files are:
- Scattered across repos with no version history
- Re-downloaded every run — temp file management in every consumer
- Overwritten on update — no rollback, no audit trail
- No deduplication — same unchanged file re-uploaded every CI run

**This service fixes all of that:**

| Problem | Solution |
|---------|----------|
| No versioning | Append-only: old versions deactivated, never deleted |
| No deduplication | SHA-256 delta detection — only changed files re-uploaded |
| Temp file mess | ReportGenClient reads directly from GridFS into memory (~5ms/file) |
| No atomicity | MongoDB transactions (replica sets) for deactivate + insert |
| Diverging clients | One SDK (`ReportGenClient`) shared across all consuming repos |

---

## Quick Start

```bash
# 1. Clone and enter the repo
git clone <repo-url>
cd Mongo

# 2. Copy environment config
cp .env.example .env
# Edit .env — set MONGO_URI at minimum

# 3. Create a virtual environment and install deps
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 4. Start with the orchestration script (recommended)
chmod +x run.sh
./run.sh

# Or start manually
uvicorn src.api:app --host 0.0.0.0 --port 3089 --reload
```

After startup:

| URL | Purpose |
|-----|---------|
| `http://localhost:3089/api/health` | Health check — DB ping, transaction support |
| `http://localhost:3089/api/docs` | Swagger UI (auto-generated, dev only) |
| `http://localhost:3089/api/details` | Interactive architecture document |
| `http://localhost:3089/api/records` | List all active records |

---

## Running with run.sh

`run.sh` is the recommended way to start the service. It orchestrates the full startup in 9 steps:

```bash
./run.sh
```

### What run.sh does

| Step | Action |
|------|--------|
| **1** | Checks Python 3 is available; prints version |
| **2** | Verifies `.venv/` exists — creates it if missing |
| **3** | Checks `.env` exists — copies `.env.example` if missing |
| **4** | Kills any existing process on **port 3089** (`lsof` + `kill -9`) |
| **5** | Removes stale `.seeder.pid` file |
| **6** | Finds and deletes all `__pycache__/` directories and `.pyc` files (skips `.venv/`) |
| **7** | Activates `.venv`, runs `pip install -r requirements.txt` silently |
| **8** | Archives any existing `output.log` → `output_YYYYMMDD_HHMMSS.log` |
| **9** | Launches `uvicorn` via `nohup` → `output.log`, saves PID to `.seeder.pid` |
| **+** | Polls `GET /api/health` every second for up to 15 s; exits with error if server never starts |
| **+** | Prints the success summary with all URLs, then `tail -f output.log` |

### After a successful start

```
  PID            : 12345  (saved to .seeder.pid)
  Health check   : http://127.0.0.1:3089/api/health
  API docs       : http://127.0.0.1:3089/api/docs
  Architecture   : http://127.0.0.1:3089/api/details
  Records        : http://127.0.0.1:3089/api/records
  Live log       : tail -f output.log

  To stop: kill $(cat .seeder.pid)
```

Press **Ctrl-C** to detach from the log tail — the server keeps running in the background.

### Files created by run.sh

| File | Purpose |
|------|---------|
| `output.log` | All uvicorn + application log output |
| `output_YYYYMMDD_HHMMSS.log` | Rotated previous log (if existed) |
| `.seeder.pid` | PID of the background uvicorn process |

---

## Architecture

```
┌────────────────────────────────────────────────────────┐
│               MongoDB Document Seeder                  │
│                                                        │
│  ┌─────────────────────┐  ┌─────────────────────────┐ │
│  │   FastAPI REST API  │  │   Click + Rich CLI      │ │
│  │   src/api.py        │  │   src/cli.py            │ │
│  └──────────┬──────────┘  └────────────┬────────────┘ │
│             │                          │               │
│  ┌──────────▼──────────────────────────▼────────────┐ │
│  │              Service Layer                        │ │
│  │  seed_service   fetch_service   export_service    │ │
│  │  cleanup_service   gridfs_service   audit_service │ │
│  └──────────────────────────┬────────────────────────┘ │
│                             │                          │
│  ┌──────────────────────────▼────────────────────────┐ │
│  │                  MongoDB                          │ │
│  │   metadata collection       GridFS bucket (fs)   │ │
│  │   (version docs, audit)     (binary files)       │ │
│  └──────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────┘
         ▲               ▲              ▲
  MAS-TRM repo     BASEL repo     DORA repo
  (seed.yaml /     (seed.yaml /   (seed.yaml /
  HTTP API)        CLI)           HTTP API)
```

External regulation repos **never write to MongoDB directly**. They push file content to this service via HTTP or CLI — the seeder handles storage, versioning, deduplication, and audit logging.

ReportGen reads files back **directly from GridFS** (no temp files) via the embedded `ReportGenClient`.

---

## Project Structure

```
Mongo/
├── run.sh                         ← Startup orchestration (kill port, clean cache, nohup)
├── output.log                     ← Live application log (created by run.sh)
├── .seeder.pid                    ← PID of the running server (created by run.sh)
│
├── src/
│   ├── api.py                     ← FastAPI app: routes, middleware, auth, serialization
│   ├── cli.py                     ← Click + Rich CLI: seed, create, modify, list, fetch,
│   │                                  history, export, cleanup
│   ├── config/
│   │   ├── settings.py            ← Typed env-var config (validated at startup)
│   │   ├── logging_config.py      ← Structured logging: text (dev) / JSON (prod)
│   │   └── database.py            ← MongoClient lifecycle, indexes, auto-reconnect,
│   │                                  transaction detection, GridFS init
│   ├── models/
│   │   └── schemas.py             ← Pydantic data models (MetadataDocument, Checksums,
│   │                                  OriginalFiles, FileSizes, AuditEntry, etc.)
│   ├── services/
│   │   ├── seed_service.py        ← Bulk seeding, single create/modify, delta logic
│   │   ├── fetch_service.py       ← Query by report_id, composite key, region, etc.
│   │   ├── export_service.py      ← Download bundle files from GridFS to disk
│   │   ├── cleanup_service.py     ← Version retention + age-based purging
│   │   ├── gridfs_service.py      ← GridFS upload/download/delete + retry decorator
│   │   │                              + GridFSOrphanTracker
│   │   └── audit_service.py       ← AuditEntry factory
│   ├── utils/
│   │   ├── checksum.py            ← SHA-256 for files and bytes
│   │   ├── report_id.py           ← Composite report_id generator
│   │   ├── validator.py           ← 6-layer validation pipeline
│   │   └── retry.py               ← Exponential backoff decorator
│   ├── errors/
│   │   └── exceptions.py          ← Custom exception hierarchy
│   └── sdk/
│       └── client.py              ← ReportGenClient (direct MongoDB) +
│                                      ReportGenHTTPClient (REST)
│
├── integration/
│   └── seed_caller.py             ← Drop-in HTTP caller for external regulation repos
│
├── seeds/
│   └── seed.yaml                  ← Manifest template
│
├── docs/
│   ├── index.html                 ← Interactive HTML5 architecture document
│   │                                  (served at /api/details)
│   └── mongo_seeder_presentation.pptx  ← 14-slide presentation deck
│
├── .env                           ← Local environment config (git-ignored)
├── .env.example                   ← Config template with all variables documented
├── .gitignore                     ← Excludes .env, __pycache__, .venv, *.pyc, logs
├── requirements.txt               ← Python dependencies
├── SEED_CONFIG_GUIDE.md           ← End-user guide: seed.yaml + bundle file reference
└── README.md                      ← This file
```

---

## Environment Variables

All configuration is loaded from `.env` (auto-discovered from project root). Copy `.env.example` to get started.

### MongoDB

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | `mongodb://localhost:27017` | Full connection string. Atlas: `mongodb+srv://user:pass@cluster/db` |
| `MONGO_DB_NAME` | `doc_management` | Target database name |
| `MONGO_METADATA_COLLECTION` | `metadata` | Collection for version documents and audit logs |
| `MONGO_GRIDFS_BUCKET` | `fs` | GridFS bucket prefix (`fs.files`, `fs.chunks`) |
| `MONGO_MAX_POOL_SIZE` | `50` | MongoClient connection pool ceiling |
| `MONGO_CONNECT_TIMEOUT_MS` | `5000` | TCP connect timeout in milliseconds |
| `MONGO_SERVER_TIMEOUT_MS` | `5000` | Server selection timeout in milliseconds |

### API Server

| Variable | Default | Description |
|----------|---------|-------------|
| `API_KEY` | `""` (auth off) | Shared secret for `X-API-Key` header. **Required in production.** |
| `API_HOST` | `0.0.0.0` | Bind address |
| `API_PORT` | `8000` | Bind port (run.sh overrides to `3089`) |
| `API_WORKERS` | `2` | Gunicorn worker count (unused in uvicorn direct mode) |

### Logging

| Variable | Default | Accepted values |
|----------|---------|-----------------|
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `LOG_FORMAT` | `text` | `text` (human-readable), `json` (structured, for log aggregators) |

### Runtime

| Variable | Default | Description |
|----------|---------|-------------|
| `ENVIRONMENT` | `development` | `development`, `staging`, `production` |

> **Production guard:** `ENVIRONMENT=production` + no `API_KEY` → boot fails immediately with a clear error.
>
> **TLS auto-enforcement:** In production, if the URI doesn't contain `localhost` or `127.0.0.1`, `tls=True` is injected into the MongoClient kwargs automatically.

---

## Composite report\_id

Every record gets a **deterministic composite** `report_id` derived from its four business keys at creation time.

```
Format:   {csi_id}_{region}_{regulation}_{json_config_stem}
Example:  CSI-001_APAC_MAS-TRM_mas_trm_report
```

- `json_config_stem` = JSON config filename **without** the `.json` extension
- The same inputs **always produce the same `report_id`** — fully predictable, no UUID randomness
- The ID encodes the full business context — human-readable at a glance

**The 4-part composite key** `(csi_id, region, regulation, json_config_filename)` is the fundamental routing mechanism:

```
Same csi_id + region + regulation, different json_config → separate records
Same composite key → same logical record (CREATE / MODIFY / SKIP routing)
```

You **never supply a `report_id`** when seeding or modifying. The system derives it. Use `report_id` only for targeted fetch, history, export, cleanup, and PATCH operations.

---

## Seeding Flow — 5 Steps

```
Step 1  Load YAML manifest → validate root structure
        (manifest must have a 'bundles' list with at least one entry)

Step 2  Pre-validate ALL bundles before any DB writes
        (6-layer validation runs on each bundle; errors collected per bundle)
        (a failed bundle does not block other bundles)

Step 3  For each valid bundle:
          a. Compute SHA-256 checksums for all files
          b. Query MongoDB for active record matching the 4-part composite key
          c. No active record found     → CREATE
               · Upload all files to GridFS
               · Derive composite report_id
               · Insert metadata document (version=1, active=true)
          d. Active record found, all checksums match → SKIP
               · No writes, no GridFS uploads
               · Idempotent — safe to re-run any time
          e. Active record found, any checksum changed → MODIFY
               · SHA-256 delta: only changed files re-uploaded to GridFS
               · Unchanged files reuse existing GridFS ObjectIds
               · Deactivate old version (active=false)
               · Insert new version (version=N+1, active=true)
               · Both steps atomic on replica sets (MongoDB transaction)
               · On standalone: GridFSOrphanTracker cleans up if metadata write fails

Step 4  Log per-bundle result with report_id, version, reason

Step 5  Return structured dict:
        { created: N, updated: N, skipped: N, failed: N, total: N, details: [...] }
```

---

## Routing Logic — CREATE / MODIFY / SKIP

The seeder uses **automatic routing** — you never specify which operation to perform.

```
Input: (csi_id, region, regulation, json_config_filename)
         ↓
Query: active record where
         csi_id       = input.csi_id
         regulation   = input.regulation
         region       = input.region
         original_files.json_config = input.json_config_filename
         active       = True
         ↓
No match found?
  → CREATE  (v1, new report_id derived from composite key)
         ↓
Match found — compare SHA-256:
  All checksums identical?
    → SKIP  (no writes, return "skipped")
  Any checksum changed?
    → MODIFY  (delta-upload, deactivate old, insert v+1)
```

This routing makes re-running `seed.yaml` completely **safe and idempotent** at any frequency.

---

## 6-Layer Validation Pipeline

All validation runs **before any database operation**. A bundle that fails any layer is rejected with a descriptive error; other bundles in the same manifest continue normally.

| Layer | What is checked |
|-------|-----------------|
| **1 — Manifest structure** | YAML root is a dict; has a `bundles` key; list is non-empty |
| **2 — Bundle fields** | `csi_id`, `region`, `regulation`, `json_config`, `sql_file` — present, non-empty |
| **3 — Token format** | Each identifier matches `^[A-Za-z0-9_\-\.]+$`; no spaces, slashes, or null bytes |
| **4 — File existence** | Every referenced file exists on disk, is a regular file, and is non-empty; no path traversal (`../`) |
| **5 — Extension allowlist** | `.json` for config; `.sql` for queries; `.txt/.html/.jinja/.j2/.tmpl/.xml/.csv` for templates |
| **6 — Content validation** | JSON config: valid JSON, root object, has `report.name` (non-empty string); SQL: valid UTF-8, non-whitespace content |

---

## seed.yaml Format

```yaml
bundles:
  # ── Minimal bundle ──────────────────────────────────────────────────────────
  - csi_id: "CSI-001"
    region: "APAC"
    regulation: "MAS-TRM"
    json_config: "configs/mas_trm_report.json"   # filename IS the 4th composite key part
    sql_file:    "sql/mas_trm_query.sql"

  # ── Bundle with optional template ──────────────────────────────────────────
  - csi_id: "CSI-002"
    region: "EU"
    regulation: "GDPR"
    json_config: "configs/gdpr_audit.json"
    sql_file:    "sql/gdpr_query.sql"
    template:    "templates/gdpr_template.html"

  # ── Two different reports for the same regulation ───────────────────────────
  # Different json_config filenames → separate composite keys → separate records
  - csi_id: "CSI-001"
    region: "APAC"
    regulation: "MAS-TRM"
    json_config: "configs/mas_trm_summary.json"    # → CSI-001_APAC_MAS-TRM_mas_trm_summary
    sql_file:    "sql/mas_trm_summary.sql"

  - csi_id: "CSI-001"
    region: "APAC"
    regulation: "MAS-TRM"
    json_config: "configs/mas_trm_detailed.json"   # → CSI-001_APAC_MAS-TRM_mas_trm_detailed
    sql_file:    "sql/mas_trm_detailed.sql"
```

All file paths are **relative to the location of `seed.yaml`**.

### JSON config minimum structure

```json
{
  "report": {
    "name": "MAS TRM Compliance Report"
  }
}
```

Any additional fields are passed through to ReportGen untouched.

---

## CLI Reference

All CLI commands are invoked as:

```bash
python -m src.cli [--verbose] <command> [options]
```

`--verbose` / `-v` enables DEBUG-level logging for any command.

### `seed` — Bulk seed from manifest

```bash
python -m src.cli seed seeds/seed.yaml
python -m src.cli -v seed seeds/seed.yaml          # debug logging
```

Prints a Rich summary table (Created / Updated / Skipped / Failed) plus a per-bundle detail table.

---

### `create` — Create a single record

```bash
python -m src.cli create \
  --csi-id   CSI-001 \
  --region   APAC \
  --regulation MAS-TRM \
  --config   configs/mas_trm_report.json \
  --sql      sql/mas_trm_query.sql \
  --template templates/mas_trm_template.txt     # optional
```

Fails with a clear error if an active record already exists for the composite key (use `modify` or `seed` instead).

---

### `modify` — Modify an existing record

```bash
python -m src.cli modify \
  --csi-id   CSI-001 \
  --region   APAC \
  --regulation MAS-TRM \
  --config   configs/mas_trm_report.json \       # required — filename = composite key lookup
  --sql      sql/updated_query.sql \             # optional — only changed files re-uploaded
  --template templates/updated.txt               # optional
```

`--config` is always required because the json_config filename is the 4th composite key part. The system will lookup the active record, compute SHA-256 deltas, and create a new version.

---

### `list` — List records

```bash
python -m src.cli list              # all active records (table: ID, name, version, region, regulation, timestamp)
python -m src.cli list --all        # include inactive historical versions
```

---

### `fetch` — Fetch record(s) by field

```bash
python -m src.cli fetch --report-id CSI-001_APAC_MAS-TRM_mas_trm_report
python -m src.cli fetch --csi-id CSI-001
python -m src.cli fetch --region APAC
python -m src.cli fetch --regulation MAS-TRM
```

---

### `history` — Version history for a record

```bash
python -m src.cli history --report-id CSI-001_APAC_MAS-TRM_mas_trm_report
```

Prints all versions (active + inactive), their checksums, sizes, and audit log entries.

---

### `export` — Download files to disk

```bash
# All files for the active version
python -m src.cli export --report-id CSI-001_APAC_MAS-TRM_mas_trm_report -o ./out/

# Specific version
python -m src.cli export --report-id CSI-001_APAC_MAS-TRM_mas_trm_report -o ./out/ -V 2

# Single file type only
python -m src.cli export --report-id CSI-001_APAC_MAS-TRM_mas_trm_report -o ./out/ --file sql_file
# --file accepts: json_config | sql_file | template

# Skip checksum mismatch abort
python -m src.cli export --report-id CSI-001_APAC_MAS-TRM_mas_trm_report -o ./out/ --force
```

Exported files are written to `{output_dir}/{report_id}/v{N}/{filename}`.

---

### `cleanup` — Purge old versions

```bash
# Preview (dry-run) for one record — keep 3 most recent versions
python -m src.cli cleanup --report-id CSI-001_APAC_MAS-TRM_mas_trm_report --keep 3 --dry-run

# Live cleanup for one record
python -m src.cli cleanup --report-id CSI-001_APAC_MAS-TRM_mas_trm_report --keep 3

# Global sweep — all records, keep 3 versions each
python -m src.cli cleanup --all --keep 3

# Age-based — purge inactive versions older than 90 days
python -m src.cli cleanup --max-age-days 90
python -m src.cli cleanup --max-age-days 90 --dry-run     # preview first
```

---

## REST API Reference

All endpoints (except `/api/health` and `/api/details`) require the `X-API-Key` header when `API_KEY` is configured in `.env`.

```
X-API-Key: your-secret-key
```

### Endpoint Overview

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/api/health` | None | DB ping, transaction support flag, timestamp |
| `GET` | `/api/details` | None | Serves `docs/index.html` (interactive architecture) |
| `GET` | `/api/docs` | None | Swagger UI (dev only — hidden in production) |
| `GET` | `/api/redoc` | None | ReDoc UI (dev only — hidden in production) |
| `GET` | `/api/records` | ✅ | List records — filterable, paginated |
| `GET` | `/api/records/{report_id}` | ✅ | Single active record by composite report_id |
| `GET` | `/api/records/{report_id}/history` | ✅ | Full version history (all versions, sorted by version) |
| `GET` | `/api/records/{report_id}/export` | ✅ | Stream all files as ZIP |
| `GET` | `/api/files/{report_id}/{filename}` | ✅ | Stream a single file from GridFS |
| `PATCH` | `/api/records/{report_id}` | ✅ | Modify a record by composite report_id |
| `POST` | `/api/seed/bundle` | ✅ | Seed a single bundle (base64 inline) |
| `POST` | `/api/seed/manifest` | ✅ | Seed multiple bundles at once |
| `POST` | `/api/cleanup` | ✅ | Run version retention cleanup |
| `DELETE` | `/api/records/{report_id}` | ✅ | Deactivate a record (soft delete — data preserved) |

---

## API Endpoint Details

### `GET /api/health`

No auth required. Pings MongoDB and returns:

```json
{
  "status": "healthy",
  "database": "doc_management",
  "transactions_supported": true,
  "timestamp": "2026-05-18T09:00:00.000000+00:00"
}
```

Returns `503` if the DB ping fails.

---

### `GET /api/records`

**Query parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `active_only` | bool | `true` | Include inactive versions when `false` |
| `region` | string | — | Exact match filter |
| `regulation` | string | — | Exact match filter |
| `csi_id` | string | — | Exact match filter |
| `limit` | int | `100` | Max records returned (1–1000) |
| `skip` | int | `0` | Pagination offset |

**Response:**

```json
{
  "records": [
    {
      "id": "...",
      "report_id": "CSI-001_APAC_MAS-TRM_mas_trm_report",
      "name": "mas_trm_report.json",
      "csi_id": "CSI-001",
      "region": "APAC",
      "regulation": "MAS-TRM",
      "version": 3,
      "active": true,
      "mongoInsertedTs": "2026-05-18T09:00:00.000000",
      "mongoUpdatedTs": "2026-05-18T10:00:00.000000"
    }
  ],
  "total": 42,
  "limit": 100,
  "skip": 0
}
```

---

### `POST /api/seed/bundle`

Seed a single bundle with base64-encoded file contents.

**Request body:**

```json
{
  "csi_id": "CSI-001",
  "region": "APAC",
  "regulation": "MAS-TRM",
  "json_config_filename": "mas_trm_report.json",
  "json_config_content": "<base64-encoded JSON bytes>",
  "sql_file_filename": "mas_trm_query.sql",
  "sql_file_content": "<base64-encoded SQL bytes>",
  "template_filename": "mas_trm_template.txt",    // optional
  "template_content": "<base64-encoded bytes>"    // optional
}
```

**Response `201`:**

```json
{
  "status": "created",
  "report_id": "CSI-001_APAC_MAS-TRM_mas_trm_report",
  "version": 1,
  "reason": "Initial seed"
}
```

`status` is one of `"created"`, `"updated"`, `"skipped"`.

---

### `POST /api/seed/manifest`

Seed multiple bundles in one request. Same format as the single bundle but wrapped in a list:

```json
{
  "bundles": [
    { "csi_id": "...", "region": "...", ... },
    { "csi_id": "...", "region": "...", ... }
  ]
}
```

Maximum 100 bundles per request.

**Response:**

```json
{
  "created": 2,
  "updated": 1,
  "skipped": 0,
  "failed": 0,
  "total": 3,
  "details": [...]
}
```

---

### `PATCH /api/records/{report_id}`

Modify an existing record. At least one file must be provided. The `json_config_filename` must be supplied when changing the config (it's used as the composite key lookup).

```json
{
  "json_config_filename": "mas_trm_report.json",
  "json_config_content": "<base64>",     // optional
  "sql_file_filename": "updated.sql",
  "sql_file_content": "<base64>",        // optional
  "template_filename": "new.txt",
  "template_content": "<base64>"         // optional
}
```

---

### `POST /api/cleanup`

```json
{
  "report_id": "CSI-001_APAC_MAS-TRM_mas_trm_report",   // one of: report_id, purge_all, or max_age_days
  "purge_all": false,
  "keep_versions": 3,         // default 3 (1–100)
  "max_age_days": 90,         // optional — prune inactive older than N days
  "dry_run": false            // true = preview only, no writes
}
```

**Response:**

```json
{
  "purged_count": 4,
  "dry_run": false,
  "details": [...]
}
```

---

### `GET /api/records/{report_id}/history`

Returns all versions for the composite key identified by `report_id`, sorted by `version` ascending (oldest first, newest last).

```json
{
  "report_id": "CSI-001_APAC_MAS-TRM_mas_trm_report",
  "total_versions": 3,
  "records": [
    { "version": 1, "active": false, "mongoInsertedTs": "...", "audit_log": [...] },
    { "version": 2, "active": false, "mongoInsertedTs": "...", "audit_log": [...] },
    { "version": 3, "active": true,  "mongoInsertedTs": "...", "audit_log": [...] }
  ]
}
```

---

## Service Layer Reference

### `seed_service`

| Function | Signature | Description |
|----------|-----------|-------------|
| `seed_from_manifest` | `(manifest_path: str) → dict` | Load YAML, validate all bundles, process each → CREATE / MODIFY / SKIP |
| `create_single_record` | `(csi_id, region, regulation, json_config_path, sql_file_path, template_path?) → tuple` | Create one record (CLI `create` command) |
| `modify_record_by_id` | `(report_id, json_config_path?, sql_file_path?, template_path?) → tuple` | Modify by report_id; delta-uploads only changed files |
| `process_bundle` | `(bundle: dict, config: dict) → (status, report_id, version, reason)` | Core router: SKIP / MODIFY / CREATE per bundle |

---

### `fetch_service`

| Function | Description |
|----------|-------------|
| `fetch_active_by_report_id(report_id)` | Return the active version for a composite report_id |
| `fetch_by_csi_id(csi_id, active_only, limit)` | List records matching CSI ID |
| `fetch_by_region(region, active_only, limit)` | List records matching region |
| `fetch_by_regulation(regulation, active_only, limit)` | List records matching regulation |
| `fetch_by_composite(filters, active_only, limit)` | Multi-field filter query |
| `fetch_version_history(report_id)` | All versions (active + inactive) for the 4-part composite key, sorted ascending |
| `list_all_active(limit)` | Projection-only list of all active records |

---

### `export_service`

| Function | Description |
|----------|-------------|
| `export_bundle(report_id, output_dir, version?, verify_checksums, force, files?)` | Download selected files from GridFS to disk. `files` set controls which file types (`json_config`, `sql_file`, `template`). SHA-256 verified on every download. |

---

### `cleanup_service`

| Function | Description |
|----------|-------------|
| `purge_old_versions(report_id, keep_versions, dry_run)` | Purge old inactive versions for one record; also deletes their GridFS files |
| `purge_all_old_versions(keep_versions, dry_run)` | Global sweep: groups by full 4-part composite key, applies per-key purge |
| `purge_by_age(max_age_days, dry_run)` | Purge inactive records whose `mongoInsertedTs` is older than N days |

---

### `gridfs_service`

| Function / Class | Description |
|------------------|-------------|
| `upload_to_gridfs(bucket, file_path, original_filename, content_type, extra_metadata?, orphan_tracker?)` | Upload with 3× exponential retry; stores SHA-256 + size in GridFS metadata |
| `download_from_gridfs(bucket, gridfs_id)` | Download bytes + metadata with 3× retry |
| `delete_from_gridfs(bucket, gridfs_id)` | Delete a GridFS file by ObjectId |
| `GridFSOrphanTracker` | Context manager: tracks all uploaded ObjectIds; bulk-deletes them if the metadata write fails (compensates for GridFS being outside transaction scope on standalone) |

---

### `database.DatabaseManager`

| Method / Property | Description |
|------------------|-------------|
| `connect()` | Open MongoClient, ping, detect transaction support (`hello` command), create all indexes, init GridFS |
| `close()` | Close client, release connection pool |
| `start_session()` | Open a MongoDB client session (used for transactions) |
| `.metadata_collection` | Returns the configured metadata collection handle |
| `.fs` | Returns the configured GridFS handle |
| `.supports_transactions` | `True` if connected to replica set or mongos |
| `get_db()` | Module-level singleton: returns connected manager; pings on every call; reconnects on stale TCP |
| `create_db_manager(uri?, db_name?)` | Create and connect a new `DatabaseManager` |
| `set_db(instance)` | Override global singleton (for testing) |
| `reset_db()` | Close and clear the global singleton |

---

### `validator`

| Function | Description |
|----------|-------------|
| `validate_manifest_structure(manifest, source)` | Ensures root is dict with non-empty `bundles` list |
| `validate_seed_bundle(bundle, base_dir, index)` | Required fields, identifier token format, file existence, path traversal guard, extension allowlist |
| `validate_json_config(path, index?)` | Valid JSON, root is dict, has non-empty `report.name` |
| `validate_sql_content(path, index?)` | UTF-8 readable, non-whitespace content |

---

### `retry`

```python
@retry_on_failure(max_retries=3, base_delay=0.5, backoff_factor=2)
def my_db_operation():
    ...
```

Retries on `AutoReconnect`, `ConnectionFailure`, `NetworkTimeout`, `ServerSelectionTimeoutError` with delays: 0.5s → 1.0s → 2.0s. Raises the original exception after exhausting all retries.

---

## Custom Exceptions

| Exception | HTTP Status | When raised |
|-----------|-------------|-------------|
| `SeederError` | 500 | Base class for all domain errors |
| `ValidationError` | 400 | Manifest / bundle / file / schema validation failure |
| `FileNotFoundError` | 400 | Referenced file path does not exist |
| `DuplicateRecordError` | 409 | Attempt to create when active record already exists for composite key |
| `DatabaseError` | 500 | MongoDB connection or query failure |
| `GridFSError` | 500 | GridFS upload / download / delete failure |
| `ChecksumMismatchError` | 500 | Stored vs. re-computed checksum mismatch detected on export |
| `RecordNotFoundError` | 404 | No record found for the given report_id or composite key |

In production (`ENVIRONMENT=production`), exception `details` fields are stripped from HTTP responses — only the message is surfaced.

---

## MongoDB Data Schema

### Metadata document

Each record in the `metadata` collection has this shape:

```json
{
  "_id": "<ObjectId>",
  "report_id": "CSI-001_APAC_MAS-TRM_mas_trm_report",
  "name": "mas_trm_report.json",
  "csi_id": "CSI-001",
  "regulation": "MAS-TRM",
  "region": "APAC",
  "version": 3,
  "active": true,
  "original_files": {
    "json_config": "mas_trm_report.json",
    "sql_file": "mas_trm_query.sql",
    "template": "mas_trm_template.html"
  },
  "file_contents": {
    "json_config_id": "<GridFS ObjectId>",
    "sql_file_id": "<GridFS ObjectId>",
    "template_id": "<GridFS ObjectId>"
  },
  "checksums": {
    "json_config": "sha256:abc123...",
    "sql_file": "sha256:def456...",
    "template": "sha256:789abc..."
  },
  "file_sizes": {
    "json_config": 1024,
    "sql_file": 4096,
    "template": 2048
  },
  "mongoInsertedTs": "2026-05-18T09:00:00.000000",
  "mongoUpdatedTs": "2026-05-18T10:00:00.000000",
  "audit_log": [
    {
      "action": "CREATED",
      "reason": "Initial seed",
      "timestamp": "2026-05-18T09:00:00.000000",
      "version": 1
    },
    {
      "action": "MODIFIED",
      "reason": "changed: sql_file",
      "timestamp": "2026-05-18T10:00:00.000000",
      "version": 3
    }
  ]
}
```

### Field descriptions

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectId | MongoDB auto-generated document ID |
| `report_id` | str | `{csi_id}_{region}_{regulation}_{json_config_stem}` — composite, deterministic |
| `name` | str | JSON config filename (e.g. `mas_trm_report.json`) |
| `csi_id` | str | Business unit identifier (e.g. `CSI-001`) |
| `regulation` | str | Regulatory framework (e.g. `MAS-TRM`) |
| `region` | str | Geographic region (e.g. `APAC`) |
| `version` | int | Auto-incremented per composite key; starts at 1 |
| `active` | bool | `true` = current version; `false` = historical (deactivated but preserved) |
| `original_files` | dict | Maps file type → original filename stored in GridFS |
| `file_contents` | dict | Maps file type → GridFS ObjectId of the binary content |
| `checksums` | dict | Maps file type → `sha256:<hex>` hash computed at upload time |
| `file_sizes` | dict | Maps file type → byte size |
| `mongoInsertedTs` | datetime | UTC timestamp when this version was first inserted |
| `mongoUpdatedTs` | datetime | UTC timestamp of the last modification to this document |
| `audit_log` | list | Chronological list of `{action, reason, timestamp, version}` entries |

---

## Indexes

All indexes are created automatically at startup via `DatabaseManager._ensure_indexes()`.

| Index Name | Fields | Type | Purpose |
|------------|--------|------|---------|
| `idx_report_id_active_unique` | `report_id`, `active=true` (partial) | Unique | DB-enforced: one active version per report_id |
| `idx_report_id_active` | `report_id`, `active` | Compound | Fast lookups by report_id + active flag |
| `idx_report_id_version` | `report_id`, `version` | Compound | Version history queries |
| `idx_composite_dedup` | `csi_id`, `regulation`, `region`, `original_files.json_config` | Compound | Full 4-part composite key deduplication |
| `idx_composite_active_unique` | `csi_id`, `regulation`, `region`, `original_files.json_config`, `active=true` (partial) | Unique | DB-enforced: one active per composite key |
| `idx_csi_id` | `csi_id` | Single-field | Filter queries by CSI ID |
| `idx_region` | `region` | Single-field | Filter queries by region |
| `idx_regulation` | `regulation` | Single-field | Filter queries by regulation |
| `idx_active` | `active` | Single-field | Active/inactive queries |

The composite partial unique index (`idx_composite_active_unique`) is the DB-level enforcement of the routing guarantee — you can have unlimited inactive versions but only one active version per 4-part composite key.

If the `idx_composite_dedup` index exists with an old key pattern (3-part, pre-migration), startup drops and recreates it automatically with the correct 4-part pattern.

---

## GridFS Binary Storage

GridFS splits binary files into 255 KB chunks and stores them across two collections:

```
fs.files   — file metadata (filename, size, contentType, uploadDate, SHA-256, custom metadata)
fs.chunks  — binary data in 255 KB chunks, linked by file ObjectId
```

### Why GridFS?

- MongoDB's BSON document limit is 16 MB — GridFS handles files of any size
- Eliminates a separate S3/blob store dependency — one system for both metadata and binary files
- GridFS ObjectIds are stored directly in the metadata document — single query to get both

### Delta uploads (MODIFY)

When a MODIFY is triggered:

1. Compute SHA-256 for each file in the new bundle
2. Compare against stored checksums in the metadata document
3. For **unchanged** files: reuse the existing GridFS ObjectId — no upload, no storage duplication
4. For **changed** files: upload to GridFS, get new ObjectId
5. Old GridFS files (from the deactivated version) are kept until `purge_old_versions` is called

---

## Safety Mechanisms

| Mechanism | Detail |
|-----------|--------|
| **Partial unique indexes** | DB-level guarantee: only one `active=true` per composite key — cannot be violated even under concurrent writes |
| **MongoDB transactions** | On replica sets: deactivate old version + insert new version are a single atomic operation — partial state is impossible |
| **GridFSOrphanTracker** | On standalone: if the metadata insert fails after GridFS uploads, the tracker deletes all uploaded GridFS files so no orphaned binaries accumulate |
| **SHA-256 checksums** | Computed at upload, stored in metadata, re-verified on export — detects in-flight corruption or GridFS data decay |
| **Delta detection** | MODIFY only re-uploads changed files — unchanged files reuse ObjectIds, saving storage and upload time |
| **6-layer validation** | All validation before any DB write — malformed bundles are rejected before touching MongoDB |
| **Pre-validation sweep** | All bundles in a manifest are validated before processing begins — one bad bundle never blocks others |
| **Exponential retry** | GridFS and DB operations retry 3× at 0.5s → 1.0s → 2.0s on `AutoReconnect`, `ConnectionFailure`, `NetworkTimeout` |
| **Auto-reconnect** | `get_db()` pings MongoDB on every call; stale TCP connections (idle timeouts, network blips) are silently replaced |
| **TLS auto-enforcement** | `ENVIRONMENT=production` + non-localhost URI → `tls=True` injected into MongoClient kwargs |
| **Credential sanitization** | Full MongoDB URI is never logged; only the host part is shown in log output |
| **Path traversal guard** | All identifier fields and file paths checked for `../`, `/`, `\`, and null bytes |
| **Production guard** | Boot fails immediately with a clear error if `ENVIRONMENT=production` and `API_KEY` is not set |
| **Request size limit** | HTTP bodies > 100 MB return `413 Payload Too Large` before reaching any handler |
| **Constant-time key compare** | `secrets.compare_digest` used for API key verification — prevents timing attacks |
| **Security headers** | Every response includes `X-Content-Type-Options`, `X-Frame-Options`, `X-XSS-Protection`, `Referrer-Policy`, `Cache-Control: no-store` |

---

## ReportGen Integration

### Option A — Direct MongoDB (recommended for co-located services)

```python
from src.sdk.client import ReportGenClient
from src.config.database import get_db

db = get_db()
client = ReportGenClient(db)

# Fetch all files into memory — no temp files, no disk I/O
files = client.fetch_all_files("CSI-001_APAC_MAS-TRM_mas_trm_report")
# files = { "mas_trm_report.json": b"...", "mas_trm_query.sql": b"...", ... }

# Or fetch a specific file
content = client.fetch_file("CSI-001_APAC_MAS-TRM_mas_trm_report", "mas_trm_report.json")

# Or fetch a specific version
files_v2 = client.fetch_all_files("CSI-001_APAC_MAS-TRM_mas_trm_report", version=2)
```

**Latency:** ~5ms per file (MongoDB network round-trip + GridFS chunk reassembly)
**Best for:** Services in the same Kubernetes cluster or VPC as MongoDB

---

### Option B — HTTP REST (for cross-network consumers)

```python
from src.sdk.client import ReportGenHTTPClient

client = ReportGenHTTPClient(
    base_url="http://seeder.internal:3089",
    api_key="your-secret-key"
)

# Stream all files as ZIP
zip_bytes = client.download_bundle("CSI-001_APAC_MAS-TRM_mas_trm_report")

# Stream a single file
content = client.download_file("CSI-001_APAC_MAS-TRM_mas_trm_report", "mas_trm_query.sql")
```

**Latency:** ~50–200ms per file (HTTP + network overhead)
**Best for:** External services, SaaS consumers, cross-region deployments

---

### Option C — integration/seed_caller.py (for CI/CD pipelines)

```bash
# From your regulation repo's CI/CD pipeline:
export SEEDER_BASE_URL=http://seeder.internal:3089
export SEEDER_API_KEY=your-secret-key

# Seed from manifest
python integration/seed_caller.py manifest seeds/seed.yaml

# Or a single bundle
python integration/seed_caller.py bundle \
  --csi-id CSI-001 \
  --region APAC \
  --regulation MAS-TRM \
  --config seeds/configs/mas_trm_report.json \
  --sql seeds/sql/mas_trm_query.sql
```

---

## Technology Decisions

| Technology | Why chosen |
|------------|------------|
| **MongoDB** | Document model fits the heterogeneous, schema-flexible nature of regulatory config files — no fixed schema migration needed when a regulation adds new fields |
| **GridFS** | Files can exceed MongoDB's 16 MB BSON limit; GridFS handles any size, eliminates S3/blob store dependency, keeps files co-located with metadata |
| **Append-only versioning** | Regulators require audit trails — hard-deleting data is a compliance risk; deactivating preserves history without application complexity |
| **Composite report\_id** | Deterministic IDs from business keys → idempotent seeding; re-run with same inputs → same ID, same routing decision; human-readable in logs and dashboards |
| **SHA-256 delta detection** | Only changed files re-uploaded → storage efficiency; fast re-seeds when only one of three files changed; corruption detection on export |
| **FastAPI** | Automatic OpenAPI/Swagger generation; Pydantic request validation at zero cost; async-ready for future streaming use cases |
| **Pydantic** | Request model validation (`SeedBundleRequest`, `CleanupRequest`) with field-level size limits and custom validators — rejection happens before any business logic |
| **Click** | Composable CLI subcommands without boilerplate; `@click.group()` + `@cli.command()` pattern scales cleanly as new commands are added |
| **Rich** | Tables, panels, and coloured output in the terminal — `list`, `seed`, `history` all render structured Rich tables for human readability |
| **Exponential retry** | MongoDB transient errors (network blips, replica failover) are recoverable — retrying 3× with backoff prevents spurious failures without hiding real errors |
| **MongoDB transactions** | Deactivate old + insert new must be atomic; partial state (both versions active, or neither) would violate the invariant enforced by the partial unique index |
| **Partial unique indexes** | DB-level enforcement of invariants is safer than application-level enforcement — concurrent writes cannot produce two active versions even if the application has a bug |

---

## Troubleshooting

### Server won't start

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `Configuration errors: API_KEY must be set` | `ENVIRONMENT=production` without `API_KEY` | Set `API_KEY` in `.env` or set `ENVIRONMENT=development` |
| `Failed to connect to MongoDB` | `MONGO_URI` wrong or MongoDB not running | Check URI in `.env`; verify MongoDB is reachable |
| `Address already in use :3089` | run.sh failed to kill previous process | Run `lsof -ti tcp:3089 \| xargs kill -9` manually |
| `ModuleNotFoundError` | Dependencies not installed | Run `pip install -r requirements.txt` inside `.venv` |

### Seeding errors

| Error | Cause | Fix |
|-------|-------|-----|
| `missing required key 'csi_id'` | Bundle missing a required field | Add the field to `seed.yaml` |
| `contains invalid characters` | Identifier has spaces or special chars | Use only `A-Z a-z 0-9 _ - .` |
| `JSON config not found` | File path is wrong or file doesn't exist | Check path is relative to `seed.yaml` location |
| `missing required field 'report.name'` | JSON config missing `report.name` | Add `{"report": {"name": "..."}}` |
| `SQL file contains only whitespace` | SQL file is empty | Add valid SQL content |
| `path escapes the base directory` | Path traversal (`../`) in seed.yaml | Use only relative paths within the seeds directory |
| `An active record already exists` | Using CLI `create` when record exists | Use `modify` command or let `seed` auto-route |

### Checking the log

```bash
# Live tail
tail -f output.log

# Last 100 lines
tail -100 output.log

# Search for errors
grep -i "error\|fail\|exception" output.log

# Search by report_id
grep "CSI-001_APAC_MAS-TRM_mas_trm_report" output.log
```

### Stopping the server

```bash
# If run.sh was used:
kill $(cat .seeder.pid)

# Or find the process:
lsof -ti tcp:3089 | xargs kill
```
