# MongoDB Document Seeder — Architecture & Flow Diagrams

All diagrams use [Mermaid](https://mermaid.js.org/) syntax and render natively on GitHub, GitLab, and Notion.

---

## 1. System Architecture

```mermaid
graph TB
    subgraph ExternalRepos["External Regulation Repos"]
        R1["MAS-TRM repo"]
        R2["BASEL repo"]
        R3["DORA repo"]
    end

    subgraph SeederEngine["Central Seeder Engine"]
        CLI["CLI - Click + Rich"]
        API["REST API - FastAPI"]

        subgraph Services["Services"]
            SS["seed_service - CREATE / MODIFY / SKIP"]
            FS["fetch_service - Query records"]
            ES["export_service - Download bundle"]
            CS["cleanup_service - Retention policy"]
        end

        subgraph Utils["Utilities"]
            VAL["validator - 6-layer validation"]
            GFS["gridfs_service - Upload / Download"]
            RET["retry - Exponential backoff"]
            CHK["checksum - SHA-256"]
            RID["report_id - UUID v4 generator"]
        end

        DB["DatabaseManager - Mongo connection + indexes"]
    end

    subgraph MongoDB["MongoDB Atlas / Replica Set"]
        META["metadata collection - versioned records + audit log"]
        GFSS["GridFS - fs.files + fs.chunks - binary file storage"]
    end

    R1 -->|HTTP POST /api/seed/manifest| API
    R2 -->|python -m src.cli seed| CLI
    R3 -->|HTTP POST /api/seed/bundle| API

    CLI --> SS & FS & ES & CS
    API --> SS & FS & ES & CS

    SS --> VAL & GFS & CHK & RID & DB
    FS --> DB
    ES --> GFS & CHK & DB
    CS --> GFS & DB
    GFS --> RET

    DB --> META & GFSS
```

---

## 2. Seeding Flow — CREATE / MODIFY / SKIP

```mermaid
flowchart TD
    START(["seed_from_manifest"])

    S1["Step 1 - Load YAML manifest"]
    S1V{"Valid structure?"}
    S1E["Raise ValidationError"]

    S2["Step 2 - Pre-validate ALL bundles before any DB write"]
    S2V{"All field checks pass?"}
    S2F["Mark bundle FAILED - Continue to next"]

    S3["Step 3 - Compute SHA-256 for all files in bundle"]
    S4["Step 4 - Lookup active record by composite key: csi_id + region + regulation + json_config filename"]

    EXIST{"Active record exists?"}

    SKIP{"All checksums match stored?"}
    SK(["SKIP - Return existing report_id"])

    MOD["MODIFY - Deactivate current version - Delta-upload only changed files - Insert new version doc"]
    CR["CREATE - Upload all files to GridFS - Insert new metadata doc - Generate UUID report_id"]

    TRANS{"Replica set or Mongos?"}
    TXN["Wrap in MongoDB Transaction - atomic deactivate + insert"]
    ORP["Use GridFSOrphanTracker - delete orphans on failure"]

    DONE(["Return: created / updated / skipped / failed + per-bundle details"])

    START --> S1 --> S1V
    S1V -- No --> S1E
    S1V -- Yes --> S2 --> S2V
    S2V -- No --> S2F --> S3
    S2V -- Yes --> S3
    S3 --> S4 --> EXIST

    EXIST -- No --> CR --> TRANS
    EXIST -- Yes --> SKIP
    SKIP -- Yes --> SK --> DONE
    SKIP -- No --> MOD --> TRANS

    TRANS -- Yes --> TXN --> DONE
    TRANS -- No --> ORP --> DONE
```

---

## 3. Composite Key Routing Logic

```mermaid
flowchart LR
    IN["Bundle Input: csi_id, region, regulation, json_config filename + content, sql_file, template"]

    KEY["Composite Key: csi_id + region + regulation + json_config filename"]
    LOOKUP["Query metadata - find active record matching composite key"]

    NONE["No record found"]
    FOUND["Record found"]

    CHK1["Compare SHA-256: json_config"]
    CHK2["Compare SHA-256: sql_file"]
    CHK3["Compare SHA-256: template"]
    ANY{"Any checksum changed?"}

    CREATE(["CREATE - New UUID report_id - All files uploaded - version = 1"])
    SKIP(["SKIP - Nothing written - Idempotent"])
    MODIFY(["MODIFY - Only changed files re-uploaded - New version appended - Old version deactivated"])

    IN --> KEY --> LOOKUP
    LOOKUP --> NONE --> CREATE
    LOOKUP --> FOUND --> CHK1 & CHK2 & CHK3 --> ANY
    ANY -- No --> SKIP
    ANY -- Yes --> MODIFY
```

---

## 4. GridFS Upload with Retry & Orphan Tracking

```mermaid
sequenceDiagram
    participant SS as seed_service
    participant OT as GridFSOrphanTracker
    participant GS as gridfs_service
    participant RET as retry decorator
    participant GFS as MongoDB GridFS
    participant META as metadata collection

    SS->>OT: create tracker
    SS->>GS: upload_to_gridfs json_config
    GS->>RET: attempt 1
    RET->>GFS: bucket.put file bytes
    GFS-->>RET: gridfs_id
    RET-->>GS: gridfs_id
    GS->>OT: tracker.track gridfs_id
    GS-->>SS: gridfs_id OK

    SS->>GS: upload_to_gridfs sql_file
    GS->>RET: attempt 1 fails on network blip
    RET->>RET: sleep 0.5s
    RET->>GFS: attempt 2
    GFS-->>RET: gridfs_id
    GS->>OT: tracker.track gridfs_id
    GS-->>SS: gridfs_id OK

    SS->>META: insert_one metadata doc
    alt Insert succeeds
        META-->>SS: ok
        SS->>OT: tracker.clear - no orphans
    else Insert fails
        META-->>SS: error
        SS->>OT: tracker.cleanup
        OT->>GFS: delete json_config_id
        OT->>GFS: delete sql_file_id
        SS-->>SS: raise DatabaseError
    end
```

---

## 5. Export Flow

```mermaid
flowchart TD
    START(["export_bundle - report_id, output_dir, version, files, verify_checksums, force"])

    LOOKUP["Lookup metadata record - active version by default or specific version"]
    NOTFOUND["Raise RecordNotFoundError"]

    FILTER["Determine file types to export - files param picks subset of json_config, sql_file, template - None exports all three"]

    DL1["Download json_config from GridFS"]
    DL2["Download sql_file from GridFS"]
    DL3["Download template from GridFS if present"]

    VER{"verify_checksums = True?"}
    CMP["Compare stored SHA-256 vs re-computed SHA-256"]
    MATCH{"All match?"}
    FORCE{"force = True?"}

    WARN["Log mismatch warning - Keep files on disk"]
    ABORT["Remove mismatched files - Raise ChecksumMismatchError"]
    WRITE["Write all files to output_dir"]

    DONE(["Return: report_id, version, files, checksum_verified, output_dir"])

    START --> LOOKUP
    LOOKUP -- not found --> NOTFOUND
    LOOKUP -- found --> FILTER
    FILTER --> DL1 & DL2 & DL3 --> VER
    VER -- Yes --> CMP --> MATCH
    MATCH -- Yes --> WRITE --> DONE
    MATCH -- No --> FORCE
    FORCE -- Yes --> WARN --> WRITE --> DONE
    FORCE -- No --> ABORT
    VER -- No --> WRITE --> DONE
```

---

## 6. API Request Lifecycle

```mermaid
sequenceDiagram
    participant Client as External Caller
    participant API as FastAPI api.py
    participant AUTH as verify_api_key
    participant SVC as Service Layer
    participant DB as DatabaseManager
    participant MONGO as MongoDB

    Client->>API: HTTP Request with X-API-Key header

    API->>AUTH: verify key
    alt Key invalid
        AUTH-->>Client: 401 Unauthorized
    end

    API->>API: Validate request body via Pydantic
    alt Validation fails
        API-->>Client: 422 Unprocessable Entity
    end

    API->>DB: get_db
    DB->>MONGO: ping for stale connection check
    alt Stale connection
        DB->>MONGO: reconnect and re-index
    end

    API->>SVC: call service function
    SVC->>MONGO: query or write

    alt SeederError raised
        SVC-->>API: RecordNotFoundError / ValidationError / etc
        API-->>Client: 404 / 400 / 409 / 500 JSON
    else Success
        SVC-->>API: result dict
        API-->>Client: 200 or 201 JSON response
    end
```

---

## 7. Version History & Cleanup

```mermaid
flowchart TD
    subgraph VersionModel["Append-Only Version Model"]
        V1["version=1 - active=false - UUID report_id"]
        V2["version=2 - active=false - UUID report_id"]
        V3["version=3 - active=true - UUID report_id"]
        V1 -->|superseded by| V2 -->|superseded by| V3
    end

    subgraph CleanupModes["Cleanup Modes"]
        CM1["purge_old_versions - keep N most recent per record"]
        CM2["purge_all_old_versions - global sweep all composite keys"]
        CM3["purge_by_age - purge inactive records older than N days"]
    end

    subgraph PurgeSteps["Purge Steps per version"]
        P1["Delete json_config from GridFS"]
        P2["Delete sql_file from GridFS"]
        P3["Delete template from GridFS"]
        P4["delete_one metadata doc"]
        P1 & P2 & P3 --> P4
    end

    subgraph Guards["Safety Guards"]
        G1["active=true versions NEVER purged"]
        G2["_is_real_record - excludes sentinel doc"]
        G3["dry_run=True - preview only no writes"]
    end

    CM1 & CM2 & CM3 --> PurgeSteps
    PurgeSteps --> Guards
```

---

## 8. Database Connection Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Disconnected

    Disconnected --> Connecting : get_db or connect called
    Connecting --> Connected : ping OK and indexes ensured
    Connecting --> Failed : ConnectionFailure or Timeout
    Failed --> Disconnected : raise DatabaseError

    Connected --> InUse : service call starts
    InUse --> Connected : operation complete

    Connected --> StaleDetected : get_db ping fails
    StaleDetected --> Closing : close called
    Closing --> Connecting : auto-reconnect

    Connected --> Closing : close or reset_db called
    Closing --> Disconnected : client closed
```

---

## 9. Validation Pipeline

```mermaid
flowchart LR
    IN["seed.yaml input"]

    L1{"Layer 1 - Manifest structure"}
    L1E["FAIL - Not a dict or no bundles key or empty list"]

    L2{"Layer 2 - Bundle fields"}
    L2E["FAIL - Missing required keys or illegal characters or empty values"]

    L3{"Layer 3 - File existence"}
    L3E["FAIL - Path does not exist or empty file or not a regular file"]

    L4{"Layer 4 - Extension allowlist"}
    L4E["FAIL - SQL not .sql or template extension not allowed"]

    L5{"Layer 5 - JSON config schema"}
    L5E["FAIL - Invalid JSON or not a dict root or missing report.name"]

    L6{"Layer 6 - SQL content"}
    L6E["FAIL - Not UTF-8 or only whitespace"]

    OK(["Bundle validated - Ready for DB operations"])

    IN --> L1
    L1 -- fail --> L1E
    L1 -- pass --> L2
    L2 -- fail --> L2E
    L2 -- pass --> L3
    L3 -- fail --> L3E
    L3 -- pass --> L4
    L4 -- fail --> L4E
    L4 -- pass --> L5
    L5 -- fail --> L5E
    L5 -- pass --> L6
    L6 -- fail --> L6E
    L6 -- pass --> OK
```

---

## 10. End-to-End Data Flow

```mermaid
flowchart TB
    subgraph Input["Input Sources"]
        YAML["seed.yaml via CLI"]
        HTTP["HTTP POST via API"]
    end

    subgraph Validation["Validation"]
        V["6-layer checks: structure, fields, files, extensions, JSON schema, SQL"]
    end

    subgraph Routing["Routing"]
        CK["Compute SHA-256 for all files"]
        QL["Query metadata by composite key"]
        RT{{"Route: CREATE / MODIFY / SKIP"}}
    end

    subgraph Storage["Storage"]
        GU["GridFS upload per changed file - with retry and orphan tracking"]
        MI["metadata.insert_one - report_id UUID, version N, audit log, checksums, file refs"]
    end

    subgraph MongoDB["MongoDB"]
        MC["metadata collection"]
        GC["GridFS chunks"]
    end

    subgraph Output["Output"]
        RES["Result: status + report_id + version + reason"]
        AUD["Audit log: CREATED / MODIFIED / DEACTIVATED"]
    end

    YAML & HTTP --> V --> CK --> QL --> RT
    RT -- CREATE or MODIFY --> GU --> MI
    RT -- SKIP --> RES
    MI --> MC & GC
    MI --> AUD --> MC
    GU --> GC
    MI --> RES
```
