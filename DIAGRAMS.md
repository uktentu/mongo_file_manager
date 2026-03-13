# MongoDB Document Seeder — Architecture & Flow Diagrams

All diagrams use [Mermaid](https://mermaid.js.org/) syntax and render natively in GitHub, GitLab, and Notion.

---

## 1. System Architecture

```mermaid
graph TB
    subgraph ExternalRepos["External Regulation Repos (CI/CD)"]
        R1["MAS-TRM repo\nseed.yaml + files"]
        R2["BASEL repo\nseed.yaml + files"]
        R3["DORA repo\nseed.yaml + files"]
    end

    subgraph SeederEngine["Central Seeder Engine"]
        CLI["CLI\n(Click + Rich)"]
        API["REST API\n(FastAPI)"]

        subgraph Services["Services"]
            SS["seed_service\nCREATE / MODIFY / SKIP"]
            FS["fetch_service\nQuery records"]
            ES["export_service\nDownload bundle"]
            CS["cleanup_service\nRetention policy"]
        end

        subgraph Utils["Utilities"]
            VAL["validator\n6-layer validation"]
            GFS["gridfs_service\nUpload / Download"]
            RET["retry\nExponential backoff"]
            CHK["checksum\nSHA-256"]
            RID["report_id\nUUID v4 generator"]
        end

        DB["DatabaseManager\nMongo connection + indexes"]
    end

    subgraph MongoDB["MongoDB Atlas / Replica Set"]
        META["metadata collection\nversioned records + audit log"]
        GFSS["GridFS (fs.files + fs.chunks)\nbinary file storage"]
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
    START(["seed_from_manifest(path)"])

    S1["Step 1: Load YAML manifest"]
    S1V{"Valid structure?\ncsi_id, bundles list"}
    S1E["Raise ValidationError"]

    S2["Step 2: Pre-validate ALL bundles\n(collect errors — no DB writes yet)"]
    S2V{"All field checks pass?\nFiles exist? JSON valid? SQL valid?"}
    S2F["Mark bundle FAILED\nContinue to next"]

    S3["Step 3: Compute SHA-256\nfor all files in bundle"]
    S4["Step 4: Lookup active record\nin metadata collection\nby composite key:\ncsi_id + region + regulation\n+ json_config filename"]

    EXIST{"Active record\nexists?"}

    SKIP{"All checksums\nmatch stored?"}
    SK(["SKIP\nReturn existing report_id"])

    MOD["MODIFY:\nDeactivate current version\nDelta-upload only changed files\nInsert new version doc"]
    CR["CREATE:\nUpload all files to GridFS\nInsert new metadata doc\nGenerate UUID report_id"]

    TRANS{"Replica set /\nMongos?"}
    TXN["Wrap in MongoDB\nTransaction\n(atomic deactivate + insert)"]
    ORP["Use GridFSOrphanTracker\n(fallback: delete orphans on failure)"]

    DONE(["Return: created/updated/skipped/failed\nper-bundle details + summary"])

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
    IN["Bundle Input:\ncsi_id, region, regulation\njson_config filename + content\nsql_file, template?"]

    KEY["Composite Key:\ncsi_id + region\n+ regulation\n+ json_config filename"]
    LOOKUP["Query metadata:\nfind active record\nmatching composite key"]

    NONE["No record found"]
    FOUND["Record found"]

    CHK1["Compare SHA-256:\njson_config"]
    CHK2["Compare SHA-256:\nsql_file"]
    CHK3["Compare SHA-256:\ntemplate"]
    ANY{"Any\nchecksum\nchanged?"}

    CREATE(["CREATE\nNew UUID report_id\nAll files uploaded\nversion = 1"])
    SKIP(["SKIP\nNothing written\nIdempotent"])
    MODIFY(["MODIFY\nOnly changed files re-uploaded\nNew version appended\nOld version deactivated"])

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
    SS->>GS: upload_to_gridfs(json_config, tracker)
    GS->>RET: attempt 1
    RET->>GFS: bucket.put(file_bytes)
    GFS-->>RET: gridfs_id
    RET-->>GS: gridfs_id
    GS->>OT: tracker.track(gridfs_id)
    GS-->>SS: gridfs_id ✅

    SS->>GS: upload_to_gridfs(sql_file, tracker)
    GS->>RET: attempt 1 → fail (network blip)
    RET->>RET: sleep 0.5s
    RET->>GFS: attempt 2
    GFS-->>RET: gridfs_id
    GS->>OT: tracker.track(gridfs_id)
    GS-->>SS: gridfs_id ✅

    SS->>META: insert_one(metadata_doc)
    alt Insert succeeds
        META-->>SS: ok
        SS->>OT: tracker.clear() — no orphans
    else Insert fails
        META-->>SS: error
        SS->>OT: tracker.cleanup()
        OT->>GFS: delete(json_config_id)
        OT->>GFS: delete(sql_file_id)
        SS-->>SS: raise DatabaseError
    end
```

---

## 5. Export Flow

```mermaid
flowchart TD
    START(["export_bundle(report_id, output_dir,\nversion?, files?, verify_checksums, force)"])

    LOOKUP["Lookup metadata record:\nactive version (default)\nor specific version"]
    NOTFOUND["Raise RecordNotFoundError"]

    FILTER["Determine file types to export:\nfiles param → subset of\n{json_config, sql_file, template}\nNone → export all three"]

    DL1["Download json_config\nfrom GridFS"]
    DL2["Download sql_file\nfrom GridFS"]
    DL3["Download template\nfrom GridFS (if present)"]

    VER{"verify_checksums\n= True?"}
    CMP["Compare stored SHA-256\nvs re-computed SHA-256"]
    MATCH{"All\nmatch?"}
    FORCE{"force\n= True?"}

    WARN["Log mismatch warning\nKeep files on disk"]
    ABORT["Remove mismatched files\nRaise ChecksumMismatchError"]
    WRITE["Write all files to output_dir"]

    DONE(["Return:\n{report_id, version, files, checksum_verified, output_dir}"])

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
    participant API as FastAPI (api.py)
    participant AUTH as verify_api_key
    participant SVC as Service Layer
    participant DB as DatabaseManager
    participant MONGO as MongoDB

    Client->>API: HTTP Request + X-API-Key header

    API->>AUTH: verify key
    alt Key invalid
        AUTH-->>Client: 401 Unauthorized
    end

    API->>API: Validate request body (Pydantic)
    alt Validation fails
        API-->>Client: 422 Unprocessable Entity
    end

    API->>DB: get_db()
    DB->>MONGO: ping (stale connection check)
    alt Stale connection
        DB->>MONGO: reconnect + re-index
    end

    API->>SVC: call service function
    SVC->>MONGO: query / write

    alt SeederError (domain error)
        SVC-->>API: raise RecordNotFoundError / ValidationError / etc.
        API-->>Client: 404 / 400 / 409 / 500 (JSON)
    else Success
        SVC-->>API: result dict
        API-->>Client: 200 / 201 JSON response
    end
```

---

## 7. Version History & Cleanup

```mermaid
flowchart TD
    subgraph VersionModel["Append-Only Version Model"]
        V1["version=1\nactive=false\nUUID report_id"]
        V2["version=2\nactive=false\nUUID report_id"]
        V3["version=3\nactive=true  ✅\nUUID report_id"]
        V1 -->|superseded by| V2 -->|superseded by| V3
    end

    subgraph CleanupModes["Cleanup Modes"]
        CM1["purge_old_versions(report_id, keep=N)\nKeep N most recent per record"]
        CM2["purge_all_old_versions(keep=N)\nGlobal sweep — all composite keys"]
        CM3["purge_by_age(max_age_days=90)\nPurge inactive records older than N days"]
    end

    subgraph PurgeSteps["Purge Steps (per version)"]
        P1["Delete json_config from GridFS"]
        P2["Delete sql_file from GridFS"]
        P3["Delete template from GridFS"]
        P4["delete_one metadata doc"]
        P1 & P2 & P3 --> P4
    end

    subgraph Guards["Safety Guards"]
        G1["active=true versions NEVER purged"]
        G2["_is_real_record() — excludes sentinel doc"]
        G3["dry_run=True — preview only, no writes"]
    end

    CM1 & CM2 & CM3 --> PurgeSteps
    PurgeSteps --> Guards
```

---

## 8. Database Connection Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Disconnected

    Disconnected --> Connecting: get_db() / connect()
    Connecting --> Connected: ping OK + indexes ensured
    Connecting --> Failed: ConnectionFailure / Timeout
    Failed --> Disconnected: raise DatabaseError

    Connected --> InUse: service call
    InUse --> Connected: operation complete

    Connected --> StaleDetected: get_db() ping fails
    StaleDetected --> Closing: close()
    Closing --> Connecting: auto-reconnect

    Connected --> Closing: close() / reset_db()
    Closing --> Disconnected

    note right of Connected
        Supports transactions:
        true if replica set / mongos
        false if standalone
    end note
```

---

## 9. Validation Pipeline

```mermaid
flowchart LR
    IN["🗂️ seed.yaml input"]

    L1{"Layer 1\nManifest structure"}
    L1E["❌ Not a dict\nNo bundles key\nEmpty list"]

    L2{"Layer 2\nBundle fields"}
    L2E["❌ Missing required keys\nIllegal characters\nEmpty values"]

    L3{"Layer 3\nFile existence"}
    L3E["❌ Path does not exist\nEmpty file\nNot a regular file"]

    L4{"Layer 4\nExtension allowlist"}
    L4E["❌ SQL not .sql\nTemplate extension not allowed"]

    L5{"Layer 5\nJSON config schema"}
    L5E["❌ Invalid JSON\nNot a dict root\nMissing report.name"]

    L6{"Layer 6\nSQL content"}
    L6E["❌ Not UTF-8\nOnly whitespace"]

    OK(["✅ Bundle validated\nReady for DB operations"])

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
        YAML["seed.yaml\n(CLI)"]
        HTTP["HTTP POST\n(API)"]
    end

    subgraph Validation["Validation (validator.py)"]
        V["6-layer checks:\nstructure → fields\n→ files → extensions\n→ JSON schema → SQL"]
    end

    subgraph Routing["Routing (_process_bundle)"]
        CK["Compute SHA-256\nfor all files"]
        QL["Query metadata\nby composite key"]
        RT{{"Route:\nCREATE / MODIFY / SKIP"}}
    end

    subgraph Storage["Storage"]
        GU["GridFS upload\n(per changed file)\nwith retry + orphan tracking"]
        MI["metadata.insert_one\nreport_id=UUID\nversion=N\naudit_log entry\nchecksums\nfile_contents refs"]
    end

    subgraph MongoDB["MongoDB"]
        MC["metadata collection"]
        GC["GridFS chunks"]
    end

    subgraph Output["Output"]
        RES["Result:\nstatus + report_id\n+ version + reason"]
        AUD["Audit log entry:\nCREATED / MODIFIED\n/ DEACTIVATED"]
    end

    YAML & HTTP --> V --> CK --> QL --> RT
    RT -- CREATE/MODIFY --> GU --> MI
    RT -- SKIP --> RES
    MI --> MC & GC
    MI --> AUD --> MC
    GU --> GC
    MI --> RES
```
