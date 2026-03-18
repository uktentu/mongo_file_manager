"""FastAPI REST API for the MongoDB Document Seeder."""

import io
import logging
import tempfile
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Any, Dict, List

from bson import ObjectId
from fastapi import FastAPI, HTTPException, Query, Depends, Security, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

from src.config.database import get_db, reset_db
from src.config.settings import get_settings
from src.config.logging_config import configure_logging
from src.errors.exceptions import (
    SeederError,
    RecordNotFoundError,
    ValidationError,
    DuplicateRecordError,
)

logger = logging.getLogger(__name__)

API_KEY = get_settings().api_key
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(key: Optional[str] = Security(api_key_header)):
    if not API_KEY:
        return
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    try:
        get_db()
        logger.info("api.startup database connected")
    except Exception as exc:
        logger.error("api.startup_failed error=%s", exc)
    yield
    reset_db()
    logger.info("api.shutdown database disconnected")


app = FastAPI(
    title="MongoDB Document Seeder API",
    description="REST API for managing regulatory document bundles.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(RecordNotFoundError)
async def record_not_found_handler(request: Request, exc: RecordNotFoundError):
    return JSONResponse(
        status_code=404,
        content={"error": "Not Found", "message": exc.message, "details": exc.details},
    )


@app.exception_handler(ValidationError)
async def validation_error_handler(request: Request, exc: ValidationError):
    return JSONResponse(
        status_code=400,
        content={"error": "Bad Request", "message": exc.message, "details": exc.details},
    )


@app.exception_handler(DuplicateRecordError)
async def duplicate_record_handler(request: Request, exc: DuplicateRecordError):
    return JSONResponse(
        status_code=409,
        content={"error": "Conflict", "message": exc.message, "details": exc.details},
    )


@app.exception_handler(SeederError)
async def seeder_error_handler(request: Request, exc: SeederError):
    return JSONResponse(
        status_code=500,
        content={"error": "Internal Server Error", "message": exc.message, "details": exc.details},
    )


class HealthResponse(BaseModel):
    status: str
    database: str
    transactions_supported: bool
    timestamp: str


class CleanupRequest(BaseModel):
    report_id: Optional[str] = None
    purge_all: bool = False
    keep_versions: int = 3
    max_age_days: Optional[int] = None
    dry_run: bool = False


def _serialize_value(value):
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return _serialize_record(value)
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    return value


def _serialize_record(record: dict) -> dict:
    serialized = {}
    for key, value in record.items():
        out_key = "id" if key == "_id" else key
        serialized[out_key] = _serialize_value(value)
    return serialized


@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    try:
        db = get_db()
        db.client.admin.command("ping")
        return HealthResponse(
            status="healthy",
            database=db._db_name,
            transactions_supported=db.supports_transactions,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Unhealthy: {exc}")


@app.get("/api/records", dependencies=[Depends(verify_api_key)])
async def list_records(
    active_only: bool = Query(True),
    region: Optional[str] = Query(None),
    regulation: Optional[str] = Query(None),
    csi_id: Optional[str] = Query(None),
    limit: int = Query(100, le=1000),
    skip: int = Query(0, ge=0),
):
    db = get_db()
    query: Dict[str, Any] = {"_id": {"$type": "objectId"}}   # exclude counter sentinel doc
    if active_only:
        query["active"] = True
    if region:
        query["region"] = region
    if regulation:
        query["regulation"] = regulation
    if csi_id:
        query["csi_id"] = csi_id

    cursor = db.metadata_collection.find(query).skip(skip).limit(limit)
    records = [_serialize_record(r) for r in cursor]
    total = db.metadata_collection.count_documents(query)

    logger.info("api.list_records query=%s total=%d returned=%d", query, total, len(records))
    return {"records": records, "total": total, "limit": limit, "skip": skip}


@app.get("/api/records/{report_id}", dependencies=[Depends(verify_api_key)])
async def get_record(report_id: str, version: Optional[int] = Query(None)):
    db = get_db()

    if version is not None:
        record = db.metadata_collection.find_one({"report_id": report_id, "version": version})
    else:
        record = db.metadata_collection.find_one({"report_id": report_id, "active": True})

    if not record:
        raise RecordNotFoundError(f"Record not found: {report_id}")

    logger.info("api.get_record report_id=%s version=%s", report_id, record.get("version"))
    return _serialize_record(record)


@app.get("/api/records/{report_id}/history", dependencies=[Depends(verify_api_key)])
async def get_record_history(report_id: str):
    db = get_db()
    # Resolve composite key from report_id anchor
    anchor = db.metadata_collection.find_one({"report_id": report_id})
    if not anchor:
        raise RecordNotFoundError(f"No records found: {report_id}")
    records = list(
        db.metadata_collection.find({
            "csi_id": anchor["csi_id"],
            "regulation": anchor["regulation"],
            "region": anchor["region"],
        }).sort("version", 1)
    )

    logger.info("api.get_history report_id=%s versions=%d", report_id, len(records))
    return {
        "report_id": report_id,
        "total_versions": len(records),
        "versions": [_serialize_record(r) for r in records],
    }


@app.get("/api/records/{report_id}/export", dependencies=[Depends(verify_api_key)])
async def export_record(report_id: str, version: Optional[int] = Query(None)):
    from src.services.export_service import export_bundle

    with tempfile.TemporaryDirectory() as tmpdir:
        result = export_bundle(
            report_id=report_id, output_dir=tmpdir,
            version=version, verify_checksums=True,
        )

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_type, file_path in result.get("files", {}).items():
                if not str(file_path).startswith("ERROR"):
                    p = Path(file_path)
                    if p.exists():
                        zf.write(p, arcname=p.name)

        zip_buffer.seek(0)
        filename = f"{report_id}_v{result.get('version', 'latest')}.zip"

        logger.info("api.export report_id=%s version=%s", report_id, result.get("version"))
        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )


@app.post("/api/cleanup", dependencies=[Depends(verify_api_key)])
async def run_cleanup(request: CleanupRequest):
    from src.services.cleanup_service import purge_old_versions, purge_all_old_versions, purge_by_age

    if request.max_age_days:
        result = purge_by_age(max_age_days=request.max_age_days, dry_run=request.dry_run)
    elif request.report_id:
        result = purge_old_versions(request.report_id, keep_versions=request.keep_versions, dry_run=request.dry_run)
    elif request.purge_all:
        result = purge_all_old_versions(keep_versions=request.keep_versions, dry_run=request.dry_run)
    else:
        raise HTTPException(status_code=400, detail="Specify report_id, purge_all, or max_age_days.")

    logger.info("api.cleanup result=%s", result)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Remote seeding endpoints (for external regulation repos)
# ─────────────────────────────────────────────────────────────────────────────

class SeedBundleRequest(BaseModel):
    """
    Payload for seeding a single bundle inline.
    File contents are base64-encoded strings; file names are used for
    original_filename and extension validation.
    """
    csi_id: str
    region: str
    regulation: str
    json_config_filename: str           # e.g. "mas_trm_report.json"
    json_config_content: str            # base64-encoded file bytes
    sql_file_filename: str              # e.g. "mas_trm_query.sql"
    sql_file_content: str              # base64-encoded file bytes
    template_filename: Optional[str] = None
    template_content: Optional[str] = None  # base64-encoded, only if template_filename set


class SeedManifestRequest(BaseModel):
    """
    Payload for seeding multiple bundles at once (same as seed.yaml but as JSON).
    Each item is a SeedBundleRequest.
    """
    bundles: List[SeedBundleRequest]


class ModifyBundleRequest(BaseModel):
    json_config_filename: Optional[str] = None
    json_config_content: Optional[str] = None  # base64-encoded
    sql_file_filename: Optional[str] = None
    sql_file_content: Optional[str] = None     # base64-encoded
    template_filename: Optional[str] = None
    template_content: Optional[str] = None     # base64-encoded


def _decode_and_write(b64_content: str, filename: str, tmpdir: str) -> Path:
    """Decode a base64 string and write it as a temp file; return its Path."""
    import base64
    raw = base64.b64decode(b64_content)
    out = Path(tmpdir) / filename
    out.write_bytes(raw)
    return out


@app.post("/api/seed/bundle", dependencies=[Depends(verify_api_key)], status_code=201)
async def seed_bundle(req: SeedBundleRequest):
    """
    Seed a single bundle from base64-encoded file contents.
    Called by external regulation repos via their CI/CD pipeline.

    Returns the assigned report_id and the action taken (created/updated/skipped).
    """
    from src.services.seed_service import _process_bundle as _pb
    from src.utils.validator import validate_json_config, validate_sql_content

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            json_path = _decode_and_write(req.json_config_content, req.json_config_filename, tmpdir)
            sql_path = _decode_and_write(req.sql_file_content, req.sql_file_filename, tmpdir)
            tmpl_path = None
            if req.template_filename and req.template_content:
                tmpl_path = _decode_and_write(req.template_content, req.template_filename, tmpdir)

            config = validate_json_config(str(json_path))
            validate_sql_content(str(sql_path))

            bundle = {
                "csi_id": req.csi_id,
                "region": req.region,
                "regulation": req.regulation,
                "json_config": str(json_path),
                "sql_file": str(sql_path),
                "template": str(tmpl_path) if tmpl_path else None,
            }

            status, report_id, version, reason = _pb(bundle, config)

            logger.info(
                "api.seed_bundle csi_id=%s regulation=%s region=%s status=%s report_id=%s",
                req.csi_id, req.regulation, req.region, status, report_id,
            )
            return {
                "status": status,
                "report_id": report_id,
                "version": version,
                "reason": reason,
            }

        except (ValidationError, DuplicateRecordError, SeederError):
            raise
        except Exception as exc:
            logger.error("api.seed_bundle_failed error=%s", exc)
            raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/seed/manifest", dependencies=[Depends(verify_api_key)])
async def seed_manifest(req: SeedManifestRequest):
    """
    Seed multiple bundles at once from inline base64-encoded file contents.
    Called by external regulation repos with their full manifest payload.

    Returns the same summary structure as the CLI `seed` command.
    """
    from src.services.seed_service import _process_bundle as _pb
    from src.utils.validator import validate_json_config, validate_sql_content

    if not req.bundles:
        raise HTTPException(status_code=400, detail="'bundles' list is empty")

    results: dict = {
        "created": 0, "updated": 0, "skipped": 0, "failed": 0,
        "total": len(req.bundles), "details": [], "errors": [],
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        for i, b in enumerate(req.bundles):
            label = b.csi_id or f"bundle-{i}"
            detail: dict = {
                "index": i, "label": label, "status": "failed",
                "report_id": None, "version": None, "reason": "", "error": None,
            }
            try:
                json_path = _decode_and_write(b.json_config_content, b.json_config_filename, tmpdir)
                sql_path = _decode_and_write(b.sql_file_content, b.sql_file_filename, tmpdir)
                tmpl_path = None
                if b.template_filename and b.template_content:
                    tmpl_path = _decode_and_write(b.template_content, b.template_filename, tmpdir)

                config = validate_json_config(str(json_path), index=i)
                validate_sql_content(str(sql_path), index=i)

                bundle = {
                    "csi_id": b.csi_id, "region": b.region, "regulation": b.regulation,
                    "json_config": str(json_path), "sql_file": str(sql_path),
                    "template": str(tmpl_path) if tmpl_path else None,
                }

                status, report_id, version, reason = _pb(bundle, config)
                detail.update({"status": status, "report_id": report_id, "version": version, "reason": reason})
                results[status] += 1
                logger.info("api.seed_manifest bundle=%s status=%s report_id=%s", label, status, report_id)

            except Exception as exc:
                detail["error"] = str(exc)
                detail["reason"] = "Processing error"
                results["failed"] += 1
                results["errors"].append(f"Bundle '{label}': {exc}")
                logger.error("api.seed_manifest bundle=%s FAILED: %s", label, exc)

            results["details"].append(detail)

    logger.info(
        "api.seed_manifest done total=%d created=%d updated=%d skipped=%d failed=%d",
        results["total"], results["created"], results["updated"],
        results["skipped"], results["failed"],
    )
    return results


@app.patch("/api/records/{report_id}", dependencies=[Depends(verify_api_key)])
async def modify_record_api(report_id: str, req: ModifyBundleRequest):
    """
    Modify a specific record by internal UUID report_id using base64-encoded files.
    At least one file must be provided.
    """
    import uuid as _uuid
    try:
        _uuid.UUID(report_id)
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid report_id format: '{report_id}' must be a UUID (e.g. 'a1b2c3d4-e5f6-7890-abcd-ef1234567890')",
        )

    from src.services.seed_service import modify_record_by_id

    if not any([req.json_config_content, req.sql_file_content, req.template_content]):
        raise HTTPException(status_code=400, detail="At least one file must be provided.")

    with tempfile.TemporaryDirectory() as tmpdir:
        json_path = str(_decode_and_write(req.json_config_content, req.json_config_filename, tmpdir)) \
            if req.json_config_content and req.json_config_filename else None
        sql_path = str(_decode_and_write(req.sql_file_content, req.sql_file_filename, tmpdir)) \
            if req.sql_file_content and req.sql_file_filename else None
        tmpl_path = str(_decode_and_write(req.template_content, req.template_filename, tmpdir)) \
            if req.template_content and req.template_filename else None

        new_version = modify_record_by_id(
            report_id=report_id,
            json_config_path=json_path,
            sql_file_path=sql_path,
            template_path=tmpl_path,
        )

    logger.info("api.modify report_id=%s new_version=%d", report_id, new_version)
    return {"report_id": report_id, "version": new_version, "status": "updated"}
