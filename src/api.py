"""FastAPI REST API for the MongoDB Document Seeder."""

import io
import logging
import re
import secrets
import tempfile
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Any, Dict, List

from bson import ObjectId
from fastapi import FastAPI, HTTPException, Query, Depends, Security, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field, validator

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

# Maximum base64 payload size per file (50 MB)
MAX_FILE_PAYLOAD_BYTES = 50 * 1024 * 1024
# Maximum string field length for identifiers
MAX_FIELD_LENGTH = 256
# Filename sanitization pattern
_SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9_\-\.]")

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _get_api_key() -> str:
    """Read API key fresh from settings (not cached at module level)."""
    return get_settings().api_key


async def verify_api_key(key: Optional[str] = Security(api_key_header)):
    """Constant-time API key verification to prevent timing attacks."""
    expected = _get_api_key()
    if not expected:
        return  # Auth disabled
    if key is None or not secrets.compare_digest(key.encode(), expected.encode()):
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
    docs_url="/api/docs" if not get_settings().is_production else None,
    redoc_url="/api/redoc" if not get_settings().is_production else None,
)

# ── CORS — restrictive by default ──────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if not get_settings().is_production else [],
    allow_methods=["GET", "POST", "PATCH"],
    allow_headers=["X-API-Key", "Content-Type"],
    allow_credentials=False,
)


# ── Security headers middleware ────────────────────────────────────────────
@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Cache-Control"] = "no-store"
    return response


# ── Request size limiting middleware ───────────────────────────────────────
MAX_REQUEST_BODY = 100 * 1024 * 1024  # 100 MB


@app.middleware("http")
async def limit_request_size(request: Request, call_next):
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_REQUEST_BODY:
        return JSONResponse(
            status_code=413,
            content={"error": "Payload Too Large", "message": f"Max request body is {MAX_REQUEST_BODY} bytes"},
        )
    return await call_next(request)


# ── Exception handlers ────────────────────────────────────────────────────
def _safe_error_response(message: str, details: dict) -> dict:
    """Strip internal details in production."""
    if get_settings().is_production:
        return {}
    return details


@app.exception_handler(RecordNotFoundError)
async def record_not_found_handler(request: Request, exc: RecordNotFoundError):
    return JSONResponse(
        status_code=404,
        content={"error": "Not Found", "message": exc.message, "details": _safe_error_response(exc.message, exc.details)},
    )


@app.exception_handler(ValidationError)
async def validation_error_handler(request: Request, exc: ValidationError):
    return JSONResponse(
        status_code=400,
        content={"error": "Bad Request", "message": exc.message, "details": _safe_error_response(exc.message, exc.details)},
    )


@app.exception_handler(DuplicateRecordError)
async def duplicate_record_handler(request: Request, exc: DuplicateRecordError):
    return JSONResponse(
        status_code=409,
        content={"error": "Conflict", "message": exc.message, "details": _safe_error_response(exc.message, exc.details)},
    )


@app.exception_handler(SeederError)
async def seeder_error_handler(request: Request, exc: SeederError):
    logger.error("api.unhandled_seeder_error message=%s", exc.message)
    msg = exc.message if not get_settings().is_production else "Internal server error"
    return JSONResponse(
        status_code=500,
        content={"error": "Internal Server Error", "message": msg, "details": _safe_error_response(msg, exc.details)},
    )


# ── Models ─────────────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str
    database: str
    transactions_supported: bool
    timestamp: str


class CleanupRequest(BaseModel):
    report_id: Optional[str] = None
    purge_all: bool = False
    keep_versions: int = Field(3, ge=1, le=100)
    max_age_days: Optional[int] = Field(None, ge=1, le=3650)
    dry_run: bool = False


def _validate_identifier(v: str, field_name: str) -> str:
    if not v or len(v) > MAX_FIELD_LENGTH:
        raise ValueError(f"{field_name} must be 1-{MAX_FIELD_LENGTH} characters")
    return v.strip()


def _validate_b64_size(v: str, field_name: str) -> str:
    if len(v) > MAX_FILE_PAYLOAD_BYTES:
        raise ValueError(f"{field_name} exceeds maximum size of {MAX_FILE_PAYLOAD_BYTES} bytes")
    return v


class SeedBundleRequest(BaseModel):
    """Payload for seeding a single bundle inline.
    File contents are base64-encoded strings."""
    csi_id: str = Field(..., max_length=MAX_FIELD_LENGTH)
    region: str = Field(..., max_length=MAX_FIELD_LENGTH)
    regulation: str = Field(..., max_length=MAX_FIELD_LENGTH)
    json_config_filename: str = Field(..., max_length=MAX_FIELD_LENGTH)
    json_config_content: str
    sql_file_filename: str = Field(..., max_length=MAX_FIELD_LENGTH)
    sql_file_content: str
    template_filename: Optional[str] = Field(None, max_length=MAX_FIELD_LENGTH)
    template_content: Optional[str] = None

    @validator("json_config_content", "sql_file_content")
    def check_content_size(cls, v):
        return _validate_b64_size(v, "file_content")

    @validator("template_content")
    def check_template_size(cls, v):
        if v is not None:
            return _validate_b64_size(v, "template_content")
        return v


class SeedManifestRequest(BaseModel):
    """Payload for seeding multiple bundles at once."""
    bundles: List[SeedBundleRequest] = Field(..., max_items=100)


class ModifyBundleRequest(BaseModel):
    json_config_filename: Optional[str] = Field(None, max_length=MAX_FIELD_LENGTH)
    json_config_content: Optional[str] = None
    sql_file_filename: Optional[str] = Field(None, max_length=MAX_FIELD_LENGTH)
    sql_file_content: Optional[str] = None
    template_filename: Optional[str] = Field(None, max_length=MAX_FIELD_LENGTH)
    template_content: Optional[str] = None


# ── Helpers ────────────────────────────────────────────────────────────────

def _sanitize_filename(name: str) -> str:
    """Remove dangerous characters from filenames for Content-Disposition."""
    sanitized = _SAFE_FILENAME_RE.sub("_", name)
    # Prevent directory traversal
    sanitized = sanitized.replace("..", "_")
    return sanitized[:200]  # Cap length


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


# ── Endpoints ──────────────────────────────────────────────────────────────

@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    try:
        db = get_db()
        db.client.admin.command("ping")
        resp = HealthResponse(
            status="healthy",
            database=db._db_name if not get_settings().is_production else "***",
            transactions_supported=db.supports_transactions,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        return resp
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Unhealthy: {exc}")


@app.get("/api/records", dependencies=[Depends(verify_api_key)])
async def list_records(
    active_only: bool = Query(True),
    region: Optional[str] = Query(None, max_length=MAX_FIELD_LENGTH),
    regulation: Optional[str] = Query(None, max_length=MAX_FIELD_LENGTH),
    csi_id: Optional[str] = Query(None, max_length=MAX_FIELD_LENGTH),
    limit: int = Query(100, ge=1, le=1000),
    skip: int = Query(0, ge=0),
):
    db = get_db()
    query: Dict[str, Any] = {"_id": {"$type": "objectId"}}
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
        safe_id = _sanitize_filename(report_id)
        filename = f"{safe_id}_v{result.get('version', 'latest')}.zip"

        logger.info("api.export report_id=%s version=%s", report_id, result.get("version"))
        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )


# ─────────────────────────────────────────────────────────────────────────────
# Direct file streaming for ReportGen integration (no export-to-disk needed)
# ─────────────────────────────────────────────────────────────────────────────

VALID_FILE_KEYS = {"json_config", "sql_file", "template"}


@app.get("/api/records/{report_id}/files/{file_key}", dependencies=[Depends(verify_api_key)])
async def stream_file(
    report_id: str,
    file_key: str,
    version: Optional[int] = Query(None),
):
    """Stream a single file directly from GridFS without writing to disk.

    This is the primary integration point for ReportGen — it can fetch
    files directly via HTTP without any export step.

    Args:
        report_id: The record's UUID report_id.
        file_key:  One of 'json_config', 'sql_file', 'template'.
        version:   Specific version (default: active).
    """
    if file_key not in VALID_FILE_KEYS:
        raise HTTPException(status_code=400, detail=f"Invalid file_key '{file_key}'. Must be one of: {VALID_FILE_KEYS}")

    from src.services.gridfs_service import download_from_gridfs

    db = get_db()

    if version is not None:
        record = db.metadata_collection.find_one({"report_id": report_id, "version": version})
    else:
        record = db.metadata_collection.find_one({"report_id": report_id, "active": True})

    if not record:
        raise RecordNotFoundError(f"Record not found: {report_id}")

    contents = record.get("file_contents", {})
    original_files = record.get("original_files", {})

    id_key = f"{file_key}_id"
    gridfs_id_str = contents.get(id_key)
    if not gridfs_id_str:
        raise HTTPException(status_code=404, detail=f"File '{file_key}' not found in record")

    try:
        file_bytes, metadata = download_from_gridfs(db.fs, ObjectId(gridfs_id_str))
    except Exception as exc:
        logger.error("api.stream_file failed report_id=%s file_key=%s error=%s", report_id, file_key, exc)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve file: {exc}")

    original_name = original_files.get(file_key, f"{file_key}.bin")
    content_type = metadata.get("content_type", "application/octet-stream")
    safe_name = _sanitize_filename(original_name)

    logger.info("api.stream_file report_id=%s file_key=%s size=%d", report_id, file_key, len(file_bytes))
    return StreamingResponse(
        io.BytesIO(file_bytes),
        media_type=content_type,
        headers={
            "Content-Disposition": f'attachment; filename="{safe_name}"',
            "X-Checksum": record.get("checksums", {}).get(file_key, ""),
            "X-Original-Filename": safe_name,
        },
    )


@app.get("/api/records/{report_id}/files", dependencies=[Depends(verify_api_key)])
async def list_record_files(
    report_id: str,
    version: Optional[int] = Query(None),
):
    """List available files for a record with metadata.

    ReportGen can call this first to discover which files exist,
    then stream each individually.
    """
    db = get_db()

    if version is not None:
        record = db.metadata_collection.find_one({"report_id": report_id, "version": version})
    else:
        record = db.metadata_collection.find_one({"report_id": report_id, "active": True})

    if not record:
        raise RecordNotFoundError(f"Record not found: {report_id}")

    contents = record.get("file_contents", {})
    original_files = record.get("original_files", {})
    checksums = record.get("checksums", {})
    file_sizes = record.get("file_sizes", {})

    files = {}
    for key in VALID_FILE_KEYS:
        id_key = f"{key}_id"
        if contents.get(id_key):
            files[key] = {
                "filename": original_files.get(key),
                "checksum": checksums.get(key),
                "size": file_sizes.get(key),
                "stream_url": f"/api/records/{report_id}/files/{key}",
            }

    return {
        "report_id": report_id,
        "version": record.get("version"),
        "active": record.get("active"),
        "files": files,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Cleanup & Seeding endpoints
# ─────────────────────────────────────────────────────────────────────────────

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


def _decode_and_write(b64_content: str, filename: str, tmpdir: str) -> Path:
    """Decode a base64 string and write it as a temp file; return its Path."""
    import base64

    # Sanitize filename to prevent path traversal
    safe_name = Path(filename).name  # Strip directory components
    if not safe_name or safe_name in (".", ".."):
        raise ValidationError(f"Invalid filename: '{filename}'")

    raw = base64.b64decode(b64_content)
    out = Path(tmpdir) / safe_name
    out.write_bytes(raw)
    return out


@app.post("/api/seed/bundle", dependencies=[Depends(verify_api_key)], status_code=201)
async def seed_bundle(req: SeedBundleRequest):
    """Seed a single bundle from base64-encoded file contents."""
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
    """Seed multiple bundles at once from inline base64-encoded file contents."""
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
    """Modify a specific record by internal UUID report_id."""
    import uuid as _uuid
    try:
        _uuid.UUID(report_id)
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid report_id format: '{report_id}' must be a UUID",
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
