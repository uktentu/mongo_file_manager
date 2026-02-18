"""FastAPI REST API for the MongoDB Document Seeder."""

import io
import logging
import os
import secrets
import tempfile
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from bson import ObjectId
from fastapi import FastAPI, HTTPException, Query, Depends, Security
from fastapi.responses import StreamingResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

from src.config.database import get_db, reset_db
from src.errors.exceptions import SeederError, RecordNotFoundError

logger = logging.getLogger(__name__)

API_KEY = os.getenv("API_KEY", "")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(key: Optional[str] = Security(api_key_header)):
    if not API_KEY:
        return
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


@asynccontextmanager
async def lifespan(app: FastAPI):
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


class HealthResponse(BaseModel):
    status: str
    database: str
    transactions_supported: bool
    timestamp: str


class CleanupRequest(BaseModel):
    unique_id: Optional[str] = None
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
    query = {}
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


@app.get("/api/records/{unique_id}", dependencies=[Depends(verify_api_key)])
async def get_record(unique_id: str, version: Optional[int] = Query(None)):
    db = get_db()

    if version is not None:
        record = db.metadata_collection.find_one({"unique_id": unique_id, "version": version})
    else:
        record = db.metadata_collection.find_one({"unique_id": unique_id, "active": True})

    if not record:
        raise HTTPException(status_code=404, detail=f"Record not found: {unique_id}")

    logger.info("api.get_record unique_id=%s version=%s", unique_id, record.get("version"))
    return _serialize_record(record)


@app.get("/api/records/{unique_id}/history", dependencies=[Depends(verify_api_key)])
async def get_record_history(unique_id: str):
    db = get_db()
    records = list(db.metadata_collection.find({"unique_id": unique_id}).sort("version", 1))
    if not records:
        raise HTTPException(status_code=404, detail=f"No records found: {unique_id}")

    logger.info("api.get_history unique_id=%s versions=%d", unique_id, len(records))
    return {
        "unique_id": unique_id,
        "total_versions": len(records),
        "versions": [_serialize_record(r) for r in records],
    }


@app.get("/api/records/{unique_id}/export", dependencies=[Depends(verify_api_key)])
async def export_record(unique_id: str, version: Optional[int] = Query(None)):
    from src.services.export_service import export_bundle

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            result = export_bundle(
                unique_id=unique_id, output_dir=tmpdir,
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
            filename = f"{unique_id}_v{result.get('version', 'latest')}.zip"

            logger.info("api.export unique_id=%s version=%s", unique_id, result.get("version"))
            return StreamingResponse(
                zip_buffer,
                media_type="application/zip",
                headers={"Content-Disposition": f"attachment; filename={filename}"},
            )

    except RecordNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except SeederError as exc:
        raise HTTPException(status_code=500, detail=exc.message)


@app.post("/api/cleanup", dependencies=[Depends(verify_api_key)])
async def run_cleanup(request: CleanupRequest):
    from src.services.cleanup_service import purge_old_versions, purge_all_old_versions, purge_by_age

    try:
        if request.max_age_days:
            result = purge_by_age(max_age_days=request.max_age_days, dry_run=request.dry_run)
        elif request.unique_id:
            result = purge_old_versions(request.unique_id, keep_versions=request.keep_versions, dry_run=request.dry_run)
        elif request.purge_all:
            result = purge_all_old_versions(keep_versions=request.keep_versions, dry_run=request.dry_run)
        else:
            raise HTTPException(status_code=400, detail="Specify unique_id, purge_all, or max_age_days.")

        logger.info("api.cleanup result=%s", result)
        return result

    except RecordNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except SeederError as exc:
        raise HTTPException(status_code=500, detail=exc.message)
