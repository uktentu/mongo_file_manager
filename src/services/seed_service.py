"""Seed service — bulk seeding, single creation, and append-only modification."""

import logging
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml

from src.config.database import get_db
from src.errors.exceptions import (
    DatabaseError,
    DuplicateRecordError,
    RecordNotFoundError,
    SeederError,
    ValidationError,
)
from src.models.schemas import (
    Checksums,
    FileReferences,
    FileSizes,
    MetadataDocument,
    OriginalFiles,
)
from src.services.audit_service import create_audit_entry
from src.services.gridfs_service import upload_to_gridfs, GridFSOrphanTracker
from src.utils.checksum import compute_file_checksum
from src.utils.unique_id import build_unique_id
from src.utils.validator import validate_json_config, validate_seed_bundle

logger = logging.getLogger(__name__)


def _detect_content_type(file_path: str) -> str:
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type or "application/octet-stream"


def seed_from_manifest(manifest_path: str | Path) -> dict[str, Any]:
    path = Path(manifest_path)
    if not path.exists():
        raise ValidationError(f"Manifest file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        manifest = yaml.safe_load(f)

    if not manifest or "bundles" not in manifest:
        raise ValidationError("Invalid manifest: must contain a 'bundles' key with a list of entries.")

    bundles = manifest["bundles"]
    if not isinstance(bundles, list) or len(bundles) == 0:
        raise ValidationError("Manifest 'bundles' must be a non-empty list.")

    base_dir = path.parent
    results = {"created": 0, "skipped": 0, "updated": 0, "failed": 0, "errors": []}

    logger.info("seed.manifest_loaded path=%s bundles=%d", path, len(bundles))

    for i, bundle in enumerate(bundles):
        bundle_label = bundle.get("csi_id", f"bundle-{i}")
        try:
            validated = validate_seed_bundle(bundle, base_dir)
            result = _seed_single_bundle(validated)
            results[result] += 1
            logger.info("seed.bundle_processed index=%d/%d label=%s result=%s", i + 1, len(bundles), bundle_label, result)
        except Exception as exc:
            results["failed"] += 1
            error_msg = f"Bundle '{bundle_label}': {exc}"
            results["errors"].append(error_msg)
            logger.error("seed.bundle_failed index=%d/%d label=%s error=%s", i + 1, len(bundles), bundle_label, exc)

    logger.info(
        "seed.manifest_complete created=%d updated=%d skipped=%d failed=%d",
        results["created"], results["updated"], results["skipped"], results["failed"],
    )
    return results


def _seed_single_bundle(bundle: dict) -> str:
    db = get_db()

    config = validate_json_config(bundle["json_config"])
    name = config["name"]
    out_file_name = config["outFileName"]

    unique_id = build_unique_id(
        regulation=bundle["regulation"],
        name=name,
        out_file_name=out_file_name,
        region=bundle["region"],
    )

    json_checksum = compute_file_checksum(bundle["json_config"])
    sql_checksum = compute_file_checksum(bundle["sql_file"])
    template_checksum = (
        compute_file_checksum(bundle["template"]) if bundle.get("template") else None
    )

    existing = db.metadata_collection.find_one({"unique_id": unique_id, "active": True})

    if existing:
        existing_checksums = existing.get("checksums", {})
        if (
            existing_checksums.get("json_config") == json_checksum
            and existing_checksums.get("sql_file") == sql_checksum
            and existing_checksums.get("template") == template_checksum
        ):
            logger.debug("seed.skipped unique_id=%s reason=checksums_match", unique_id)
            return "skipped"

        logger.info("seed.modifying unique_id=%s reason=checksums_changed", unique_id)
        precomputed = {"json_config": json_checksum, "sql_file": sql_checksum, "template": template_checksum}
        return _modify_record(unique_id, bundle, config, existing, precomputed_checksums=precomputed)

    precomputed = {"json_config": json_checksum, "sql_file": sql_checksum, "template": template_checksum}
    return _create_record(unique_id, bundle, config, precomputed_checksums=precomputed)


def create_single_record(
    csi_id: str,
    region: str,
    regulation: str,
    json_config_path: str,
    sql_file_path: str,
    template_path: Optional[str] = None,
) -> str:
    config = validate_json_config(json_config_path)
    name = config["name"]
    out_file_name = config["outFileName"]

    unique_id = build_unique_id(
        regulation=regulation, name=name, out_file_name=out_file_name, region=region
    )

    db = get_db()
    existing = db.metadata_collection.find_one({"unique_id": unique_id, "active": True})
    if existing:
        raise DuplicateRecordError(
            f"An active record already exists with unique_id '{unique_id}'. Use 'modify' command to update it."
        )

    bundle = {
        "csi_id": csi_id,
        "region": region,
        "regulation": regulation,
        "json_config": json_config_path,
        "sql_file": sql_file_path,
        "template": template_path,
    }

    _create_record(unique_id, bundle, config)
    return unique_id


def _create_record(unique_id: str, bundle: dict, config: dict, precomputed_checksums: dict | None = None) -> str:
    db = get_db()
    tracker = GridFSOrphanTracker()

    json_config_path = Path(bundle["json_config"])
    sql_file_path = Path(bundle["sql_file"])
    template_path = Path(bundle["template"]) if bundle.get("template") else None

    checksums = precomputed_checksums or {}
    json_checksum = checksums.get("json_config") or compute_file_checksum(json_config_path)
    sql_checksum = checksums.get("sql_file") or compute_file_checksum(sql_file_path)
    template_checksum = checksums.get("template") or (compute_file_checksum(template_path) if template_path else None)

    try:
        config_doc = {
            "unique_id": unique_id,
            "config": config,
            "uploaded_at": datetime.now(timezone.utc),
        }
        config_result = db.configs_collection.insert_one(config_doc)
        json_config_id = config_result.inserted_id
        tracker.track_config(db.configs_collection, json_config_id)

        sql_gridfs_id = upload_to_gridfs(
            bucket=db.sqlfiles_gridfs,
            file_path=sql_file_path,
            original_filename=sql_file_path.name,
            content_type="application/sql",
            extra_metadata={"unique_id": unique_id},
            orphan_tracker=tracker,
            precomputed_checksum=sql_checksum,
        )

        template_gridfs_id = None
        if template_path:
            template_gridfs_id = upload_to_gridfs(
                bucket=db.templates_gridfs,
                file_path=template_path,
                original_filename=template_path.name,
                content_type=_detect_content_type(str(template_path)),
                extra_metadata={"unique_id": unique_id},
                orphan_tracker=tracker,
                precomputed_checksum=template_checksum,
            )

        metadata = MetadataDocument(
            unique_id=unique_id,
            csi_id=bundle["csi_id"],
            region=bundle["region"],
            regulation=bundle["regulation"],
            name=config["name"],
            out_file_name=config["outFileName"],
            original_files=OriginalFiles(
                json_config=json_config_path.name,
                template=template_path.name if template_path else None,
                sql_file=sql_file_path.name,
            ),
            file_references=FileReferences(
                json_config_id=json_config_id,
                template_gridfs_id=template_gridfs_id,
                sql_gridfs_id=sql_gridfs_id,
            ),
            checksums=Checksums(
                json_config=json_checksum,
                template=template_checksum,
                sql_file=sql_checksum,
            ),
            file_sizes=FileSizes(
                json_config=json_config_path.stat().st_size,
                template=template_path.stat().st_size if template_path else None,
                sql_file=sql_file_path.stat().st_size,
            ),
            uploaded_at=datetime.now(timezone.utc),
            active=True,
            version=1,
            audit_log=[create_audit_entry("CREATED", "Initial seed")],
        )

        db.metadata_collection.insert_one(metadata.to_mongo_dict())
        tracker.clear()

        logger.info("seed.created unique_id=%s version=1 csi_id=%s region=%s", unique_id, bundle["csi_id"], bundle["region"])
        return "created"

    except Exception as exc:
        cleaned = tracker.cleanup()
        if cleaned:
            logger.warning("seed.orphan_cleanup unique_id=%s cleaned=%d", unique_id, cleaned)
        raise DatabaseError(f"Failed to create record '{unique_id}': {exc}") from exc


def _modify_record(unique_id: str, bundle: dict, config: dict, existing: dict, precomputed_checksums: dict | None = None) -> str:
    db = get_db()
    old_version = existing.get("version", 1)
    new_version = old_version + 1
    tracker = GridFSOrphanTracker()

    checksums = precomputed_checksums or {}
    json_config_path = Path(bundle["json_config"])
    sql_file_path = Path(bundle["sql_file"])
    template_path = Path(bundle["template"]) if bundle.get("template") else None

    json_checksum = checksums.get("json_config") or compute_file_checksum(json_config_path)
    sql_checksum = checksums.get("sql_file") or compute_file_checksum(sql_file_path)
    template_checksum = checksums.get("template") or (compute_file_checksum(template_path) if template_path else None)

    def _do_modify(session=None):
        db.metadata_collection.update_one(
            {"_id": existing["_id"]},
            {
                "$set": {"active": False},
                "$push": {"audit_log": create_audit_entry("DEACTIVATED", f"Superseded by version {new_version}")},
            },
            session=session,
        )

        config_doc = {
            "unique_id": unique_id,
            "config": config,
            "uploaded_at": datetime.now(timezone.utc),
            "version": new_version,
        }
        config_result = db.configs_collection.insert_one(config_doc, session=session)
        tracker.track_config(db.configs_collection, config_result.inserted_id)

        sql_gridfs_id = upload_to_gridfs(
            bucket=db.sqlfiles_gridfs,
            file_path=sql_file_path,
            original_filename=sql_file_path.name,
            content_type="application/sql",
            extra_metadata={"unique_id": unique_id, "version": new_version},
            orphan_tracker=tracker,
            precomputed_checksum=sql_checksum,
        )

        template_gridfs_id = None
        if template_path:
            template_gridfs_id = upload_to_gridfs(
                bucket=db.templates_gridfs,
                file_path=template_path,
                original_filename=template_path.name,
                content_type=_detect_content_type(str(template_path)),
                extra_metadata={"unique_id": unique_id, "version": new_version},
                orphan_tracker=tracker,
                precomputed_checksum=template_checksum,
            )

        metadata = MetadataDocument(
            unique_id=unique_id,
            csi_id=bundle["csi_id"],
            region=bundle["region"],
            regulation=bundle["regulation"],
            name=config["name"],
            out_file_name=config["outFileName"],
            original_files=OriginalFiles(
                json_config=json_config_path.name,
                template=template_path.name if template_path else None,
                sql_file=sql_file_path.name,
            ),
            file_references=FileReferences(
                json_config_id=config_result.inserted_id,
                template_gridfs_id=template_gridfs_id,
                sql_gridfs_id=sql_gridfs_id,
            ),
            checksums=Checksums(
                json_config=json_checksum,
                template=template_checksum,
                sql_file=sql_checksum,
            ),
            file_sizes=FileSizes(
                json_config=json_config_path.stat().st_size,
                template=template_path.stat().st_size if template_path else None,
                sql_file=sql_file_path.stat().st_size,
            ),
            uploaded_at=datetime.now(timezone.utc),
            active=True,
            version=new_version,
            audit_log=[create_audit_entry("MODIFIED", f"Updated from version {old_version} to {new_version}")],
        )

        db.metadata_collection.insert_one(metadata.to_mongo_dict(), session=session)

    try:
        if db.supports_transactions:
            with db.start_session() as session:
                session.start_transaction()
                try:
                    _do_modify(session=session)
                    session.commit_transaction()
                except Exception:
                    session.abort_transaction()
                    raise
        else:
            logger.warning("seed.modify_no_transaction unique_id=%s standalone=true", unique_id)
            _do_modify(session=None)

        tracker.clear()
        logger.info("seed.modified unique_id=%s version=%d->%d", unique_id, old_version, new_version)
        return "updated"

    except Exception as exc:
        cleaned = tracker.cleanup()
        if cleaned:
            logger.warning("seed.orphan_cleanup unique_id=%s cleaned=%d", unique_id, cleaned)
        raise DatabaseError(f"Transaction failed during modify of '{unique_id}': {exc}") from exc


def modify_record_by_id(
    unique_id: str,
    json_config_path: Optional[str] = None,
    sql_file_path: Optional[str] = None,
    template_path: Optional[str] = None,
) -> int:
    if not any([json_config_path, sql_file_path, template_path]):
        raise ValidationError("At least one file must be provided for modification.")

    db = get_db()
    existing = db.metadata_collection.find_one({"unique_id": unique_id, "active": True})
    if not existing:
        raise RecordNotFoundError(f"No active record found with unique_id '{unique_id}'")

    if json_config_path:
        config = validate_json_config(json_config_path)
    else:
        old_config_doc = db.configs_collection.find_one({"_id": existing["file_references"]["json_config_id"]})
        if not old_config_doc:
            raise DatabaseError("Could not find previous JSON config in database.")
        config = old_config_doc["config"]

    old_version = existing.get("version", 1)
    new_version = old_version + 1
    tracker = GridFSOrphanTracker()

    def _do_modify(session=None):
        nonlocal config

        db.metadata_collection.update_one(
            {"_id": existing["_id"]},
            {
                "$set": {"active": False},
                "$push": {"audit_log": create_audit_entry("DEACTIVATED", f"Superseded by version {new_version}")},
            },
            session=session,
        )

        if json_config_path:
            config = validate_json_config(json_config_path)
            jc_path = Path(json_config_path)
            config_doc = {
                "unique_id": unique_id, "config": config,
                "uploaded_at": datetime.now(timezone.utc), "version": new_version,
            }
            config_result = db.configs_collection.insert_one(config_doc, session=session)
            new_json_config_id = config_result.inserted_id
            tracker.track_config(db.configs_collection, new_json_config_id)
            new_json_checksum = compute_file_checksum(jc_path)
            new_json_size = jc_path.stat().st_size
            new_json_original = jc_path.name
        else:
            new_json_config_id = existing["file_references"]["json_config_id"]
            new_json_checksum = existing["checksums"]["json_config"]
            new_json_size = existing["file_sizes"]["json_config"]
            new_json_original = existing["original_files"]["json_config"]

        if sql_file_path:
            sq_path = Path(sql_file_path)
            new_sql_checksum = compute_file_checksum(sq_path)
            new_sql_gridfs_id = upload_to_gridfs(
                bucket=db.sqlfiles_gridfs, file_path=sq_path,
                original_filename=sq_path.name, content_type="application/sql",
                extra_metadata={"unique_id": unique_id, "version": new_version},
                orphan_tracker=tracker,
                precomputed_checksum=new_sql_checksum,
            )
            new_sql_size = sq_path.stat().st_size
            new_sql_original = sq_path.name
        else:
            new_sql_gridfs_id = existing["file_references"]["sql_gridfs_id"]
            new_sql_checksum = existing["checksums"]["sql_file"]
            new_sql_size = existing["file_sizes"]["sql_file"]
            new_sql_original = existing["original_files"]["sql_file"]

        if template_path:
            tp_path = Path(template_path)
            new_template_checksum = compute_file_checksum(tp_path)
            new_template_gridfs_id = upload_to_gridfs(
                bucket=db.templates_gridfs, file_path=tp_path,
                original_filename=tp_path.name, content_type=_detect_content_type(str(tp_path)),
                extra_metadata={"unique_id": unique_id, "version": new_version},
                orphan_tracker=tracker,
                precomputed_checksum=new_template_checksum,
            )
            new_template_size = tp_path.stat().st_size
            new_template_original = tp_path.name
        else:
            new_template_gridfs_id = existing["file_references"].get("template_gridfs_id")
            new_template_checksum = existing["checksums"].get("template")
            new_template_size = existing["file_sizes"].get("template")
            new_template_original = existing["original_files"].get("template")

        changed_parts = []
        if json_config_path:
            changed_parts.append("json_config")
        if sql_file_path:
            changed_parts.append("sql_file")
        if template_path:
            changed_parts.append("template")

        metadata = MetadataDocument(
            unique_id=unique_id,
            csi_id=existing["csi_id"],
            region=existing["region"],
            regulation=existing["regulation"],
            name=config["name"] if isinstance(config, dict) else existing["name"],
            out_file_name=config["outFileName"] if isinstance(config, dict) else existing["out_file_name"],
            original_files=OriginalFiles(
                json_config=new_json_original, template=new_template_original, sql_file=new_sql_original,
            ),
            file_references=FileReferences(
                json_config_id=new_json_config_id, template_gridfs_id=new_template_gridfs_id, sql_gridfs_id=new_sql_gridfs_id,
            ),
            checksums=Checksums(
                json_config=new_json_checksum, template=new_template_checksum, sql_file=new_sql_checksum,
            ),
            file_sizes=FileSizes(
                json_config=new_json_size, template=new_template_size, sql_file=new_sql_size,
            ),
            uploaded_at=datetime.now(timezone.utc),
            active=True,
            version=new_version,
            audit_log=[create_audit_entry("MODIFIED", f"Updated files: {', '.join(changed_parts)} (v{old_version} → v{new_version})")],
        )

        db.metadata_collection.insert_one(metadata.to_mongo_dict(), session=session)

    try:
        if db.supports_transactions:
            with db.start_session() as session:
                session.start_transaction()
                try:
                    _do_modify(session=session)
                    session.commit_transaction()
                except Exception:
                    session.abort_transaction()
                    raise
        else:
            _do_modify(session=None)

        tracker.clear()
        logger.info("seed.modified_by_id unique_id=%s version=%d->%d", unique_id, old_version, new_version)
        return new_version

    except Exception as exc:
        cleaned = tracker.cleanup()
        if cleaned:
            logger.warning("seed.orphan_cleanup unique_id=%s cleaned=%d", unique_id, cleaned)
        raise DatabaseError(f"Transaction failed during modify of '{unique_id}': {exc}") from exc
