"""Cleanup service — retention policy and version purging."""

import logging
from typing import Any, Dict
from datetime import datetime, timezone, timedelta

from bson import ObjectId

from src.config.database import get_db
from src.errors.exceptions import RecordNotFoundError
from src.services.gridfs_service import delete_from_gridfs

logger = logging.getLogger(__name__)


def _is_real_record(record: dict) -> bool:
    """Return True only for genuine metadata records, excluding the counter sentinel doc."""
    return isinstance(record.get("_id"), ObjectId)


def _cleanup_gridfs_files(db, contents: dict) -> None:
    if contents.get("json_config_id"):
        try:
            delete_from_gridfs(db.fs, ObjectId(contents["json_config_id"]))
        except Exception as exc:
            logger.warning("cleanup.gridfs_failed file=json_config id=%s error=%s", contents["json_config_id"], exc)

    if contents.get("sql_file_id"):
        try:
            delete_from_gridfs(db.fs, ObjectId(contents["sql_file_id"]))
        except Exception as exc:
            logger.warning("cleanup.gridfs_failed file=sql_file id=%s error=%s", contents["sql_file_id"], exc)

    if contents.get("template_id"):
        try:
            delete_from_gridfs(db.fs, ObjectId(contents["template_id"]))
        except Exception as exc:
            logger.warning("cleanup.gridfs_failed file=template id=%s error=%s", contents["template_id"], exc)


def purge_old_versions(report_id: str, keep_versions: int = 3, dry_run: bool = False) -> dict:
    db = get_db()

    # Resolve the business composite key from the report_id
    anchor = db.metadata_collection.find_one({
        "report_id": report_id,
        "_id": {"$ne": "report_id_seq"},   # exclude counter sentinel doc
    })
    if not anchor:
        raise RecordNotFoundError(f"No records found with report_id '{report_id}'")

    composite_query = {
        "csi_id": anchor["csi_id"],
        "regulation": anchor["regulation"],
        "region": anchor["region"],
    }
    all_versions = list(db.metadata_collection.find(composite_query).sort("version", -1))

    active_ids = {r["_id"] for r in all_versions if r.get("active")}
    non_active = [r for r in all_versions if r["_id"] not in active_ids]
    protected = [r for r in all_versions if r["_id"] in active_ids]

    slots_remaining = max(0, keep_versions - len(protected))
    to_keep_inactive = non_active[:slots_remaining]
    to_purge = non_active[slots_remaining:]

    result: Dict[str, Any] = {
        "purged": 0,
        "kept": len(protected) + len(to_keep_inactive),
        "errors": [],
        "dry_run": dry_run,
    }

    if not to_purge:
        logger.info("cleanup.noop report_id=%s total=%d keep=%d", report_id, len(all_versions), keep_versions)
        return result

    if dry_run:
        result["purged"] = len(to_purge)
        logger.info("cleanup.dry_run report_id=%s would_purge=%d", report_id, len(to_purge))
        return result

    to_purge = [r for r in to_purge if _is_real_record(r)]
    for record in to_purge:
        version = record.get("version", "?")
        try:
            _cleanup_gridfs_files(db, record.get("file_contents", {}))
            db.metadata_collection.delete_one({"_id": record["_id"]})
            result["purged"] += 1
            logger.info("cleanup.purged report_id=%s version=%s", report_id, version)
        except Exception as exc:
            result["errors"].append(f"version {version}: {exc}")
            logger.error("cleanup.purge_failed report_id=%s version=%s error=%s", report_id, version, exc)

    logger.info("cleanup.complete report_id=%s purged=%d", report_id, result["purged"])
    return result


def purge_all_old_versions(keep_versions: int = 3, dry_run: bool = False) -> dict:
    db = get_db()

    # Collect all distinct logical records by composite key
    pipeline = [
        {"$match": {"_id": {"$ne": "report_id_seq"}}},   # exclude counter sentinel doc
        {"$group": {"_id": {"csi_id": "$csi_id", "regulation": "$regulation", "region": "$region"}}},
    ]
    composite_keys = [doc["_id"] for doc in db.metadata_collection.aggregate(pipeline)]

    aggregate: Dict[str, Any] = {
        "total_purged": 0,
        "records_processed": len(composite_keys),
        "errors": [],
        "dry_run": dry_run,
    }

    logger.info("cleanup.global_start records=%d keep=%d dry_run=%s", len(composite_keys), keep_versions, dry_run)

    for key in composite_keys:
        try:
            all_versions = list(
                db.metadata_collection.find(key).sort("version", -1)
            )
            active_ids = {r["_id"] for r in all_versions if r.get("active")}
            non_active = [r for r in all_versions if r["_id"] not in active_ids]
            protected = [r for r in all_versions if r["_id"] in active_ids]

            slots_remaining = max(0, keep_versions - len(protected))
            to_purge = non_active[slots_remaining:]

            if dry_run:
                aggregate["total_purged"] += len(to_purge)
                continue

            to_purge = [r for r in to_purge if _is_real_record(r)]
            for record in to_purge:
                version = record.get("version", "?")
                try:
                    _cleanup_gridfs_files(db, record.get("file_contents", {}))
                    db.metadata_collection.delete_one({"_id": record["_id"]})
                    aggregate["total_purged"] += 1
                    logger.info("cleanup.global_purged key=%s version=%s", key, version)
                except Exception as exc:
                    aggregate["errors"].append(f"key={key} version={version}: {exc}")
                    logger.error("cleanup.global_purge_failed key=%s version=%s error=%s", key, version, exc)

        except Exception as exc:
            aggregate["errors"].append(f"key={key}: {exc}")
            logger.error("cleanup.global_record_failed key=%s error=%s", key, exc)

    logger.info(
        "cleanup.global_complete processed=%d purged=%d",
        aggregate["records_processed"], aggregate["total_purged"],
    )
    return aggregate


def purge_by_age(max_age_days: int = 90, dry_run: bool = False) -> dict:
    db = get_db()
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

    old_records = list(db.metadata_collection.find({"active": False, "uploaded_at": {"$lt": cutoff}}))
    result: Dict[str, Any] = {"purged": 0, "errors": [], "dry_run": dry_run}

    if not old_records:
        logger.info("cleanup.age_noop max_age_days=%d", max_age_days)
        return result

    if dry_run:
        result["purged"] = len(old_records)
        logger.info("cleanup.age_dry_run max_age_days=%d would_purge=%d", max_age_days, len(old_records))
        return result

    old_records = [r for r in old_records if _is_real_record(r)]
    for record in old_records:
        try:
            _cleanup_gridfs_files(db, record.get("file_contents", {}))
            db.metadata_collection.delete_one({"_id": record["_id"]})
            result["purged"] += 1
        except Exception as exc:
            result["errors"].append(f"record {record.get('_id')}: {exc}")
            logger.error("cleanup.age_purge_failed record_id=%s error=%s", record.get("_id"), exc)

    logger.info("cleanup.age_complete max_age_days=%d purged=%d", max_age_days, result["purged"])
    return result
