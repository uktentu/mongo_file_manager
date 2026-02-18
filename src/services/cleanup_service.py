"""Cleanup service — retention policy and version purging."""

import logging
from datetime import datetime, timezone, timedelta

from src.config.database import get_db
from src.errors.exceptions import RecordNotFoundError
from src.services.gridfs_service import delete_from_gridfs

logger = logging.getLogger(__name__)


def _delete_record_files(db, record: dict) -> tuple[int, list[str]]:
    """Delete GridFS files and config doc for a single record. Returns (freed_count, errors)."""
    freed = 0
    errors = []
    refs = record.get("file_references", {})

    sql_id = refs.get("sql_gridfs_id")
    if sql_id:
        try:
            delete_from_gridfs(db.sqlfiles_gridfs, sql_id)
            freed += 1
        except Exception as exc:
            errors.append(f"sql id={sql_id}: {exc}")
            logger.warning("cleanup.gridfs_delete_failed type=sql id=%s error=%s", sql_id, exc)

    template_id = refs.get("template_gridfs_id")
    if template_id:
        try:
            delete_from_gridfs(db.templates_gridfs, template_id)
            freed += 1
        except Exception as exc:
            errors.append(f"template id={template_id}: {exc}")
            logger.warning("cleanup.gridfs_delete_failed type=template id=%s error=%s", template_id, exc)

    config_id = refs.get("json_config_id")
    if config_id:
        try:
            db.configs_collection.delete_one({"_id": config_id})
        except Exception as exc:
            errors.append(f"config id={config_id}: {exc}")
            logger.warning("cleanup.config_delete_failed id=%s error=%s", config_id, exc)

    return freed, errors


def purge_old_versions(unique_id: str, keep_versions: int = 3, dry_run: bool = False) -> dict:
    db = get_db()

    all_versions = list(db.metadata_collection.find({"unique_id": unique_id}).sort("version", -1))
    if not all_versions:
        raise RecordNotFoundError(f"No records found with unique_id '{unique_id}'")

    active_ids = {r["_id"] for r in all_versions if r.get("active")}
    non_active = [r for r in all_versions if r["_id"] not in active_ids]
    protected = [r for r in all_versions if r["_id"] in active_ids]

    slots_remaining = max(0, keep_versions - len(protected))
    to_keep_inactive = non_active[:slots_remaining]
    to_purge = non_active[slots_remaining:]

    result = {
        "purged": 0,
        "kept": len(protected) + len(to_keep_inactive),
        "freed_gridfs": 0,
        "errors": [],
        "dry_run": dry_run,
    }

    if not to_purge:
        logger.info("cleanup.noop unique_id=%s total=%d keep=%d", unique_id, len(all_versions), keep_versions)
        return result

    if dry_run:
        result["purged"] = len(to_purge)
        logger.info("cleanup.dry_run unique_id=%s would_purge=%d", unique_id, len(to_purge))
        return result

    for record in to_purge:
        version = record.get("version", "?")
        try:
            freed, file_errors = _delete_record_files(db, record)
            result["freed_gridfs"] += freed
            result["errors"].extend(file_errors)

            db.metadata_collection.delete_one({"_id": record["_id"]})
            result["purged"] += 1
            logger.info("cleanup.purged unique_id=%s version=%s", unique_id, version)

        except Exception as exc:
            result["errors"].append(f"version {version}: {exc}")
            logger.error("cleanup.purge_failed unique_id=%s version=%s error=%s", unique_id, version, exc)

    logger.info("cleanup.complete unique_id=%s purged=%d freed_gridfs=%d", unique_id, result["purged"], result["freed_gridfs"])
    return result


def purge_all_old_versions(keep_versions: int = 3, dry_run: bool = False) -> dict:
    db = get_db()
    unique_ids = db.metadata_collection.distinct("unique_id")

    aggregate = {
        "total_purged": 0,
        "total_freed_gridfs": 0,
        "records_processed": len(unique_ids),
        "errors": [],
        "dry_run": dry_run,
    }

    logger.info("cleanup.global_start records=%d keep=%d dry_run=%s", len(unique_ids), keep_versions, dry_run)

    for uid in unique_ids:
        try:
            result = purge_old_versions(uid, keep_versions=keep_versions, dry_run=dry_run)
            aggregate["total_purged"] += result["purged"]
            aggregate["total_freed_gridfs"] += result["freed_gridfs"]
            aggregate["errors"].extend(result.get("errors", []))
        except Exception as exc:
            aggregate["errors"].append(f"{uid}: {exc}")
            logger.error("cleanup.global_record_failed unique_id=%s error=%s", uid, exc)

    logger.info(
        "cleanup.global_complete processed=%d purged=%d freed_gridfs=%d",
        aggregate["records_processed"], aggregate["total_purged"], aggregate["total_freed_gridfs"],
    )
    return aggregate


def purge_by_age(max_age_days: int = 90, dry_run: bool = False) -> dict:
    db = get_db()
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

    old_records = list(db.metadata_collection.find({"active": False, "uploaded_at": {"$lt": cutoff}}))
    result = {"purged": 0, "freed_gridfs": 0, "errors": [], "dry_run": dry_run}

    if not old_records:
        logger.info("cleanup.age_noop max_age_days=%d", max_age_days)
        return result

    if dry_run:
        result["purged"] = len(old_records)
        logger.info("cleanup.age_dry_run max_age_days=%d would_purge=%d", max_age_days, len(old_records))
        return result

    for record in old_records:
        try:
            freed, file_errors = _delete_record_files(db, record)
            result["freed_gridfs"] += freed
            result["errors"].extend(file_errors)

            db.metadata_collection.delete_one({"_id": record["_id"]})
            result["purged"] += 1

        except Exception as exc:
            result["errors"].append(f"record {record.get('_id')}: {exc}")
            logger.error("cleanup.age_purge_failed record_id=%s error=%s", record.get("_id"), exc)

    logger.info("cleanup.age_complete max_age_days=%d purged=%d freed_gridfs=%d", max_age_days, result["purged"], result["freed_gridfs"])
    return result
