"""Fetch service — query operations for metadata retrieval."""

import logging
from typing import Optional

from src.config.database import get_db
from src.errors.exceptions import RecordNotFoundError

logger = logging.getLogger(__name__)

DEFAULT_LIMIT = 500


def fetch_active_by_unique_id(unique_id: str) -> dict:
    db = get_db()
    record = db.metadata_collection.find_one({"unique_id": unique_id, "active": True})
    if not record:
        raise RecordNotFoundError(f"No active record found with unique_id '{unique_id}'")
    logger.info("fetch.by_unique_id unique_id=%s version=%s", unique_id, record.get("version"))
    return record


def fetch_by_csi_id(csi_id: str, active_only: bool = True, limit: int = DEFAULT_LIMIT) -> list[dict]:
    db = get_db()
    query = {"csi_id": csi_id}
    if active_only:
        query["active"] = True
    results = list(db.metadata_collection.find(query).limit(limit))
    logger.info("fetch.by_csi_id csi_id=%s active_only=%s count=%d", csi_id, active_only, len(results))
    return results


def fetch_by_region(region: str, active_only: bool = True, limit: int = DEFAULT_LIMIT) -> list[dict]:
    db = get_db()
    query = {"region": region}
    if active_only:
        query["active"] = True
    results = list(db.metadata_collection.find(query).limit(limit))
    logger.info("fetch.by_region region=%s active_only=%s count=%d", region, active_only, len(results))
    return results


def fetch_by_regulation(regulation: str, active_only: bool = True, limit: int = DEFAULT_LIMIT) -> list[dict]:
    db = get_db()
    query = {"regulation": regulation}
    if active_only:
        query["active"] = True
    results = list(db.metadata_collection.find(query).limit(limit))
    logger.info("fetch.by_regulation regulation=%s active_only=%s count=%d", regulation, active_only, len(results))
    return results


def fetch_by_composite(filters: dict, active_only: bool = True, limit: int = DEFAULT_LIMIT) -> list[dict]:
    db = get_db()
    query = {}
    for key in ["csi_id", "region", "regulation", "unique_id"]:
        if key in filters and filters[key]:
            query[key] = filters[key]
    if active_only:
        query["active"] = True
    results = list(db.metadata_collection.find(query).limit(limit))
    logger.info("fetch.composite filters=%s active_only=%s count=%d", filters, active_only, len(results))
    return results


def list_all_active(limit: int = DEFAULT_LIMIT) -> list[dict]:
    db = get_db()
    results = list(
        db.metadata_collection.find(
            {"active": True},
            {
                "unique_id": 1, "csi_id": 1, "region": 1, "regulation": 1,
                "name": 1, "out_file_name": 1, "version": 1, "uploaded_at": 1,
            },
        ).limit(limit)
    )
    logger.info("fetch.list_active count=%d", len(results))
    return results


def fetch_version_history(unique_id: str) -> list[dict]:
    db = get_db()
    records = list(db.metadata_collection.find({"unique_id": unique_id}).sort("version", 1))
    if not records:
        raise RecordNotFoundError(f"No records found with unique_id '{unique_id}'")
    logger.info("fetch.history unique_id=%s versions=%d", unique_id, len(records))
    return records
