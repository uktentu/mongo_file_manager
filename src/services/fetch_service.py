"""Fetch service — query operations for metadata retrieval."""

import logging
from typing import Any, Dict, List

from src.config.database import get_db
from src.errors.exceptions import RecordNotFoundError

logger = logging.getLogger(__name__)

DEFAULT_LIMIT = 500


def fetch_active_by_report_id(report_id: str) -> dict:
    db = get_db()
    record = db.metadata_collection.find_one({"report_id": report_id, "active": True})
    if not record:
        raise RecordNotFoundError(f"No active record found with report_id '{report_id}'")
    logger.info("fetch.by_report_id report_id=%s version=%s", report_id, record.get("version"))
    return record


def fetch_by_csi_id(csi_id: str, active_only: bool = True, limit: int = DEFAULT_LIMIT) -> List[dict]:
    db = get_db()
    query: Dict[str, Any] = {"csi_id": csi_id}
    if active_only:
        query["active"] = True
    results = list(db.metadata_collection.find(query).limit(limit))
    logger.info("fetch.by_csi_id csi_id=%s active_only=%s count=%d", csi_id, active_only, len(results))
    return results


def fetch_by_region(region: str, active_only: bool = True, limit: int = DEFAULT_LIMIT) -> List[dict]:
    db = get_db()
    query: Dict[str, Any] = {"region": region}
    if active_only:
        query["active"] = True
    results = list(db.metadata_collection.find(query).limit(limit))
    logger.info("fetch.by_region region=%s active_only=%s count=%d", region, active_only, len(results))
    return results


def fetch_by_regulation(regulation: str, active_only: bool = True, limit: int = DEFAULT_LIMIT) -> List[dict]:
    db = get_db()
    query: Dict[str, Any] = {"regulation": regulation}
    if active_only:
        query["active"] = True
    results = list(db.metadata_collection.find(query).limit(limit))
    logger.info("fetch.by_regulation regulation=%s active_only=%s count=%d", regulation, active_only, len(results))
    return results


def fetch_by_composite(filters: dict, active_only: bool = True, limit: int = DEFAULT_LIMIT) -> List[dict]:
    db = get_db()
    query = {}
    for key in ["csi_id", "region", "regulation", "report_id"]:
        if key in filters and filters[key]:
            query[key] = filters[key]
    if active_only:
        query["active"] = True
    results = list(db.metadata_collection.find(query).limit(limit))
    logger.info("fetch.composite filters=%s active_only=%s count=%d", filters, active_only, len(results))
    return results


def list_all_active(limit: int = DEFAULT_LIMIT) -> List[dict]:
    db = get_db()
    results = list(
        db.metadata_collection.find(
            {"active": True},
            {
                "report_id": 1, "csi_id": 1, "region": 1, "regulation": 1,
                "name": 1, "version": 1, "uploaded_at": 1,
            },
        ).limit(limit)
    )
    logger.info("fetch.list_active count=%d", len(results))
    return results


def fetch_version_history(report_id: str) -> List[dict]:
    """Return all versions (active + inactive) for the logical record identified by report_id."""
    db = get_db()
    # Anchor on the record with this report_id to get the composite business key
    anchor = db.metadata_collection.find_one({
        "report_id": report_id,
        "_id": {"$ne": "report_id_seq"},   # exclude counter sentinel doc
    })
    if not anchor:
        raise RecordNotFoundError(f"No records found with report_id '{report_id}'")

    records = list(
        db.metadata_collection.find({
            "csi_id": anchor["csi_id"],
            "regulation": anchor["regulation"],
            "region": anchor["region"],
        }).sort("version", 1)
    )
    logger.info("fetch.history report_id=%s versions=%d", report_id, len(records))
    return records
