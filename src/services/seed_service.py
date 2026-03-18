"""
Seed service — bulk seeding, single creation, and append-only modification.

Flow for seed_from_manifest:
  Step 1: Load & validate manifest structure
  Step 2: Pre-validate ALL bundles (collect errors before touching DB)
  Step 3: For each valid bundle:
    a. Compute checksums
    b. Resolve existing record by composite key (csi_id + region + regulation + json_config filename)
    c. CREATE new record  — if no existing active record
    d. SKIP              — if all checksums match
    e. MODIFY            — if checksums changed
  Step 4: Return structured result with per-bundle details + summary
"""

import logging
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml

from src.config.database import get_db
from src.errors.exceptions import (
    DatabaseError,
    DuplicateRecordError,
    RecordNotFoundError,
    ValidationError,
)
from src.models.schemas import (
    AuditEntry,
    Checksums,
    FileContents,
    FileSizes,
    MetadataDocument,
    OriginalFiles,
)
from src.services.audit_service import create_audit_entry
from src.services.gridfs_service import GridFSOrphanTracker, upload_to_gridfs
from src.utils.checksum import compute_file_checksum
from src.utils.report_id import generate_report_id
from src.utils.validator import (
    validate_json_config,
    validate_manifest_structure,
    validate_seed_bundle,
)

logger = logging.getLogger(__name__)


def _detect_content_type(file_path: str) -> str:
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type or "application/octet-stream"


# ---------------------------------------------------------------------------
# Public: bulk seeding from manifest
# ---------------------------------------------------------------------------

def seed_from_manifest(manifest_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load a YAML manifest and seed all bundles.

    Returned dict:
    {
      "created": int, "updated": int, "skipped": int, "failed": int,
      "total": int,
      "details": [
        {
          "index": int, "label": str, "status": str,
          "report_id": Optional[str], "version": Optional[int],
          "reason": str, "error": Optional[str]
        }, ...
      ],
      "errors": [str, ...]          # compact error list for quick display
    }
    """
    path = Path(manifest_path)
    logger.info("═" * 60)
    logger.info("seed.start  manifest=%s", path)

    # ── Step 1: Load YAML ────────────────────────────────────────
    if not path.exists():
        raise ValidationError(f"Manifest file not found: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        raise ValidationError(f"Failed to parse YAML manifest: {exc}") from exc

    # ── Step 2: Validate manifest structure ──────────────────────
    logger.info("seed.step1  Validating manifest structure")
    raw_bundles = validate_manifest_structure(raw, source=str(path))
    logger.info("seed.step1  OK — %d bundle(s) found", len(raw_bundles))

    base_dir = path.parent

    # ── Step 3: Pre-validate ALL bundle fields & files ───────────
    logger.info("seed.step2  Pre-validating all bundles before database operations")
    validated_bundles: List[Tuple[int, dict, dict]] = []   # (original_index, resolved, config)
    pre_errors: List[str] = []

    for i, raw_bundle in enumerate(raw_bundles):
        label = raw_bundle.get("csi_id", f"bundle-{i}") if isinstance(raw_bundle, dict) else f"bundle-{i}"
        try:
            resolved = validate_seed_bundle(raw_bundle, base_dir, index=i)
            # Parse JSON config once — result is passed downstream to avoid re-reading
            parsed_config = validate_json_config(resolved["json_config"], index=i)
            validated_bundles.append((i, resolved, parsed_config))
            logger.info(
                "seed.step2  [%d/%d] %s — fields/files OK",
                i + 1, len(raw_bundles), label,
            )
        except Exception as exc:
            msg = f"Bundle #{i} '{label}': {exc}"
            pre_errors.append(msg)
            logger.error("seed.step2  [%d/%d] %s — VALIDATION FAILED: %s", i + 1, len(raw_bundles), label, exc)

    if pre_errors:
        logger.warning(
            "seed.step2  %d/%d bundle(s) failed pre-validation — they will be skipped",
            len(pre_errors), len(raw_bundles),
        )

    # ── Step 4: Process each validated bundle against DB ─────────
    logger.info("seed.step3  Processing %d validated bundle(s)", len(validated_bundles))

    results: Dict[str, Any] = {
        "created": 0, "updated": 0, "skipped": 0, "failed": 0,
        "total": len(raw_bundles),
        "details": [],
        "errors": list(pre_errors),
    }

    # Mark pre-validation failures in details
    for i, raw_bundle in enumerate(raw_bundles):
        if not any(idx == i for idx, _, _ in validated_bundles):
            label = raw_bundle.get("csi_id", f"bundle-{i}") if isinstance(raw_bundle, dict) else f"bundle-{i}"
            matching_err = next((e for e in pre_errors if f"Bundle #{i}" in e), "Pre-validation failed")
            results["details"].append({
                "index": i, "label": label, "status": "failed",
                "report_id": None, "version": None,
                "reason": "Pre-validation failed",
                "error": matching_err,
            })
            results["failed"] += 1

    for idx, (i, bundle, config) in enumerate(validated_bundles):
        label = bundle.get("csi_id", f"bundle-{i}")
        logger.info("seed.step3  ── Bundle [%d/%d] '%s' ──", idx + 1, len(validated_bundles), label)

        detail: Dict[str, Any] = {
            "index": i, "label": label, "status": "failed",
            "report_id": None, "version": None,
            "reason": "", "error": None,
        }

        try:
            status, report_id, version, reason = _process_bundle(bundle, config)
            detail["status"] = status
            detail["report_id"] = report_id
            detail["version"] = version
            detail["reason"] = reason
            results[status] = results.get(status, 0) + 1
            logger.info(
                "seed.step3  '%s' → %s  report_id=%s version=%s reason=%s",
                label, status.upper(), report_id, version, reason,
            )
        except Exception as exc:
            detail["status"] = "failed"
            detail["error"] = str(exc)
            detail["reason"] = "Database/processing error"
            results["failed"] += 1
            results["errors"].append(f"Bundle '{label}': {exc}")
            logger.error("seed.step3  '%s' → FAILED: %s", label, exc)

        results["details"].append(detail)

    # ── Step 5: Final summary ─────────────────────────────────────
    logger.info("═" * 60)
    logger.info(
        "seed.done   total=%d  created=%d  updated=%d  skipped=%d  failed=%d",
        results["total"], results["created"], results["updated"],
        results["skipped"], results["failed"],
    )
    if results["errors"]:
        for err in results["errors"]:
            logger.error("seed.error  %s", err)
    logger.info("═" * 60)

    return results


# ---------------------------------------------------------------------------
# Internal: single bundle dispatcher
# ---------------------------------------------------------------------------

def _process_bundle(bundle: dict, config: dict) -> Tuple[str, Optional[str], Optional[int], str]:
    """
    Determine whether to create/skip/modify a bundle.
    Returns (status, report_id, version, reason).

    Routing rules (no user-supplied report_id):
      Composite key: (csi_id, region, regulation, original_files.json_config filename)
      - Active record found with same key → MODIFY
          • All checksums unchanged → SKIP (idempotent re-run)
          • Any checksum changed    → MODIFY (includes json_config if its content changed)
      - No active record found         → CREATE (UUID report_id assigned internally)
    """
    db = get_db()

    json_config_filename = Path(bundle["json_config"]).name

    logger.debug("seed.checksum  Computing checksums for '%s'", bundle["csi_id"])
    json_checksum = compute_file_checksum(bundle["json_config"])
    sql_checksum = compute_file_checksum(bundle["sql_file"])
    template_checksum = (
        compute_file_checksum(bundle["template"]) if bundle.get("template") else None
    )
    logger.debug(
        "seed.checksum  json=%s sql=%s template=%s",
        json_checksum[:12] + "…", sql_checksum[:12] + "…",
        (template_checksum[:12] + "…") if template_checksum else "N/A",
    )

    precomputed = {
        "json_config": json_checksum,
        "sql_file": sql_checksum,
        "template": template_checksum,
    }

    # ── Composite key lookup ─────────────────────────────────────
    logger.debug(
        "seed.lookup  csi_id=%s regulation=%s region=%s json_config=%s",
        bundle["csi_id"], bundle["regulation"], bundle["region"], json_config_filename,
    )
    existing = db.metadata_collection.find_one({
        "csi_id": bundle["csi_id"],
        "regulation": bundle["regulation"],
        "region": bundle["region"],
        "original_files.json_config": json_config_filename,
        "active": True,
    })

    if not existing:
        # ── CREATE ───────────────────────────────────────────────
        logger.debug(
            "seed.create  No existing record — creating for csi_id=%s regulation=%s region=%s",
            bundle["csi_id"], bundle["regulation"], bundle["region"],
        )
        report_id = _create_record(bundle, config, precomputed_checksums=precomputed)
        return "created", report_id, 1, "new record"

    # ── Existing record found — check if anything changed ────────
    internal_report_id = existing["report_id"]
    existing_checksums = existing.get("checksums", {})
    checksums_match = (
        existing_checksums.get("json_config") == json_checksum
        and existing_checksums.get("sql_file") == sql_checksum
        and existing_checksums.get("template") == template_checksum
    )
    if checksums_match:
        logger.debug(
            "seed.skip  report_id=%s — all checksums match, nothing to do", internal_report_id
        )
        return "skipped", internal_report_id, existing.get("version"), "checksums unchanged"

    # Identify which checksums actually changed (for meaningful log/reason)
    changed = [
        k for k in ("json_config", "sql_file", "template")
        if existing_checksums.get(k) != precomputed.get(k)
    ]
    logger.debug(
        "seed.modify  report_id=%s v%d — changed files: %s",
        internal_report_id, existing.get("version", 1), changed,
    )
    new_version = _modify_record(
        internal_report_id, bundle, config, existing,
        precomputed_checksums=precomputed,
    )
    return "updated", internal_report_id, new_version, f"changed: {', '.join(changed)}"


# ---------------------------------------------------------------------------
# Public: single create
# ---------------------------------------------------------------------------

def create_single_record(
    csi_id: str,
    region: str,
    regulation: str,
    json_config_path: str,
    sql_file_path: str,
    template_path: Optional[str] = None,
) -> str:
    """Create one record manually. Returns the generated report_id."""
    logger.info("seed.create_single  csi_id=%s regulation=%s region=%s", csi_id, regulation, region)

    # Validate files
    config = validate_json_config(json_config_path)

    json_config_filename = Path(json_config_path).name
    db = get_db()
    existing = db.metadata_collection.find_one({
        "csi_id": csi_id,
        "regulation": regulation,
        "region": region,
        "original_files.json_config": json_config_filename,
        "active": True,
    })
    if existing:
        raise DuplicateRecordError(
            f"An active record already exists for csi_id='{csi_id}' regulation='{regulation}' "
            f"region='{region}' json_config='{json_config_filename}' "
            f"(internal report_id={existing.get('report_id')}). "
            "Use 'modify' to update it."
        )

    bundle = {
        "csi_id": csi_id, "region": region, "regulation": regulation,
        "json_config": json_config_path, "sql_file": sql_file_path,
        "template": template_path,
    }
    report_id = _create_record(bundle, config)
    logger.info("seed.create_single  DONE report_id=%s", report_id)
    return report_id


# ---------------------------------------------------------------------------
# Public: modify by composite key (user-facing — no report_id needed)
# ---------------------------------------------------------------------------

def modify_record_by_composite_key(
    csi_id: str,
    region: str,
    regulation: str,
    json_config_path: str,
    sql_file_path: Optional[str] = None,
    template_path: Optional[str] = None,
) -> int:
    """
    Modify an existing active record identified by composite key
    (csi_id, region, regulation, json_config filename).

    json_config_path is always required — its filename is the lookup key.
    If its content has changed (different checksum), it will be updated too.
    sql_file_path and template_path are optional additional files to update.

    Returns the new version number.
    """
    json_config_filename = Path(json_config_path).name
    logger.info(
        "seed.modify_composite  csi_id=%s regulation=%s region=%s json_config=%s",
        csi_id, regulation, region, json_config_filename,
    )

    db = get_db()
    existing = db.metadata_collection.find_one({
        "csi_id": csi_id,
        "regulation": regulation,
        "region": region,
        "original_files.json_config": json_config_filename,
        "active": True,
    })
    if not existing:
        raise RecordNotFoundError(
            f"No active record found for csi_id='{csi_id}' regulation='{regulation}' "
            f"region='{region}' json_config='{json_config_filename}'"
        )

    report_id = existing["report_id"]
    logger.info("seed.modify_composite  found report_id=%s v%d", report_id, existing.get("version", 1))
    return modify_record_by_id(
        report_id=report_id,
        json_config_path=json_config_path,
        sql_file_path=sql_file_path,
        template_path=template_path,
    )


# ---------------------------------------------------------------------------
# Public: modify by internal report_id (API use — UUID)
# ---------------------------------------------------------------------------

def modify_record_by_id(
    report_id: str,
    json_config_path: Optional[str] = None,
    sql_file_path: Optional[str] = None,
    template_path: Optional[str] = None,
) -> int:
    """Modify an existing record identified by report_id. Returns new version number."""
    logger.info("seed.modify_by_id  report_id=%s", report_id)

    if not any([json_config_path, sql_file_path, template_path]):
        raise ValidationError("At least one file must be provided for modification.")

    db = get_db()
    existing = db.metadata_collection.find_one({"report_id": report_id, "active": True})
    if not existing:
        raise RecordNotFoundError(f"No active record found with report_id '{report_id}'")

    logger.info(
        "seed.modify_by_id  found v%d csi_id=%s regulation=%s region=%s",
        existing.get("version", 1), existing["csi_id"], existing["regulation"], existing["region"],
    )

    config = validate_json_config(json_config_path) if json_config_path else None

    old_version = existing.get("version", 1)
    new_version = old_version + 1
    tracker = GridFSOrphanTracker()

    try:
        # Resolve per-file data
        if json_config_path:
            jc_path = Path(json_config_path)
            new_json_checksum = compute_file_checksum(jc_path)
            logger.debug("seed.modify_by_id  uploading json_config")
            new_json_id = str(upload_to_gridfs(
                bucket=db.fs, file_path=jc_path,
                original_filename=jc_path.name, content_type="application/json",
                orphan_tracker=tracker, precomputed_checksum=new_json_checksum,
            ))
            new_json_size = jc_path.stat().st_size
            new_json_original = jc_path.name
        else:
            new_json_checksum = existing["checksums"]["json_config"]
            new_json_id = existing["file_contents"]["json_config_id"]
            new_json_size = existing["file_sizes"]["json_config"]
            new_json_original = existing["original_files"]["json_config"]

        if sql_file_path:
            sq_path = Path(sql_file_path)
            new_sql_checksum = compute_file_checksum(sq_path)
            logger.debug("seed.modify_by_id  uploading sql_file")
            new_sql_id = str(upload_to_gridfs(
                bucket=db.fs, file_path=sq_path,
                original_filename=sq_path.name,
                content_type=_detect_content_type(str(sq_path)),
                orphan_tracker=tracker, precomputed_checksum=new_sql_checksum,
            ))
            new_sql_size = sq_path.stat().st_size
            new_sql_original = sq_path.name
        else:
            new_sql_checksum = existing["checksums"]["sql_file"]
            new_sql_id = existing["file_contents"]["sql_file_id"]
            new_sql_size = existing["file_sizes"]["sql_file"]
            new_sql_original = existing["original_files"]["sql_file"]

        if template_path:
            tp_path = Path(template_path)
            new_template_checksum = compute_file_checksum(tp_path)
            logger.debug("seed.modify_by_id  uploading template")
            new_template_id = str(upload_to_gridfs(
                bucket=db.fs, file_path=tp_path,
                original_filename=tp_path.name,
                content_type=_detect_content_type(str(tp_path)),
                orphan_tracker=tracker, precomputed_checksum=new_template_checksum,
            ))
            new_template_size = tp_path.stat().st_size
            new_template_original = tp_path.name
        else:
            new_template_id = existing["file_contents"].get("template_id")
            new_template_checksum = existing["checksums"].get("template")
            new_template_size = existing["file_sizes"].get("template")
            new_template_original = existing["original_files"].get("template")

        changed_parts = [
            p for p, v in [("json_config", json_config_path), ("sql_file", sql_file_path), ("template", template_path)]
            if v
        ]

        def _do_modify(session=None):
            db.metadata_collection.update_one(
                {"report_id": report_id, "active": True},
                {
                    "$set": {"active": False},
                    "$push": {"audit_log": create_audit_entry(
                        "DEACTIVATED", f"Superseded by version {new_version}"
                    )},
                },
                session=session,
            )
            metadata = MetadataDocument(
                report_id=report_id,
                csi_id=existing["csi_id"], region=existing["region"],
                regulation=existing["regulation"],
                name=config["report"]["name"] if config else existing["name"],
                original_files=OriginalFiles(
                    json_config=new_json_original,
                    template=new_template_original,
                    sql_file=new_sql_original,
                ),
                file_contents=FileContents(
                    json_config_id=new_json_id, sql_file_id=new_sql_id,
                    template_id=new_template_id,
                ),
                checksums=Checksums(
                    json_config=new_json_checksum,
                    template=new_template_checksum, sql_file=new_sql_checksum,
                ),
                file_sizes=FileSizes(
                    json_config=new_json_size,
                    template=new_template_size, sql_file=new_sql_size,
                ),
                uploaded_at=datetime.now(timezone.utc),
                active=True, version=new_version,
                audit_log=[AuditEntry(**create_audit_entry(
                    "MODIFIED",
                    f"Updated {', '.join(changed_parts)} (v{old_version} → v{new_version})",
                ))],
            )
            db.metadata_collection.insert_one(metadata.to_mongo_dict(), session=session)

        _run_with_transaction(db, _do_modify, context=f"modify report_id={report_id}")
        tracker.clear()
        logger.info(
            "seed.modify_by_id  DONE report_id=%s version=%d→%d changed=%s",
            report_id, old_version, new_version, changed_parts,
        )
        return new_version

    except Exception as exc:
        tracker.cleanup()
        raise DatabaseError(f"Modify failed for report_id='{report_id}': {exc}") from exc


# ---------------------------------------------------------------------------
# Internal: create
# ---------------------------------------------------------------------------

def _create_record(bundle: dict, config: dict, precomputed_checksums: Optional[dict] = None) -> str:
    db = get_db()
    tracker = GridFSOrphanTracker()

    json_config_path = Path(bundle["json_config"])
    sql_file_path = Path(bundle["sql_file"])
    template_path = Path(bundle["template"]) if bundle.get("template") else None

    checksums = precomputed_checksums or {}
    json_checksum = checksums.get("json_config") or compute_file_checksum(json_config_path)
    sql_checksum = checksums.get("sql_file") or compute_file_checksum(sql_file_path)
    template_checksum = checksums.get("template") or (
        compute_file_checksum(template_path) if template_path else None
    )

    try:
        report_id = generate_report_id(db)
        logger.debug("seed.create  report_id=%s — uploading files to GridFS", report_id)

        json_id = upload_to_gridfs(
            bucket=db.fs, file_path=json_config_path,
            original_filename=json_config_path.name, content_type="application/json",
            orphan_tracker=tracker, precomputed_checksum=json_checksum,
        )
        logger.debug("seed.create  report_id=%s — json_config uploaded id=%s", report_id, json_id)

        sql_id = upload_to_gridfs(
            bucket=db.fs, file_path=sql_file_path,
            original_filename=sql_file_path.name,
            content_type=_detect_content_type(str(sql_file_path)),
            orphan_tracker=tracker, precomputed_checksum=sql_checksum,
        )
        logger.debug("seed.create  report_id=%s — sql_file uploaded id=%s", report_id, sql_id)

        template_id = None
        if template_path:
            template_id = upload_to_gridfs(
                bucket=db.fs, file_path=template_path,
                original_filename=template_path.name,
                content_type=_detect_content_type(str(template_path)),
                orphan_tracker=tracker, precomputed_checksum=template_checksum,
            )
            logger.debug("seed.create  report_id=%s — template uploaded id=%s", report_id, template_id)

        metadata = MetadataDocument(
            report_id=report_id,
            csi_id=bundle["csi_id"], region=bundle["region"],
            regulation=bundle["regulation"],
            name=config["report"]["name"],
            original_files=OriginalFiles(
                json_config=json_config_path.name,
                template=template_path.name if template_path else None,
                sql_file=sql_file_path.name,
            ),
            file_contents=FileContents(
                json_config_id=str(json_id), sql_file_id=str(sql_id),
                template_id=str(template_id) if template_id else None,
            ),
            checksums=Checksums(
                json_config=json_checksum, template=template_checksum, sql_file=sql_checksum,
            ),
            file_sizes=FileSizes(
                json_config=json_config_path.stat().st_size,
                template=template_path.stat().st_size if template_path else None,
                sql_file=sql_file_path.stat().st_size,
            ),
            uploaded_at=datetime.now(timezone.utc),
            active=True, version=1,
            audit_log=[AuditEntry(**create_audit_entry("CREATED", "Initial seed from manifest"))],
        )

        def _do_create(session=None):
            db.metadata_collection.insert_one(metadata.to_mongo_dict(), session=session)

        _run_with_transaction(db, _do_create, context=f"create csi_id={bundle.get('csi_id')}")
        tracker.clear()
        logger.info(
            "seed.create  DONE report_id=%s csi_id=%s regulation=%s region=%s v1",
            report_id, bundle["csi_id"], bundle["regulation"], bundle["region"],
        )
        return report_id

    except Exception as exc:
        tracker.cleanup()
        raise DatabaseError(f"Failed to create record for csi_id='{bundle.get('csi_id')}': {exc}") from exc


# ---------------------------------------------------------------------------
# Internal: modify
# ---------------------------------------------------------------------------

def _modify_record(
    report_id: str,
    bundle: dict,
    config: dict,
    existing: dict,
    precomputed_checksums: Optional[dict] = None,
) -> int:
    db = get_db()
    tracker = GridFSOrphanTracker()
    old_version = existing.get("version", 1)
    new_version = old_version + 1

    checksums = precomputed_checksums or {}
    json_config_path = Path(bundle["json_config"])
    sql_file_path = Path(bundle["sql_file"])
    template_path = Path(bundle["template"]) if bundle.get("template") else None

    json_checksum = checksums.get("json_config") or compute_file_checksum(json_config_path)
    sql_checksum = checksums.get("sql_file") or compute_file_checksum(sql_file_path)
    template_checksum = checksums.get("template") or (
        compute_file_checksum(template_path) if template_path else None
    )

    existing_checksums = existing.get("checksums", {})
    existing_contents = existing.get("file_contents", {})
    existing_sizes = existing.get("file_sizes", {})
    existing_originals = existing.get("original_files", {})

    changed_parts: List[str] = []

    try:
        # ── json_config ─────────────────────────────────────────────
        if json_checksum != existing_checksums.get("json_config"):
            logger.debug("seed.modify  report_id=%s — json_config changed, uploading", report_id)
            json_id = str(upload_to_gridfs(
                bucket=db.fs, file_path=json_config_path,
                original_filename=json_config_path.name, content_type="application/json",
                orphan_tracker=tracker, precomputed_checksum=json_checksum,
            ))
            json_size = json_config_path.stat().st_size
            json_original = json_config_path.name
            changed_parts.append("json_config")
        else:
            logger.debug("seed.modify  report_id=%s — json_config unchanged, reusing", report_id)
            json_id = existing_contents.get("json_config_id")
            if not json_id:
                raise DatabaseError(
                    f"Corrupt record report_id='{report_id}': missing json_config_id in file_contents"
                )
            json_size = existing_sizes.get("json_config")
            json_original = existing_originals.get("json_config")

        # ── sql_file ────────────────────────────────────────────────
        if sql_checksum != existing_checksums.get("sql_file"):
            logger.debug("seed.modify  report_id=%s — sql_file changed, uploading", report_id)
            sql_id = str(upload_to_gridfs(
                bucket=db.fs, file_path=sql_file_path,
                original_filename=sql_file_path.name,
                content_type=_detect_content_type(str(sql_file_path)),
                orphan_tracker=tracker, precomputed_checksum=sql_checksum,
            ))
            sql_size = sql_file_path.stat().st_size
            sql_original = sql_file_path.name
            changed_parts.append("sql_file")
        else:
            logger.debug("seed.modify  report_id=%s — sql_file unchanged, reusing", report_id)
            sql_id = existing_contents.get("sql_file_id")
            if not sql_id:
                raise DatabaseError(
                    f"Corrupt record report_id='{report_id}': missing sql_file_id in file_contents"
                )
            sql_size = existing_sizes.get("sql_file")
            sql_original = existing_originals.get("sql_file")

        # ── template ────────────────────────────────────────────────
        if template_path and template_checksum != existing_checksums.get("template"):
            logger.debug("seed.modify  report_id=%s — template changed, uploading", report_id)
            template_id = str(upload_to_gridfs(
                bucket=db.fs, file_path=template_path,
                original_filename=template_path.name,
                content_type=_detect_content_type(str(template_path)),
                orphan_tracker=tracker, precomputed_checksum=template_checksum,
            ))
            template_size = template_path.stat().st_size
            template_original = template_path.name
            changed_parts.append("template")
        else:
            # Carry over existing template refs (may be None if never had one)
            logger.debug("seed.modify  report_id=%s — template unchanged/absent, reusing", report_id)
            template_id = existing_contents.get("template_id")
            template_size = existing_sizes.get("template")
            template_original = existing_originals.get("template")
            if template_path:
                # template provided but checksum matched
                pass  # template provided but checksum matched — existing refs already set above
            else:
                # No template in this bundle run — preserve existing
                template_checksum = existing_checksums.get("template")

        if not changed_parts:
            # Shouldn't normally happen since _process_bundle checks checksums first,
            # but guard here defensively.
            logger.info("seed.modify  report_id=%s — no actual changes detected (all checksums match)", report_id)
            tracker.clear()
            return old_version

        def _do_modify(session=None):
            db.metadata_collection.update_one(
                {"report_id": report_id, "active": True},
                {
                    "$set": {"active": False},
                    "$push": {"audit_log": create_audit_entry(
                        "DEACTIVATED", f"Superseded by version {new_version}"
                    )},
                },
                session=session,
            )
            metadata = MetadataDocument(
                report_id=report_id,
                csi_id=bundle["csi_id"], region=bundle["region"],
                regulation=bundle["regulation"],
                name=config["report"]["name"],
                original_files=OriginalFiles(
                    json_config=json_original,
                    template=template_original,
                    sql_file=sql_original,
                ),
                file_contents=FileContents(
                    json_config_id=json_id, sql_file_id=sql_id,
                    template_id=template_id,
                ),
                checksums=Checksums(
                    json_config=json_checksum,
                    template=template_checksum,
                    sql_file=sql_checksum,
                ),
                file_sizes=FileSizes(
                    json_config=json_size,
                    template=template_size,
                    sql_file=sql_size,
                ),
                uploaded_at=datetime.now(timezone.utc),
                active=True, version=new_version,
                audit_log=[AuditEntry(**create_audit_entry(
                    "MODIFIED",
                    f"Changed: {', '.join(changed_parts)} (v{old_version} → v{new_version})",
                ))],
            )
            db.metadata_collection.insert_one(metadata.to_mongo_dict(), session=session)

        _run_with_transaction(db, _do_modify, context=f"modify report_id={report_id}")
        tracker.clear()
        logger.info(
            "seed.modify  DONE report_id=%s version=%d→%d changed=%s",
            report_id, old_version, new_version, changed_parts,
        )
        return new_version

    except Exception as exc:
        tracker.cleanup()
        raise DatabaseError(
            f"Modify failed for report_id='{report_id}': {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Helper: transaction wrapper
# ---------------------------------------------------------------------------

def _run_with_transaction(db, operation, context: str = "") -> None:
    """Run `operation(session=...)` inside a transaction if supported, else bare."""
    if db.supports_transactions:
        with db.start_session() as session:
            session.start_transaction()
            try:
                operation(session=session)
                session.commit_transaction()
                logger.debug("seed.tx  committed context=%s", context)
            except Exception:
                session.abort_transaction()
                logger.error("seed.tx  aborted context=%s", context)
                raise
    else:
        logger.warning("seed.tx  no_transaction context=%s (standalone MongoDB)", context)
        operation(session=None)
