"""Export service — reconstruct bundle files from MongoDB to disk."""

import json
import logging
from pathlib import Path
from typing import Optional

from src.config.database import get_db
from src.errors.exceptions import ChecksumMismatchError, RecordNotFoundError
from src.services.gridfs_service import download_from_gridfs
from src.utils.checksum import compute_bytes_checksum

logger = logging.getLogger(__name__)


def export_bundle(
    unique_id: str,
    output_dir: str | Path,
    version: Optional[int] = None,
    verify_checksums: bool = True,
    force: bool = False,
) -> dict:
    db = get_db()
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    if version is not None:
        record = db.metadata_collection.find_one({"unique_id": unique_id, "version": version})
        if not record:
            raise RecordNotFoundError(f"No record found with unique_id '{unique_id}', version {version}")
    else:
        record = db.metadata_collection.find_one({"unique_id": unique_id, "active": True})
        if not record:
            raise RecordNotFoundError(f"No active record found with unique_id '{unique_id}'")

    result = {
        "unique_id": unique_id,
        "version": record.get("version", 1),
        "files": {},
        "checksum_verified": {},
        "output_dir": str(out_path),
    }

    refs = record.get("file_references", {})
    original_files = record.get("original_files", {})
    checksums = record.get("checksums", {})
    mismatches = []

    json_config_id = refs.get("json_config_id")
    json_filename = original_files.get("json_config", "config.json")
    if json_config_id:
        config_doc = db.configs_collection.find_one({"_id": json_config_id})
        if config_doc and "config" in config_doc:
            json_path = out_path / json_filename
            config_content = json.dumps(config_doc["config"], indent=2)
            json_path.write_text(config_content, encoding="utf-8")
            result["files"]["json_config"] = str(json_path)

            if verify_checksums and checksums.get("json_config"):
                actual = compute_bytes_checksum(json_path.read_bytes())
                expected = checksums["json_config"]
                matched = actual == expected
                result["checksum_verified"]["json_config"] = matched
                if not matched:
                    mismatches.append(("json_config", json_path, expected, actual))
        else:
            logger.warning("export.config_not_found id=%s", json_config_id)

    sql_gridfs_id = refs.get("sql_gridfs_id")
    sql_filename = original_files.get("sql_file", "query.sql")
    if sql_gridfs_id:
        try:
            data, metadata = download_from_gridfs(db.sqlfiles_gridfs, sql_gridfs_id)
            sql_path = out_path / sql_filename
            sql_path.write_bytes(data)
            result["files"]["sql_file"] = str(sql_path)

            if verify_checksums and checksums.get("sql_file"):
                actual = compute_bytes_checksum(data)
                expected = checksums["sql_file"]
                matched = actual == expected
                result["checksum_verified"]["sql_file"] = matched
                if not matched:
                    mismatches.append(("sql_file", sql_path, expected, actual))
        except Exception as exc:
            logger.error("export.sql_failed id=%s error=%s", sql_gridfs_id, exc)
            result["files"]["sql_file"] = f"ERROR: {exc}"

    template_gridfs_id = refs.get("template_gridfs_id")
    template_filename = original_files.get("template")
    if template_gridfs_id and template_filename:
        try:
            data, metadata = download_from_gridfs(db.templates_gridfs, template_gridfs_id)
            template_path = out_path / template_filename
            template_path.write_bytes(data)
            result["files"]["template"] = str(template_path)

            if verify_checksums and checksums.get("template"):
                actual = compute_bytes_checksum(data)
                expected = checksums["template"]
                matched = actual == expected
                result["checksum_verified"]["template"] = matched
                if not matched:
                    mismatches.append(("template", template_path, expected, actual))
        except Exception as exc:
            logger.error("export.template_failed id=%s error=%s", template_gridfs_id, exc)
            result["files"]["template"] = f"ERROR: {exc}"

    if mismatches and not force:
        for file_type, file_path, expected, actual in mismatches:
            logger.error(
                "export.checksum_failed type=%s expected=%s actual=%s path=%s",
                file_type, expected, actual, file_path,
            )
            file_path.unlink(missing_ok=True)
            result["files"].pop(file_type, None)

        failed_types = [m[0] for m in mismatches]
        raise ChecksumMismatchError(
            f"Export aborted: checksum verification failed for {', '.join(failed_types)}. "
            f"Corrupted files have been removed. Use --force to export anyway."
        )

    if mismatches and force:
        for file_type, file_path, expected, actual in mismatches:
            logger.warning(
                "export.checksum_mismatch type=%s expected=%s actual=%s (forced)",
                file_type, expected, actual,
            )

    exported_count = sum(1 for v in result["files"].values() if not str(v).startswith("ERROR"))
    verified_count = sum(1 for v in result["checksum_verified"].values() if v is True)
    total_checks = len(result["checksum_verified"])

    logger.info(
        "export.complete unique_id=%s version=%d output=%s files=%d checksums=%d/%d",
        unique_id, result["version"], out_path, exported_count, verified_count, total_checks,
    )
    return result
