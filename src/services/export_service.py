"""Export service — reconstruct bundle files from the metadata collection."""

import logging
from pathlib import Path
from typing import Optional, Union

from bson import ObjectId

from src.config.database import get_db
from src.errors.exceptions import ChecksumMismatchError, RecordNotFoundError
from src.services.gridfs_service import download_from_gridfs
from src.utils.checksum import compute_bytes_checksum

logger = logging.getLogger(__name__)

VALID_FILE_KEYS = {"json_config", "sql_file", "template"}


def export_bundle(
    report_id: str,
    output_dir: Union[str, Path],
    version: Optional[int] = None,
    verify_checksums: bool = True,
    force: bool = False,
    files: Optional[set[str]] = None,
) -> dict:
    """
    Export files from a stored bundle to disk.

    Args:
        report_id:        Internal UUID report_id (shown in list/history output).
        output_dir:       Directory to write exported files into.
        version:          Specific version to export (default: active).
        verify_checksums: Validate checksums after download.
        force:            Write files even on checksum mismatch.
        files:            Optional set of file keys to export.
                          Valid values: "json_config", "sql_file", "template".
                          If None or empty, all available files are exported.
    """
    db = get_db()
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    if version is not None:
        record = db.metadata_collection.find_one({"report_id": report_id, "version": version})
        if not record:
            raise RecordNotFoundError(f"No record found with report_id '{report_id}', version {version}")
    else:
        record = db.metadata_collection.find_one({"report_id": report_id, "active": True})
        if not record:
            raise RecordNotFoundError(f"No active record found with report_id '{report_id}'")

    result = {
        "report_id": report_id,
        "version": record.get("version", 1),
        "files": {},
        "checksum_verified": {},
        "output_dir": str(out_path),
    }

    # Normalise filter: None / empty-set → export all
    requested = files if files else VALID_FILE_KEYS

    contents = record.get("file_contents", {})
    original_files = record.get("original_files", {})
    checksums = record.get("checksums", {})
    mismatches = []

    # --- json_config ---
    if "json_config" in requested:
        json_id_str = contents.get("json_config_id")
        json_filename = original_files.get("json_config", "config.json")
        if json_id_str is not None:
            try:
                json_bytes, _ = download_from_gridfs(db.fs, ObjectId(json_id_str))
                json_path = out_path / json_filename
                json_path.write_bytes(json_bytes)
                result["files"]["json_config"] = str(json_path)

                if verify_checksums and checksums.get("json_config"):
                    actual = compute_bytes_checksum(json_bytes)
                    expected = checksums["json_config"]
                    matched = actual == expected
                    result["checksum_verified"]["json_config"] = matched
                    if not matched:
                        mismatches.append(("json_config", json_path, expected, actual))
            except Exception as exc:
                logger.error("export.json_config_failed report_id=%s error=%s", report_id, exc)
                result["files"]["json_config"] = f"ERROR: {exc}"
        else:
            logger.warning("export.json_config_missing report_id=%s", report_id)

    # --- sql_file ---
    if "sql_file" in requested:
        sql_id_str = contents.get("sql_file_id")
        sql_filename = original_files.get("sql_file", "query.sql")
        if sql_id_str is not None:
            try:
                sql_bytes, _ = download_from_gridfs(db.fs, ObjectId(sql_id_str))
                sql_path = out_path / sql_filename
                sql_path.write_bytes(sql_bytes)
                result["files"]["sql_file"] = str(sql_path)

                if verify_checksums and checksums.get("sql_file"):
                    actual = compute_bytes_checksum(sql_bytes)
                    expected = checksums["sql_file"]
                    matched = actual == expected
                    result["checksum_verified"]["sql_file"] = matched
                    if not matched:
                        mismatches.append(("sql_file", sql_path, expected, actual))
            except Exception as exc:
                logger.error("export.sql_failed report_id=%s error=%s", report_id, exc)
                result["files"]["sql_file"] = f"ERROR: {exc}"

    # --- template ---
    if "template" in requested:
        template_id_str = contents.get("template_id")
        template_filename = original_files.get("template")
        if template_id_str is not None and template_filename:
            try:
                template_bytes, _ = download_from_gridfs(db.fs, ObjectId(template_id_str))
                template_path = out_path / template_filename
                template_path.write_bytes(template_bytes)
                result["files"]["template"] = str(template_path)

                if verify_checksums and checksums.get("template"):
                    actual = compute_bytes_checksum(template_bytes)
                    expected = checksums["template"]
                    matched = actual == expected
                    result["checksum_verified"]["template"] = matched
                    if not matched:
                        mismatches.append(("template", template_path, expected, actual))
            except Exception as exc:
                logger.error("export.template_failed report_id=%s error=%s", report_id, exc)
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
        "export.complete report_id=%s version=%d output=%s files=%d checksums=%d/%d",
        report_id, result["version"], out_path, exported_count, verified_count, total_checks,
    )
    return result
