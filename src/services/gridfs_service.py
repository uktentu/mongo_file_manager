"""GridFS upload, download, delete operations with orphan tracking."""

import logging
from pathlib import Path
from typing import Optional

from bson import ObjectId
from gridfs import GridFS

from src.errors.exceptions import GridFSError
from src.utils.checksum import compute_file_checksum
from src.utils.retry import retry_on_failure

logger = logging.getLogger(__name__)


class GridFSOrphanTracker:
    """Tracks GridFS uploads and config inserts for cleanup if the parent operation fails."""

    def __init__(self):
        self._pending_gridfs: list[tuple[GridFS, ObjectId]] = []
        self._pending_configs: list[tuple] = []

    def track(self, bucket: GridFS, gridfs_id: ObjectId) -> None:
        self._pending_gridfs.append((bucket, gridfs_id))

    def track_config(self, collection, config_id: ObjectId) -> None:
        self._pending_configs.append((collection, config_id))

    def cleanup(self) -> int:
        cleaned = 0
        for bucket, gridfs_id in self._pending_gridfs:
            try:
                bucket.delete(gridfs_id)
                logger.info("gridfs.orphan_cleaned id=%s", gridfs_id)
                cleaned += 1
            except Exception as exc:
                logger.error("gridfs.orphan_cleanup_failed id=%s error=%s", gridfs_id, exc)
        for collection, config_id in self._pending_configs:
            try:
                collection.delete_one({"_id": config_id})
                logger.info("gridfs.orphan_config_cleaned id=%s", config_id)
                cleaned += 1
            except Exception as exc:
                logger.error("gridfs.orphan_config_cleanup_failed id=%s error=%s", config_id, exc)
        self._pending_gridfs.clear()
        self._pending_configs.clear()
        return cleaned

    def clear(self) -> None:
        self._pending_gridfs.clear()
        self._pending_configs.clear()

    @property
    def pending_count(self) -> int:
        return len(self._pending_gridfs) + len(self._pending_configs)


@retry_on_failure(max_retries=3)
def upload_to_gridfs(
    bucket: GridFS,
    file_path: str | Path,
    original_filename: str,
    content_type: str = "application/octet-stream",
    extra_metadata: Optional[dict] = None,
    orphan_tracker: Optional[GridFSOrphanTracker] = None,
    precomputed_checksum: Optional[str] = None,
) -> ObjectId:
    path = Path(file_path)
    if not path.exists():
        raise GridFSError(f"Cannot upload: file not found at {path}")

    try:
        checksum = precomputed_checksum or compute_file_checksum(path)
        metadata = {
            "original_filename": original_filename,
            "content_type": content_type,
            "checksum": checksum,
        }
        if extra_metadata:
            metadata.update(extra_metadata)

        with open(path, "rb") as f:
            gridfs_id = bucket.put(
                f,
                filename=original_filename,
                content_type=content_type,
                metadata=metadata,
            )

        if orphan_tracker:
            orphan_tracker.track(bucket, gridfs_id)

        logger.info(
            "gridfs.uploaded file=%s id=%s size=%d checksum=%s",
            original_filename, gridfs_id, path.stat().st_size, checksum,
        )
        return gridfs_id

    except GridFSError:
        raise
    except Exception as exc:
        raise GridFSError(f"Failed to upload '{original_filename}' to GridFS: {exc}") from exc


@retry_on_failure(max_retries=3)
def download_from_gridfs(bucket: GridFS, gridfs_id: ObjectId) -> tuple[bytes, dict]:
    try:
        if not bucket.exists(gridfs_id):
            raise GridFSError(f"GridFS file not found: {gridfs_id}")

        grid_out = bucket.get(gridfs_id)
        data = grid_out.read()
        metadata = {
            "filename": grid_out.filename,
            "content_type": grid_out.content_type,
            "length": grid_out.length,
            "upload_date": grid_out.upload_date,
            "metadata": grid_out.metadata,
        }

        logger.info("gridfs.downloaded id=%s file=%s size=%d", gridfs_id, grid_out.filename, grid_out.length)
        return data, metadata

    except GridFSError:
        raise
    except Exception as exc:
        raise GridFSError(f"Failed to download GridFS file {gridfs_id}: {exc}") from exc


def delete_from_gridfs(bucket: GridFS, gridfs_id: ObjectId) -> None:
    try:
        bucket.delete(gridfs_id)
        logger.info("gridfs.deleted id=%s", gridfs_id)
    except Exception as exc:
        raise GridFSError(f"Failed to delete GridFS file {gridfs_id}: {exc}") from exc
