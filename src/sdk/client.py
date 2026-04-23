"""
src/sdk/client.py — ReportGen integration client.

This module provides TWO ways for ReportGen to access files stored in
the MongoDB Document Seeder:

  1. **Direct MongoDB** (recommended for same-network / co-located services):
     Uses the same MongoDB connection to read files directly from GridFS.
     No HTTP overhead, no temp files, maximum reliability.

  2. **HTTP API** (for cross-network / separate deployments):
     Calls the Seeder's REST API to stream files over HTTP.

Usage (Direct MongoDB — recommended):
    from src.sdk.client import ReportGenClient

    client = ReportGenClient.from_env()        # reads MONGO_URI from env
    # or: client = ReportGenClient.from_uri("mongodb+srv://...")

    # Get file bytes directly (no disk I/O)
    json_bytes = client.get_file_bytes("report-uuid-here", "json_config")
    sql_bytes  = client.get_file_bytes("report-uuid-here", "sql_file")

    # Get parsed JSON config as dict
    config = client.get_json_config("report-uuid-here")

    # Get SQL query as string
    sql = client.get_sql_query("report-uuid-here")

    # Get full record metadata
    record = client.get_record("report-uuid-here")

    # List all active records for a regulation
    records = client.list_records(regulation="MAS-TRM")

    # Stream file to disk (only if you really need disk files)
    path = client.export_file("report-uuid-here", "sql_file", "/tmp/output")

    client.close()

Usage (HTTP API):
    from src.sdk.client import ReportGenHTTPClient

    client = ReportGenHTTPClient(
        base_url="http://seeder-host:8000",
        api_key="your-key",
    )

    json_bytes = client.get_file_bytes("report-uuid-here", "json_config")
    client.close()
"""

import io
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

VALID_FILE_KEYS = {"json_config", "sql_file", "template"}


class ReportGenClient:
    """Direct MongoDB client for ReportGen — zero HTTP overhead.

    This is the RECOMMENDED integration method when ReportGen runs on
    the same network as the MongoDB instance. Files are read directly
    from GridFS with no intermediate export step.
    """

    def __init__(
        self,
        mongo_uri: str,
        db_name: str = "doc_management",
        metadata_collection: str = "metadata",
        gridfs_bucket: str = "fs",
    ):
        from pymongo import MongoClient
        from gridfs import GridFS

        self._client = MongoClient(
            mongo_uri,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000,
            retryWrites=True,
        )
        self._db = self._client[db_name]
        self._metadata = self._db[metadata_collection]
        self._fs = GridFS(self._db, collection=gridfs_bucket)

        # Verify connection
        self._client.admin.command("ping")
        logger.info(
            "reportgen_client.connected db=%s collection=%s",
            db_name, metadata_collection,
        )

    @classmethod
    def from_env(cls) -> "ReportGenClient":
        """Create client from environment variables (same as the Seeder uses).

        Reads: MONGO_URI, MONGO_DB_NAME, MONGO_METADATA_COLLECTION, MONGO_GRIDFS_BUCKET
        """
        import os
        return cls(
            mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
            db_name=os.getenv("MONGO_DB_NAME", "doc_management"),
            metadata_collection=os.getenv("MONGO_METADATA_COLLECTION", "metadata"),
            gridfs_bucket=os.getenv("MONGO_GRIDFS_BUCKET", "fs"),
        )

    @classmethod
    def from_uri(
        cls,
        mongo_uri: str,
        db_name: str = "doc_management",
    ) -> "ReportGenClient":
        """Create client with explicit connection string."""
        return cls(mongo_uri=mongo_uri, db_name=db_name)

    def close(self) -> None:
        """Close the MongoDB connection."""
        if self._client:
            self._client.close()
            logger.info("reportgen_client.closed")

    def __enter__(self) -> "ReportGenClient":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    # ── Record queries ─────────────────────────────────────────────────

    def get_record(
        self,
        report_id: str,
        version: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get metadata for a record (active version by default)."""
        if version is not None:
            record = self._metadata.find_one({
                "report_id": report_id,
                "version": version,
            })
        else:
            record = self._metadata.find_one({
                "report_id": report_id,
                "active": True,
            })

        if record:
            logger.info(
                "reportgen_client.get_record report_id=%s version=%s",
                report_id, record.get("version"),
            )
        return record

    def list_records(
        self,
        csi_id: Optional[str] = None,
        region: Optional[str] = None,
        regulation: Optional[str] = None,
        active_only: bool = True,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """List records matching filters."""
        query: Dict[str, Any] = {}
        if csi_id:
            query["csi_id"] = csi_id
        if region:
            query["region"] = region
        if regulation:
            query["regulation"] = regulation
        if active_only:
            query["active"] = True

        results = list(self._metadata.find(query).limit(limit))
        logger.info("reportgen_client.list_records query=%s count=%d", query, len(results))
        return results

    # ── Direct file access (the key integration point) ─────────────────

    def get_file_bytes(
        self,
        report_id: str,
        file_key: str,
        version: Optional[int] = None,
        verify_checksum: bool = True,
    ) -> bytes:
        """Get raw file bytes directly from GridFS — no temp files, no disk I/O.

        This is the primary method ReportGen should use. It reads the file
        content directly from MongoDB GridFS and returns it as bytes.

        Args:
            report_id:       UUID report_id of the record.
            file_key:        One of 'json_config', 'sql_file', 'template'.
            version:         Specific version (default: active).
            verify_checksum: Validate SHA-256 checksum after reading.

        Returns:
            Raw file bytes.

        Raises:
            ValueError:  Invalid file_key or record not found.
            RuntimeError: Checksum mismatch (data corruption).
        """
        if file_key not in VALID_FILE_KEYS:
            raise ValueError(f"Invalid file_key '{file_key}'. Must be one of: {VALID_FILE_KEYS}")

        record = self.get_record(report_id, version=version)
        if not record:
            raise ValueError(f"Record not found: report_id={report_id}, version={version}")

        from bson import ObjectId

        contents = record.get("file_contents", {})
        id_key = f"{file_key}_id"
        gridfs_id_str = contents.get(id_key)

        if not gridfs_id_str:
            raise ValueError(
                f"File '{file_key}' not available in record "
                f"report_id={report_id} v{record.get('version')}"
            )

        grid_out = self._fs.get(ObjectId(gridfs_id_str))
        data = grid_out.read()

        if verify_checksum:
            import hashlib
            expected = record.get("checksums", {}).get(file_key)
            if expected:
                actual = "sha256:" + hashlib.sha256(data).hexdigest()
                if actual != expected:
                    raise RuntimeError(
                        f"Checksum mismatch for {file_key} in report_id={report_id}: "
                        f"expected={expected}, actual={actual}"
                    )

        logger.info(
            "reportgen_client.get_file report_id=%s file=%s size=%d",
            report_id, file_key, len(data),
        )
        return data

    def get_json_config(
        self,
        report_id: str,
        version: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Get parsed JSON config as a Python dict.

        Convenience wrapper around get_file_bytes() — reads the json_config
        file and parses it directly from memory.
        """
        raw = self.get_file_bytes(report_id, "json_config", version=version)
        return json.loads(raw.decode("utf-8"))

    def get_sql_query(
        self,
        report_id: str,
        version: Optional[int] = None,
    ) -> str:
        """Get SQL query as a string.

        Convenience wrapper — reads the sql_file and decodes it.
        """
        raw = self.get_file_bytes(report_id, "sql_file", version=version)
        return raw.decode("utf-8")

    def get_template(
        self,
        report_id: str,
        version: Optional[int] = None,
    ) -> Optional[str]:
        """Get template content as a string (returns None if no template)."""
        record = self.get_record(report_id, version=version)
        if not record:
            raise ValueError(f"Record not found: report_id={report_id}")

        contents = record.get("file_contents", {})
        if not contents.get("template_id"):
            return None

        raw = self.get_file_bytes(report_id, "template", version=version)
        return raw.decode("utf-8")

    def get_all_files(
        self,
        report_id: str,
        version: Optional[int] = None,
    ) -> Dict[str, bytes]:
        """Get all available files for a record as a dict of {key: bytes}.

        Returns:
            Dict with keys like 'json_config', 'sql_file', 'template' (if exists).
        """
        record = self.get_record(report_id, version=version)
        if not record:
            raise ValueError(f"Record not found: report_id={report_id}")

        files = {}
        contents = record.get("file_contents", {})
        for key in VALID_FILE_KEYS:
            id_key = f"{key}_id"
            if contents.get(id_key):
                try:
                    files[key] = self.get_file_bytes(report_id, key, version=version)
                except Exception as exc:
                    logger.warning(
                        "reportgen_client.get_all_files skipped file=%s error=%s",
                        key, exc,
                    )
        return files

    def export_file(
        self,
        report_id: str,
        file_key: str,
        output_dir: Union[str, Path],
        version: Optional[int] = None,
    ) -> Path:
        """Export a single file to disk (use only when disk files are needed).

        Most ReportGen use cases should use get_file_bytes() or
        get_json_config() instead to avoid disk I/O entirely.
        """
        data = self.get_file_bytes(report_id, file_key, version=version)
        record = self.get_record(report_id, version=version)
        original_name = record.get("original_files", {}).get(file_key, f"{file_key}.bin")

        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        file_path = out_path / original_name
        file_path.write_bytes(data)

        logger.info(
            "reportgen_client.export_file report_id=%s file=%s path=%s",
            report_id, file_key, file_path,
        )
        return file_path


class ReportGenHTTPClient:
    """HTTP-based client for ReportGen — for cross-network deployments.

    Use this when ReportGen cannot directly connect to the same MongoDB
    instance (e.g., different VPC, cloud region, etc.).
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        api_key: str = "",
        timeout: int = 120,
    ):
        import urllib.request
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout = timeout
        logger.info("reportgen_http_client.init base_url=%s", self._base_url)

    def _headers(self) -> dict:
        h = {"Content-Type": "application/json"}
        if self._api_key:
            h["X-API-Key"] = self._api_key
        return h

    def _get(self, endpoint: str) -> Dict[str, Any]:
        import urllib.request
        import urllib.error

        url = f"{self._base_url}/{endpoint.lstrip('/')}"
        req = urllib.request.Request(url, headers=self._headers(), method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            body = exc.read().decode()
            raise RuntimeError(f"HTTP {exc.code}: {body}") from exc

    def _get_bytes(self, endpoint: str) -> bytes:
        import urllib.request
        import urllib.error

        url = f"{self._base_url}/{endpoint.lstrip('/')}"
        req = urllib.request.Request(url, headers=self._headers(), method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            body = exc.read().decode()
            raise RuntimeError(f"HTTP {exc.code}: {body}") from exc

    def get_record(self, report_id: str) -> Dict[str, Any]:
        return self._get(f"/api/records/{report_id}")

    def list_records(self, **filters) -> Dict[str, Any]:
        params = "&".join(f"{k}={v}" for k, v in filters.items() if v is not None)
        endpoint = f"/api/records?{params}" if params else "/api/records"
        return self._get(endpoint)

    def get_file_bytes(self, report_id: str, file_key: str) -> bytes:
        """Stream file bytes from the Seeder API."""
        return self._get_bytes(f"/api/records/{report_id}/files/{file_key}")

    def get_json_config(self, report_id: str) -> Dict[str, Any]:
        raw = self.get_file_bytes(report_id, "json_config")
        return json.loads(raw.decode("utf-8"))

    def get_sql_query(self, report_id: str) -> str:
        raw = self.get_file_bytes(report_id, "sql_file")
        return raw.decode("utf-8")

    def list_record_files(self, report_id: str) -> Dict[str, Any]:
        return self._get(f"/api/records/{report_id}/files")

    def close(self) -> None:
        pass  # No persistent connection to close for urllib

    def __enter__(self) -> "ReportGenHTTPClient":
        return self

    def __exit__(self, *args) -> None:
        self.close()
