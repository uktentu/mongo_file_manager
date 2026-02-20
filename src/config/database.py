"""MongoDB connection management — single metadata collection."""

import logging
from pymongo import MongoClient, ASCENDING
from pymongo.errors import (
    ConnectionFailure,
    OperationFailure,
    ServerSelectionTimeoutError,
)
from gridfs import GridFS

from src.config.settings import get_settings
from src.errors.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class DatabaseManager:

    def __init__(self, uri: str | None = None, db_name: str | None = None):
        settings = get_settings()
        self._uri = uri or settings.mongo_uri
        self._db_name = db_name or settings.mongo_db_name
        self._col_metadata = settings.mongo_metadata_collection
        self._bucket_name = settings.mongo_gridfs_bucket
        self._client: MongoClient | None = None
        self._db = None
        self._fs: GridFS | None = None
        self._supports_transactions: bool = False

    def connect(self):
        settings = get_settings()
        try:
            self._client = MongoClient(
                self._uri,
                maxPoolSize=settings.mongo_max_pool_size,
                serverSelectionTimeoutMS=settings.mongo_server_timeout_ms,
                connectTimeoutMS=settings.mongo_connect_timeout_ms,
                retryWrites=True,
            )
            self._client.admin.command("ping")
            self._db = self._client[self._db_name]

            # Initialize GridFS with the configured bucket name
            self._fs = GridFS(self._db, collection=self._bucket_name)

            self._detect_transaction_support()
            self._ensure_indexes()

            # Log host only — never log the full URI (may contain credentials)
            try:
                host_display = self._client.address or self._uri.split("@")[-1].split("/")[0]
            except Exception:
                host_display = "unknown"
            logger.info(
                "database.connected host=%s db=%s metadata=%s gridfs=%s transactions=%s",
                host_display, self._db_name,
                self._col_metadata, self._bucket_name,
                self._supports_transactions,
            )
        except (ConnectionFailure, ServerSelectionTimeoutError) as exc:
            logger.error("database.connection_failed error=%s", exc)
            raise DatabaseError(f"Failed to connect to MongoDB: {exc}") from exc

    def _detect_transaction_support(self):
        try:
            hello = self._client.admin.command("hello")
            is_replica = "setName" in hello
            is_mongos = hello.get("msg") == "isdbgrid"
            self._supports_transactions = is_replica or is_mongos

            if not self._supports_transactions:
                logger.warning(
                    "database.standalone_mode transactions are NOT available; "
                    "operations will proceed without atomicity guarantees"
                )
        except Exception:
            self._supports_transactions = False
            logger.warning("database.transaction_detection_failed assuming standalone")

    def _ensure_indexes(self):
        col_name = self._col_metadata
        metadata = self._db[col_name]

        # Unique partial index: one active document per report_id
        try:
            metadata.create_index(
                [("report_id", ASCENDING)],
                name="idx_report_id_active_unique",
                unique=True,
                partialFilterExpression={"active": True},
            )
        except OperationFailure as exc:
            if "already exists" not in str(exc).lower():
                logger.warning("database.partial_index_failed error=%s", exc)

        metadata.create_index(
            [("report_id", ASCENDING), ("active", ASCENDING)],
            name="idx_report_id_active",
        )
        metadata.create_index(
            [("report_id", ASCENDING), ("version", ASCENDING)],
            name="idx_report_id_version",
        )
        # Composite key index for deduplication during seeding
        metadata.create_index(
            [("csi_id", ASCENDING), ("regulation", ASCENDING), ("region", ASCENDING), ("active", ASCENDING)],
            name="idx_composite_dedup",
        )
        metadata.create_index("csi_id", name="idx_csi_id")
        metadata.create_index("region", name="idx_region")
        metadata.create_index("regulation", name="idx_regulation")
        metadata.create_index("active", name="idx_active")
        logger.info("database.indexes_ensured collection=%s", col_name)

    @property
    def supports_transactions(self) -> bool:
        return self._supports_transactions

    @property
    def client(self) -> MongoClient:
        if self._client is None:
            raise DatabaseError("Database not connected. Call connect() first.")
        return self._client

    @property
    def db(self):
        if self._db is None:
            raise DatabaseError("Database not connected. Call connect() first.")
        return self._db

    @property
    def metadata_collection(self):
        return self.db[self._col_metadata]


    @property
    def fs(self) -> GridFS:
        if self._fs is None:
            raise DatabaseError("Database not connected.")
        return self._fs

    def close(self):
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
            logger.info("database.disconnected")

    def start_session(self):
        return self.client.start_session()


_default_instance: DatabaseManager | None = None


def create_db_manager(uri: str | None = None, db_name: str | None = None) -> DatabaseManager:
    mgr = DatabaseManager(uri=uri, db_name=db_name)
    mgr.connect()
    return mgr


def get_db() -> DatabaseManager:
    global _default_instance
    if _default_instance is None or _default_instance._client is None:
        _default_instance = create_db_manager()
    return _default_instance


def set_db(instance: DatabaseManager) -> None:
    global _default_instance
    _default_instance = instance


def reset_db() -> None:
    global _default_instance
    if _default_instance:
        _default_instance.close()
    _default_instance = None
