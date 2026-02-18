"""MongoDB and GridFS connection management."""

import os
import logging
from pymongo import MongoClient, ASCENDING
from pymongo.errors import (
    ConnectionFailure,
    OperationFailure,
    ServerSelectionTimeoutError,
)
from gridfs import GridFS
from dotenv import load_dotenv

from src.errors.exceptions import DatabaseError

load_dotenv()

logger = logging.getLogger(__name__)


class DatabaseManager:

    def __init__(self, uri: str | None = None, db_name: str | None = None):
        self._uri = uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self._db_name = db_name or os.getenv("MONGO_DB_NAME", "doc_management")
        self._client: MongoClient | None = None
        self._db = None
        self._templates_gridfs: GridFS | None = None
        self._sqlfiles_gridfs: GridFS | None = None
        self._supports_transactions: bool = False

    def connect(self):
        try:
            self._client = MongoClient(
                self._uri,
                maxPoolSize=50,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                retryWrites=True,
            )
            self._client.admin.command("ping")
            self._db = self._client[self._db_name]
            self._detect_transaction_support()
            self._templates_gridfs = GridFS(self._db, collection="templates")
            self._sqlfiles_gridfs = GridFS(self._db, collection="sqlfiles")
            self._ensure_indexes()

            logger.info(
                "database.connected uri=%s db=%s transactions=%s",
                self._uri,
                self._db_name,
                self._supports_transactions,
            )
        except (ConnectionFailure, ServerSelectionTimeoutError) as exc:
            logger.error("database.connection_failed uri=%s error=%s", self._uri, exc)
            raise DatabaseError(f"Failed to connect to MongoDB at {self._uri}: {exc}") from exc

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
        metadata = self._db["metadata"]

        try:
            metadata.create_index(
                [("unique_id", ASCENDING)],
                name="idx_unique_id_active_unique",
                unique=True,
                partialFilterExpression={"active": True},
            )
        except OperationFailure as exc:
            if "already exists" not in str(exc).lower():
                logger.warning("database.partial_index_failed error=%s", exc)

        metadata.create_index(
            [("unique_id", ASCENDING), ("active", ASCENDING)],
            name="idx_unique_id_active",
        )
        metadata.create_index("csi_id", name="idx_csi_id")
        metadata.create_index("region", name="idx_region")
        metadata.create_index("regulation", name="idx_regulation")
        metadata.create_index("active", name="idx_active")
        metadata.create_index(
            [("unique_id", ASCENDING), ("version", ASCENDING)],
            name="idx_unique_id_version",
        )
        logger.info("database.indexes_ensured collection=metadata")

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
        return self.db["metadata"]

    @property
    def configs_collection(self):
        return self.db["configs"]

    @property
    def templates_gridfs(self) -> GridFS:
        if self._templates_gridfs is None:
            raise DatabaseError("Database not connected. Call connect() first.")
        return self._templates_gridfs

    @property
    def sqlfiles_gridfs(self) -> GridFS:
        if self._sqlfiles_gridfs is None:
            raise DatabaseError("Database not connected. Call connect() first.")
        return self._sqlfiles_gridfs

    def close(self):
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
            self._templates_gridfs = None
            self._sqlfiles_gridfs = None
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
