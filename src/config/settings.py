"""
src/config/settings.py — Central application configuration.

All environment variable reads happen here. Every other module imports
`get_settings()` instead of calling `os.getenv` directly.

Environment variables (also see .env.example at the repo root):

┌─────────────────────────┬───────────────────────────────────────────────┬──────────────────────────────────────────┐
│ Variable                │ Description                                   │ Default                                  │
├─────────────────────────┼───────────────────────────────────────────────┼──────────────────────────────────────────┤
│ MONGO_URI               │ Full MongoDB connection string                 │ mongodb://localhost:27017                │
│ MONGO_DB_NAME           │ Target database name                          │ doc_management                           │
│ MONGO_METADATA_COLLECTION│ Collection to store document metadata        │ metadata                                 │
│ MONGO_GRIDFS_BUCKET     │ GridFS bucket name for binary file storage    │ fs                                       │
│ MONGO_MAX_POOL_SIZE     │ Connection pool size                          │ 50                                       │
│ MONGO_CONNECT_TIMEOUT_MS│ Connect timeout in milliseconds               │ 5000                                     │
│ MONGO_SERVER_TIMEOUT_MS │ Server selection timeout in milliseconds      │ 5000                                     │
│ API_KEY                 │ Shared secret for API auth (empty = disabled) │ (empty — auth disabled)                  │
│ API_HOST                │ Host to bind the API server to                │ 0.0.0.0                                  │
│ API_PORT                │ Port to bind the API server to                │ 8000                                     │
│ API_WORKERS             │ Number of Gunicorn worker processes           │ 2                                        │
│ LOG_LEVEL               │ Logging level: DEBUG/INFO/WARNING/ERROR       │ INFO                                     │
│ LOG_FORMAT              │ json or text                                  │ text                                     │
│ ENVIRONMENT             │ development / staging / production            │ development                              │
└─────────────────────────┴───────────────────────────────────────────────┴──────────────────────────────────────────┘
"""

import logging
import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the project root (one level above src/)
_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_ROOT / ".env")

logger = logging.getLogger(__name__)


class Settings:
    """
    Typed, validated application settings loaded from environment variables.
    Use get_settings() (cached singleton) rather than instantiating directly.
    """

    # ── MongoDB ──────────────────────────────────────────────────────────────
    mongo_uri: str
    mongo_db_name: str
    mongo_metadata_collection: str
    mongo_gridfs_bucket: str
    mongo_max_pool_size: int
    mongo_connect_timeout_ms: int
    mongo_server_timeout_ms: int

    # ── API server ───────────────────────────────────────────────────────────
    api_key: str                    # empty string means auth is disabled
    api_host: str
    api_port: int
    api_workers: int

    # ── Logging ──────────────────────────────────────────────────────────────
    log_level: str
    log_format: str                 # "text" or "json"

    # ── Runtime ──────────────────────────────────────────────────────────────
    environment: str                # development | staging | production

    def __init__(self) -> None:
        # ── MongoDB ──────────────────────────────────────────────────────
        self.mongo_uri = _require_env(
            "MONGO_URI",
            default="mongodb://localhost:27017",
            description="MongoDB connection string",
        )
        self.mongo_db_name = _require_env(
            "MONGO_DB_NAME",
            default="doc_management",
            description="MongoDB database name",
        )
        self.mongo_metadata_collection = _require_env(
            "MONGO_METADATA_COLLECTION",
            default="metadata",
            description="Collection for document metadata",
        )
        self.mongo_gridfs_bucket = _require_env(
            "MONGO_GRIDFS_BUCKET",
            default="fs",
            description="GridFS bucket name for binary file storage",
        )
        self.mongo_max_pool_size = _int_env("MONGO_MAX_POOL_SIZE", default=50)
        self.mongo_connect_timeout_ms = _int_env("MONGO_CONNECT_TIMEOUT_MS", default=5000)
        self.mongo_server_timeout_ms = _int_env("MONGO_SERVER_TIMEOUT_MS", default=5000)

        # ── API ──────────────────────────────────────────────────────────
        self.api_key = os.getenv("API_KEY", "")
        self.api_host = os.getenv("API_HOST", "0.0.0.0")
        self.api_port = _int_env("API_PORT", default=8000)
        self.api_workers = _int_env("API_WORKERS", default=2)

        # ── Logging ──────────────────────────────────────────────────────
        raw_level = os.getenv("LOG_LEVEL", "INFO").upper()
        if raw_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise SettingsError(
                f"LOG_LEVEL must be one of DEBUG/INFO/WARNING/ERROR/CRITICAL, got '{raw_level}'"
            )
        self.log_level = raw_level

        raw_format = os.getenv("LOG_FORMAT", "text").lower()
        if raw_format not in {"text", "json"}:
            raise SettingsError(
                f"LOG_FORMAT must be 'text' or 'json', got '{raw_format}'"
            )
        self.log_format = raw_format

        # ── Runtime ──────────────────────────────────────────────────────
        self.environment = os.getenv("ENVIRONMENT", "development").lower()

        self._validate_required()
        logger.debug(
            "settings.loaded env=%s db=%s log_level=%s api_auth=%s",
            self.environment, self.mongo_db_name,
            self.log_level, "enabled" if self.api_key else "disabled",
        )

    # ── Convenience properties ────────────────────────────────────────────────

    @property
    def is_production(self) -> bool:
        return self.environment == "production"

    @property
    def api_auth_enabled(self) -> bool:
        return bool(self.api_key)

    @property
    def log_level_int(self) -> int:
        return getattr(logging, self.log_level, logging.INFO)

    def __repr__(self) -> str:
        return (
            f"Settings(env={self.environment!r}, db={self.mongo_db_name!r}, "
            f"log_level={self.log_level!r}, api_auth={self.api_auth_enabled})"
        )

    # ── Validation ────────────────────────────────────────────────────────────

    def _validate_required(self) -> None:
        errors: list[str] = []

        if not self.mongo_uri.strip():
            errors.append("MONGO_URI must not be empty")
        if not self.mongo_db_name.strip():
            errors.append("MONGO_DB_NAME must not be empty")
        if self.mongo_max_pool_size < 1:
            errors.append("MONGO_MAX_POOL_SIZE must be >= 1")
        if self.api_port < 1 or self.api_port > 65535:
            errors.append(f"API_PORT must be 1–65535, got {self.api_port}")
        if self.api_workers < 1:
            errors.append("API_WORKERS must be >= 1")
        if self.is_production and not self.api_key:
            errors.append(
                "API_KEY must be set in ENVIRONMENT=production (auth cannot be disabled in prod)"
            )

        if errors:
            raise SettingsError("Configuration errors:\n" + "\n".join(f"  • {e}" for e in errors))


# ── Helpers ───────────────────────────────────────────────────────────────────

class SettingsError(Exception):
    """Raised when required config is missing or invalid at startup."""


def _require_env(name: str, default: str, description: str) -> str:
    value = os.getenv(name, default)
    if not value.strip():
        raise SettingsError(f"{name} ({description}) must not be empty")
    return value


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(raw)
    except ValueError:
        raise SettingsError(f"{name} must be an integer, got '{raw}'")


# ── Cached singleton ──────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the global Settings singleton (created once, then cached)."""
    return Settings()
