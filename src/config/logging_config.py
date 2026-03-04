"""
src/config/logging_config.py — Centralized logging setup.

Call `configure_logging()` once at application/CLI startup.
All loggers across the project inherit from the root configuration set here.
"""

import logging
import sys
from typing import Optional

from src.config.settings import get_settings


def configure_logging(level: Optional[str] = None, fmt: Optional[str] = None) -> None:
    """
    Configure root logger for the whole application.

    Args:
        level: Override log level (DEBUG/INFO/WARNING/ERROR). Defaults to Settings.log_level.
        fmt:   Override format ("text" or "json"). Defaults to Settings.log_format.
    """
    settings = get_settings()
    log_level_str = level or settings.log_level
    log_format = fmt or settings.log_format
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)

    if log_format == "json":
        formatter = _JsonFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(log_level)

    # Quieten noisy libraries
    logging.getLogger("pymongo").setLevel(logging.WARNING)
    logging.getLogger("gridfs").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    logging.getLogger(__name__).debug(
        "logging.configured level=%s format=%s", log_level_str, log_format
    )


class _JsonFormatter(logging.Formatter):
    """Minimal structured JSON log formatter (no external dependencies)."""

    def format(self, record: logging.LogRecord) -> str:
        import json
        from datetime import datetime, timezone

        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)
