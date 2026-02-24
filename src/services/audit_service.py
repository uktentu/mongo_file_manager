"""Audit log entry factory."""

from datetime import datetime, timezone
from typing import Any

from src.models.schemas import AuditEntry


def create_audit_entry(action: str, details: str = "") -> dict[str, Any]:
    entry = AuditEntry(
        action=action,
        timestamp=datetime.now(timezone.utc),
        details=details,
    )
    return entry.model_dump()
