"""Audit log entry factory."""

from datetime import datetime, timezone
from src.models.schemas import AuditEntry


def create_audit_entry(action: str, details: str = "") -> dict:
    entry = AuditEntry(
        action=action,
        timestamp=datetime.now(timezone.utc),
        details=details,
    )
    return entry.model_dump()
