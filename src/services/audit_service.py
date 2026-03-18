"""Audit log entry factory."""

from datetime import datetime, timezone
from typing import Any, Dict

from src.models.schemas import AuditEntry


def create_audit_entry(action: str, details: str = "") -> Dict[str, Any]:
    entry = AuditEntry(
        action=action,
        timestamp=datetime.now(timezone.utc),
        details=details,
    )
    return entry.model_dump()
