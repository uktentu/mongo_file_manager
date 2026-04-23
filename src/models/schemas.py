"""Pydantic models for data validation and serialization."""

from datetime import datetime, timezone
from typing import List, Optional

from pydantic import BaseModel, Field


# Standard timestamp format used everywhere in this project
TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def _now_formatted() -> str:
    """Return current UTC time as string in project standard format."""
    return datetime.now(timezone.utc).strftime(TS_FORMAT)


class AuditEntry(BaseModel):
    action: str
    timestamp: str = Field(default_factory=_now_formatted)
    details: str = ""


class OriginalFiles(BaseModel):
    json_config: str
    template: Optional[str] = None
    sql_file: str


class FileContents(BaseModel):
    """Stores GridFS ObjectIds for file contents."""
    json_config_id: str
    sql_file_id: str
    template_id: Optional[str] = None


class Checksums(BaseModel):
    json_config: str
    template: Optional[str] = None
    sql_file: str


class FileSizes(BaseModel):
    json_config: int
    template: Optional[int] = None
    sql_file: int


class MetadataDocument(BaseModel):
    report_id: str
    csi_id: str
    region: str
    regulation: str
    name: str
    original_files: OriginalFiles
    file_contents: FileContents
    checksums: Checksums
    file_sizes: FileSizes
    mongoInsertedTs: str = Field(default_factory=_now_formatted)
    mongoUpdatedTs: str = Field(default_factory=_now_formatted)
    active: bool = True
    version: int = Field(default=1, ge=1)
    audit_log: List[AuditEntry] = Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True

    def to_mongo_dict(self) -> dict:
        data = self.model_dump()
        return data


class SeedBundleEntry(BaseModel):
    csi_id: str
    region: str
    regulation: str
    json_config: str
    template: Optional[str] = None
    sql_file: str
