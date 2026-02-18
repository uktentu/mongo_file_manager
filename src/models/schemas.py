"""Pydantic models for data validation and serialization."""

from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field


class AuditEntry(BaseModel):
    action: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    details: str = ""


class OriginalFiles(BaseModel):
    json_config: str
    template: Optional[str] = None
    sql_file: str


class FileReferences(BaseModel):
    json_config_id: Any
    template_gridfs_id: Optional[Any] = None
    sql_gridfs_id: Any


class Checksums(BaseModel):
    json_config: str
    template: Optional[str] = None
    sql_file: str


class FileSizes(BaseModel):
    json_config: int
    template: Optional[int] = None
    sql_file: int


class MetadataDocument(BaseModel):
    unique_id: str
    csi_id: str
    region: str
    regulation: str
    name: str
    out_file_name: str
    original_files: OriginalFiles
    file_references: FileReferences
    checksums: Checksums
    file_sizes: FileSizes
    uploaded_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    active: bool = True
    version: int = Field(default=1, ge=1)
    audit_log: list[AuditEntry] = Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True

    def to_mongo_dict(self) -> dict:
        data = self.model_dump()
        data["uploaded_at"] = self.uploaded_at
        return data


class SeedBundleEntry(BaseModel):
    csi_id: str
    region: str
    regulation: str
    json_config: str
    template: Optional[str] = None
    sql_file: str
