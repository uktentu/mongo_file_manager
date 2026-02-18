"""Custom exception hierarchy."""


class SeederError(Exception):
    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class ValidationError(SeederError):
    pass


class FileNotFoundError(SeederError):
    pass


class DuplicateRecordError(SeederError):
    pass


class DatabaseError(SeederError):
    pass


class GridFSError(SeederError):
    pass


class ChecksumMismatchError(SeederError):
    pass


class RecordNotFoundError(SeederError):
    pass
