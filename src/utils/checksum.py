"""SHA-256 checksum utilities."""

import hashlib
from pathlib import Path

CHUNK_SIZE = 8192


def compute_file_checksum(file_path: str | Path) -> str:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            sha256.update(chunk)
    return f"sha256:{sha256.hexdigest()}"


def compute_bytes_checksum(data: bytes) -> str:
    sha256 = hashlib.sha256(data)
    return f"sha256:{sha256.hexdigest()}"


def verify_checksum(file_path: str | Path, expected_checksum: str) -> bool:
    actual = compute_file_checksum(file_path)
    return actual == expected_checksum
