"""
Unit tests for the checksum utility.
"""

import hashlib
import tempfile
from pathlib import Path

import pytest

from src.utils.checksum import compute_file_checksum, compute_bytes_checksum, verify_checksum


class TestComputeFileChecksum:
    """Tests for compute_file_checksum."""

    def test_known_content(self, tmp_path):
        """Checksum of known content should match manually computed hash."""
        content = b"Hello, World!"
        file = tmp_path / "test.txt"
        file.write_bytes(content)

        expected_hash = hashlib.sha256(content).hexdigest()
        result = compute_file_checksum(file)
        assert result == f"sha256:{expected_hash}"

    def test_empty_file(self, tmp_path):
        """Checksum of an empty file should be the SHA-256 of empty bytes."""
        file = tmp_path / "empty.txt"
        file.write_bytes(b"")

        expected_hash = hashlib.sha256(b"").hexdigest()
        result = compute_file_checksum(file)
        assert result == f"sha256:{expected_hash}"

    def test_binary_content(self, tmp_path):
        """Checksum should work on binary content."""
        content = bytes(range(256))
        file = tmp_path / "binary.bin"
        file.write_bytes(content)

        expected_hash = hashlib.sha256(content).hexdigest()
        result = compute_file_checksum(file)
        assert result == f"sha256:{expected_hash}"

    def test_file_not_found(self):
        """Should raise FileNotFoundError for missing files."""
        with pytest.raises(FileNotFoundError):
            compute_file_checksum("/nonexistent/path/file.txt")

    def test_deterministic(self, tmp_path):
        """Same file content should always produce the same checksum."""
        content = b"deterministic test"
        file = tmp_path / "det.txt"
        file.write_bytes(content)

        result1 = compute_file_checksum(file)
        result2 = compute_file_checksum(file)
        assert result1 == result2


class TestComputeBytesChecksum:
    """Tests for compute_bytes_checksum."""

    def test_known_bytes(self):
        data = b"test data"
        expected = f"sha256:{hashlib.sha256(data).hexdigest()}"
        assert compute_bytes_checksum(data) == expected

    def test_empty_bytes(self):
        data = b""
        expected = f"sha256:{hashlib.sha256(data).hexdigest()}"
        assert compute_bytes_checksum(data) == expected


class TestVerifyChecksum:
    """Tests for verify_checksum."""

    def test_matching_checksum(self, tmp_path):
        content = b"verify me"
        file = tmp_path / "verify.txt"
        file.write_bytes(content)

        checksum = compute_file_checksum(file)
        assert verify_checksum(file, checksum) is True

    def test_mismatched_checksum(self, tmp_path):
        content = b"original"
        file = tmp_path / "mismatch.txt"
        file.write_bytes(content)

        assert verify_checksum(file, "sha256:000000") is False
