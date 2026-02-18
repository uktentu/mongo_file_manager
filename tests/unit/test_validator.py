"""
Unit tests for the validator utilities.
"""

import json
from pathlib import Path

import pytest

from src.utils.validator import validate_json_config, validate_file_exists, validate_seed_bundle
from src.errors.exceptions import ValidationError


class TestValidateJsonConfig:
    """Tests for validate_json_config."""

    def test_valid_config(self, tmp_path):
        config = {"name": "Test Report", "outFileName": "test_output", "extra": "data"}
        config_file = tmp_path / "valid.json"
        config_file.write_text(json.dumps(config))

        result = validate_json_config(config_file)
        assert result["name"] == "Test Report"
        assert result["outFileName"] == "test_output"

    def test_missing_name(self, tmp_path):
        config = {"outFileName": "test_output"}
        config_file = tmp_path / "no_name.json"
        config_file.write_text(json.dumps(config))

        with pytest.raises(ValidationError, match="missing required fields"):
            validate_json_config(config_file)

    def test_missing_out_file_name(self, tmp_path):
        config = {"name": "Test"}
        config_file = tmp_path / "no_out.json"
        config_file.write_text(json.dumps(config))

        with pytest.raises(ValidationError, match="missing required fields"):
            validate_json_config(config_file)

    def test_invalid_json(self, tmp_path):
        config_file = tmp_path / "bad.json"
        config_file.write_text("{invalid json")

        with pytest.raises(ValidationError, match="Invalid JSON"):
            validate_json_config(config_file)

    def test_file_not_found(self):
        with pytest.raises(ValidationError, match="not found"):
            validate_json_config("/nonexistent/config.json")

    def test_non_json_extension(self, tmp_path):
        config_file = tmp_path / "config.txt"
        config_file.write_text('{"name": "Test", "outFileName": "out"}')

        with pytest.raises(ValidationError, match=".json"):
            validate_json_config(config_file)

    def test_json_array_rejected(self, tmp_path):
        config_file = tmp_path / "array.json"
        config_file.write_text('[{"name": "Test"}]')

        with pytest.raises(ValidationError, match="object/dict"):
            validate_json_config(config_file)

    def test_empty_name_rejected(self, tmp_path):
        config = {"name": "", "outFileName": "test_output"}
        config_file = tmp_path / "empty_name.json"
        config_file.write_text(json.dumps(config))

        with pytest.raises(ValidationError, match="missing required fields"):
            validate_json_config(config_file)


class TestValidateFileExists:
    """Tests for validate_file_exists."""

    def test_existing_file(self, tmp_path):
        file = tmp_path / "exists.txt"
        file.write_text("content")

        result = validate_file_exists(file)
        assert result.exists()

    def test_missing_file(self):
        with pytest.raises(ValidationError, match="not found"):
            validate_file_exists("/nonexistent/file.txt")

    def test_directory_rejected(self, tmp_path):
        with pytest.raises(ValidationError, match="not a file"):
            validate_file_exists(tmp_path, "Directory")


class TestValidateSeedBundle:
    """Tests for validate_seed_bundle."""

    def test_valid_bundle(self, tmp_path):
        # Create required files
        config = {"name": "Test", "outFileName": "out"}
        (tmp_path / "configs").mkdir()
        (tmp_path / "sql").mkdir()
        (tmp_path / "configs" / "test.json").write_text(json.dumps(config))
        (tmp_path / "sql" / "test.sql").write_text("SELECT 1;")

        bundle = {
            "csi_id": "CSI-001",
            "region": "APAC",
            "regulation": "MAS-TRM",
            "json_config": "configs/test.json",
            "sql_file": "sql/test.sql",
        }

        result = validate_seed_bundle(bundle, tmp_path)
        assert result["csi_id"] == "CSI-001"
        assert result["template"] is None

    def test_missing_required_key(self, tmp_path):
        bundle = {"csi_id": "CSI-001", "region": "APAC"}

        with pytest.raises(ValidationError, match="missing required keys"):
            validate_seed_bundle(bundle, tmp_path)

    def test_with_template(self, tmp_path):
        config = {"name": "Test", "outFileName": "out"}
        (tmp_path / "configs").mkdir()
        (tmp_path / "sql").mkdir()
        (tmp_path / "templates").mkdir()
        (tmp_path / "configs" / "test.json").write_text(json.dumps(config))
        (tmp_path / "sql" / "test.sql").write_text("SELECT 1;")
        (tmp_path / "templates" / "test.xlsx").write_bytes(b"template data")

        bundle = {
            "csi_id": "CSI-001",
            "region": "APAC",
            "regulation": "MAS-TRM",
            "json_config": "configs/test.json",
            "sql_file": "sql/test.sql",
            "template": "templates/test.xlsx",
        }

        result = validate_seed_bundle(bundle, tmp_path)
        assert result["template"] is not None
