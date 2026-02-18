"""
Unit tests for the unique ID builder.
"""

import pytest

from src.utils.unique_id import build_unique_id, _normalize


class TestNormalize:
    """Tests for the _normalize helper."""

    def test_basic_normalization(self):
        assert _normalize("Hello World") == "hello_world"

    def test_strips_whitespace(self):
        assert _normalize("  spaces  ") == "spaces"

    def test_replaces_multiple_spaces(self):
        assert _normalize("a   b   c") == "a_b_c"

    def test_removes_special_characters(self):
        assert _normalize("report@v2!") == "reportv2"

    def test_keeps_hyphens(self):
        assert _normalize("MAS-TRM") == "mas-trm"

    def test_keeps_underscores(self):
        assert _normalize("out_file_name") == "out_file_name"

    def test_empty_string(self):
        assert _normalize("") == ""


class TestBuildUniqueId:
    """Tests for build_unique_id."""

    def test_basic_id(self):
        result = build_unique_id(
            regulation="MAS-TRM",
            name="Compliance Report",
            out_file_name="mas_output",
            region="APAC",
        )
        assert result == "mas-trm_compliance_report_mas_output_apac"

    def test_normalization_applied(self):
        result = build_unique_id(
            regulation="  GDPR  ",
            name="Privacy Report",
            out_file_name="gdpr output file",
            region=" EU ",
        )
        assert result == "gdpr_privacy_report_gdpr_output_file_eu"

    def test_deterministic(self):
        """Same inputs should always produce the same ID."""
        args = {
            "regulation": "MAS-TRM",
            "name": "Report",
            "out_file_name": "output",
            "region": "APAC",
        }
        assert build_unique_id(**args) == build_unique_id(**args)

    def test_empty_regulation_raises(self):
        with pytest.raises(ValueError, match="regulation"):
            build_unique_id(
                regulation="",
                name="Report",
                out_file_name="output",
                region="APAC",
            )

    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="name"):
            build_unique_id(
                regulation="GDPR",
                name="   ",
                out_file_name="output",
                region="EU",
            )

    def test_empty_region_raises(self):
        with pytest.raises(ValueError, match="region"):
            build_unique_id(
                regulation="GDPR",
                name="Report",
                out_file_name="output",
                region="",
            )

    def test_special_chars_stripped(self):
        result = build_unique_id(
            regulation="REG@1.0",
            name="Report (v2)",
            out_file_name="out.file",
            region="US/CA",
        )
        # @, ., (, ), / are stripped
        assert result == "reg10_report_v2_outfile_usca"
