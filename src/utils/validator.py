"""
End-to-end input validation for seed manifests, JSON configs, and file paths.

Validation layers:
  1. Manifest structure (YAML-level)
  2. Bundle fields (required keys, non-empty, type checks, string formats)
  3. File existence + readability + non-empty content
  4. JSON config schema (required fields, value types, no blank strings)
  5. SQL file content (non-empty, UTF-8 readable)
  6. report_id format when supplied (7-digit zero-padded string)
"""

import json
import logging
import re
from pathlib import Path
from typing import Any

from src.errors.exceptions import ValidationError

logger = logging.getLogger(__name__)

# Allowed file extensions
_ALLOWED_SQL_EXTENSIONS = {".sql"}
_ALLOWED_TEMPLATE_EXTENSIONS = {".txt", ".html", ".jinja", ".j2", ".tmpl", ".xml", ".csv"}

# Simple token pattern: letters, digits, hyphens, underscores, dots
_TOKEN_RE = re.compile(r"^[A-Za-z0-9_\-\.]+$")
_REPORT_ID_RE = re.compile(r"^\d{7}$")


# ---------------------------------------------------------------------------
# Manifest-level
# ---------------------------------------------------------------------------

def validate_manifest_structure(manifest: Any, source: str = "manifest") -> list[dict]:
    """
    Validate top-level YAML manifest structure.
    Returns the bundles list on success; raises ValidationError otherwise.
    """
    if not isinstance(manifest, dict):
        raise ValidationError(
            f"{source}: YAML root must be a mapping, got {type(manifest).__name__}"
        )
    if "bundles" not in manifest:
        raise ValidationError(
            f"{source}: missing required top-level key 'bundles'"
        )
    bundles = manifest["bundles"]
    if bundles is None or not isinstance(bundles, list):
        raise ValidationError(
            f"{source}: 'bundles' must be a YAML list, got {type(bundles).__name__ if bundles is not None else 'null'}"
        )
    if len(bundles) == 0:
        raise ValidationError(
            f"{source}: 'bundles' list is empty — nothing to seed"
        )
    logger.debug("validator.manifest_ok source=%s bundles=%d", source, len(bundles))
    return bundles


# ---------------------------------------------------------------------------
# Bundle-level field validation
# ---------------------------------------------------------------------------

def validate_seed_bundle(bundle: Any, base_dir: Path, index: int = 0) -> dict:
    """
    Full validation of a single seed bundle dict.
    Returns a resolved copy with absolute file paths on success.
    """
    if not isinstance(bundle, dict):
        raise ValidationError(
            f"Bundle #{index}: must be a mapping/dict, got {type(bundle).__name__}"
        )

    # --- Required string fields ---
    required_string_keys = ["csi_id", "region", "regulation", "json_config", "sql_file"]
    errors = []
    for key in required_string_keys:
        val = bundle.get(key)
        if val is None:
            errors.append(f"missing required key '{key}'")
        elif not isinstance(val, str):
            errors.append(f"'{key}' must be a string, got {type(val).__name__}")
        elif not val.strip():
            errors.append(f"'{key}' must not be blank")

    if errors:
        raise ValidationError(
            f"Bundle #{index} (csi_id={bundle.get('csi_id', '?')}): validation failed — {'; '.join(errors)}",
            details={"bundle_index": index, "errors": errors},
        )

    # --- Token-format checks (no spaces, safe characters) ---
    _validate_token_field(bundle["csi_id"], "csi_id", index)
    _validate_token_field(bundle["region"], "region", index)
    _validate_token_field(bundle["regulation"], "regulation", index)

    # --- Optional report_id ---
    # Absent  → CREATE intent (service layer will generate one)
    # Present → MODIFY intent (service layer enforces the record must exist)
    # Must be a quoted 7-digit string in YAML: report_id: "0000001"
    report_id = bundle.get("report_id")
    if report_id is not None:
        if not isinstance(report_id, str):
            raise ValidationError(
                f"Bundle #{index}: 'report_id' must be a quoted 7-digit string "
                f"(e.g. report_id: \"0000001\"), got {type(report_id).__name__} '{report_id}'. "
                "Add quotes around the value in your seed.yaml."
            )
        if not _REPORT_ID_RE.match(report_id):
            raise ValidationError(
                f"Bundle #{index}: 'report_id' must be exactly 7 digits (e.g. '0000001'), got '{report_id}'"
            )

    # --- Resolve and validate file paths ---
    resolved = dict(bundle)
    resolved["report_id"] = report_id  # None = CREATE, 7-digit str = MODIFY target

    resolved["json_config"] = str(
        validate_file_exists(base_dir / bundle["json_config"], "JSON config", index=index)
    )
    resolved["sql_file"] = str(
        validate_file_exists(base_dir / bundle["sql_file"], "SQL file", index=index)
    )

    if bundle.get("template"):
        resolved["template"] = str(
            validate_file_exists(base_dir / bundle["template"], "Template", index=index)
        )
    else:
        resolved["template"] = None

    # --- Extension checks ---
    sql_ext = Path(resolved["sql_file"]).suffix.lower()
    if sql_ext not in _ALLOWED_SQL_EXTENSIONS:
        raise ValidationError(
            f"Bundle #{index}: SQL file must have extension {_ALLOWED_SQL_EXTENSIONS}, got '{sql_ext}'"
        )

    if resolved["template"]:
        tmpl_ext = Path(resolved["template"]).suffix.lower()
        if tmpl_ext not in _ALLOWED_TEMPLATE_EXTENSIONS:
            raise ValidationError(
                f"Bundle #{index}: Template file extension '{tmpl_ext}' is not in "
                f"allowed set {_ALLOWED_TEMPLATE_EXTENSIONS}"
            )

    # --- Content checks ---
    validate_sql_content(resolved["sql_file"], index)

    logger.debug(
        "validator.bundle_ok index=%d csi_id=%s regulation=%s region=%s",
        index, bundle["csi_id"], bundle["regulation"], bundle["region"],
    )
    return resolved


# ---------------------------------------------------------------------------
# File-level validators
# ---------------------------------------------------------------------------

def validate_file_exists(file_path: str | Path, label: str = "File", index: int = 0) -> Path:
    path = Path(file_path)
    if not path.exists():
        raise ValidationError(
            f"Bundle #{index}: {label} not found: {path}"
        )
    if not path.is_file():
        raise ValidationError(
            f"Bundle #{index}: {label} path is not a regular file: {path}"
        )
    if path.stat().st_size == 0:
        raise ValidationError(
            f"Bundle #{index}: {label} is empty (0 bytes): {path}"
        )
    return path.resolve()


def validate_json_config(config_path: str | Path, index: int = 0) -> dict[str, Any]:
    """Validate JSON config file: existence, extension, parseable JSON, required fields."""
    path = Path(config_path)

    if not path.exists():
        raise ValidationError(f"Bundle #{index}: JSON config not found: {path}")
    if path.suffix.lower() != ".json":
        raise ValidationError(
            f"Bundle #{index}: JSON config must be a .json file, got: '{path.suffix}'"
        )
    if path.stat().st_size == 0:
        raise ValidationError(f"Bundle #{index}: JSON config is empty: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)
    except json.JSONDecodeError as exc:
        raise ValidationError(
            f"Bundle #{index}: Invalid JSON in {path.name}: {exc}"
        ) from exc
    except UnicodeDecodeError as exc:
        raise ValidationError(
            f"Bundle #{index}: JSON config file is not valid UTF-8: {path.name}"
        ) from exc

    if not isinstance(config, dict):
        raise ValidationError(
            f"Bundle #{index}: JSON config root must be a JSON object, got {type(config).__name__}"
        )

    # Required fields
    required_fields = ["name", "outFileName"]
    missing = [f for f in required_fields if f not in config or not str(config[f]).strip()]
    if missing:
        raise ValidationError(
            f"Bundle #{index}: JSON config '{path.name}' missing required fields: {missing}",
            details={"file": str(path), "missing_fields": missing},
        )

    # Type checks
    for field in required_fields:
        if not isinstance(config[field], str):
            raise ValidationError(
                f"Bundle #{index}: JSON config field '{field}' must be a string, "
                f"got {type(config[field]).__name__}"
            )

    logger.debug(
        "validator.json_config_ok index=%d path=%s name=%s outFileName=%s",
        index, path.name, config["name"], config["outFileName"],
    )
    return config


def validate_sql_content(sql_path: str | Path, index: int = 0) -> None:
    """Validate SQL file: non-empty, UTF-8 readable, contains some non-whitespace content."""
    path = Path(sql_path)
    try:
        content = path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise ValidationError(
            f"Bundle #{index}: SQL file is not valid UTF-8: {path.name}"
        ) from exc

    if not content.strip():
        raise ValidationError(
            f"Bundle #{index}: SQL file contains only whitespace: {path.name}"
        )
    logger.debug("validator.sql_ok index=%d path=%s", index, path.name)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validate_token_field(value: str, field: str, index: int) -> None:
    """Ensure a field value uses only safe token characters (no whitespace, no special chars)."""
    if not _TOKEN_RE.match(value.strip()):
        raise ValidationError(
            f"Bundle #{index}: '{field}' contains invalid characters (only letters, digits, "
            f"hyphens, underscores, dots allowed): '{value}'"
        )
