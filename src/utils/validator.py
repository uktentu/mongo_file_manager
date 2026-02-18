"""Input validation for seed manifests, JSON configs, and file paths."""

import json
import logging
from pathlib import Path
from typing import Any

from src.errors.exceptions import ValidationError

logger = logging.getLogger(__name__)


def validate_json_config(config_path: str | Path) -> dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise ValidationError(f"JSON config file not found: {path}")
    if not path.suffix.lower() == ".json":
        raise ValidationError(f"JSON config must be a .json file, got: {path.suffix}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)
    except json.JSONDecodeError as exc:
        raise ValidationError(f"Invalid JSON in {path}: {exc}") from exc

    if not isinstance(config, dict):
        raise ValidationError(f"JSON config must be an object/dict, got: {type(config).__name__}")

    required_fields = ["name", "outFileName"]
    missing = [field for field in required_fields if field not in config or not config[field]]
    if missing:
        raise ValidationError(
            f"JSON config {path.name} is missing required fields: {missing}",
            details={"file": str(path), "missing_fields": missing},
        )

    logger.debug("validator.json_config_valid path=%s name=%s", path, config["name"])
    return config


def validate_file_exists(file_path: str | Path, label: str = "File") -> Path:
    path = Path(file_path)
    if not path.exists():
        raise ValidationError(f"{label} not found: {path}")
    if not path.is_file():
        raise ValidationError(f"{label} is not a file: {path}")
    return path.resolve()


def validate_seed_bundle(bundle: dict, base_dir: Path) -> dict:
    required_keys = ["csi_id", "region", "regulation", "json_config", "sql_file"]
    missing = [key for key in required_keys if key not in bundle or not bundle[key]]
    if missing:
        raise ValidationError(
            f"Seed bundle missing required keys: {missing}",
            details={"bundle": bundle, "missing_keys": missing},
        )

    resolved = dict(bundle)
    resolved["json_config"] = str(validate_file_exists(base_dir / bundle["json_config"], "JSON config"))
    resolved["sql_file"] = str(validate_file_exists(base_dir / bundle["sql_file"], "SQL file"))

    if bundle.get("template"):
        resolved["template"] = str(validate_file_exists(base_dir / bundle["template"], "Template"))
    else:
        resolved["template"] = None

    logger.debug("validator.bundle_valid csi_id=%s region=%s", bundle.get("csi_id"), bundle.get("region"))
    return resolved
