"""Deterministic unique ID builder for document bundles."""

import re


def _normalize(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9_\-]", "", value)
    return value


def build_unique_id(regulation: str, name: str, out_file_name: str, region: str) -> str:
    parts = {
        "regulation": _normalize(regulation),
        "name": _normalize(name),
        "out_file_name": _normalize(out_file_name),
        "region": _normalize(region),
    }

    for field_name, value in parts.items():
        if not value:
            raise ValueError(f"Cannot build unique_id: '{field_name}' is empty after normalization.")

    return f"{parts['regulation']}_{parts['name']}_{parts['out_file_name']}_{parts['region']}"
