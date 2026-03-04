#!/usr/bin/env python3
"""
integration/seed_caller.py
──────────────────────────
Template script for external regulation repos to call the central
MongoDB Document Seeder via HTTP API.

Copy this file into your regulation repo, configure the constants at
the top, then call it from your CI/CD pipeline:

    python integration/seed_caller.py seeds/seed.yaml

Or for a single bundle:

    python integration/seed_caller.py --single \
        --csi-id CSI-001 --region APAC --regulation MAS-TRM \
        --config configs/mas_trm_report.json \
        --sql sql/mas_trm_query.sql
"""

import argparse
import base64
import json
import os
import sys
from pathlib import Path

import urllib.request
import urllib.error

# ─── Configuration ────────────────────────────────────────────────────────────
# Set via environment variables in your pipeline:
#   SEEDER_BASE_URL  — base URL of the central seeder API
#   SEEDER_API_KEY   — API key configured on the seeder server
SEEDER_BASE_URL = os.getenv("SEEDER_BASE_URL", "http://localhost:8000")
SEEDER_API_KEY = os.getenv("SEEDER_API_KEY", "")
# ─────────────────────────────────────────────────────────────────────────────


def _headers() -> dict:
    h = {"Content-Type": "application/json"}
    if SEEDER_API_KEY:
        h["X-API-Key"] = SEEDER_API_KEY
    return h


def _b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode()


def _post(endpoint: str, payload: dict) -> dict:
    url = f"{SEEDER_BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    body = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=body, headers=_headers(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        print(f"[seeder] HTTP {exc.code} — {body}", file=sys.stderr)
        sys.exit(1)


def seed_from_yaml(manifest_path: Path) -> None:
    """Read a seed.yaml and POST each bundle to /api/seed/manifest."""
    try:
        import yaml
    except ImportError:
        print("[seeder] PyYAML is required: pip install pyyaml", file=sys.stderr)
        sys.exit(1)

    with open(manifest_path) as f:
        data = yaml.safe_load(f)

    raw_bundles = (data or {}).get("bundles") or []
    real_bundles = [b for b in raw_bundles if isinstance(b, dict) and b.get("csi_id")]

    if not real_bundles:
        print("[seeder] No active bundles in manifest — nothing to seed.")
        return

    base_dir = manifest_path.parent
    api_bundles = []
    for i, b in enumerate(real_bundles):
        json_path = base_dir / b["json_config"]
        sql_path = base_dir / b["sql_file"]
        tmpl_path = (base_dir / b["template"]) if b.get("template") else None

        entry = {
            "csi_id": b["csi_id"],
            "region": b["region"],
            "regulation": b["regulation"],
            "json_config_filename": json_path.name,
            "json_config_content": _b64(json_path),
            "sql_file_filename": sql_path.name,
            "sql_file_content": _b64(sql_path),
        }
        if tmpl_path:
            entry["template_filename"] = tmpl_path.name
            entry["template_content"] = _b64(tmpl_path)
        if b.get("report_id"):
            entry["report_id"] = b["report_id"]

        api_bundles.append(entry)

    print(f"[seeder] Sending {len(api_bundles)} bundle(s) to {SEEDER_BASE_URL}/api/seed/manifest ...")
    result = _post("/api/seed/manifest", {"bundles": api_bundles})

    print(f"[seeder] Done — created={result['created']}  updated={result['updated']}  "
          f"skipped={result['skipped']}  failed={result['failed']}")

    for d in result.get("details", []):
        status_icon = {"created": "✅", "updated": "🔄", "skipped": "⏭", "failed": "❌"}.get(d["status"], "?")
        rid = d.get("report_id") or "—"
        ver = d.get("version") or "—"
        note = d.get("error") or d.get("reason") or ""
        print(f"  {status_icon} [{d['index']+1}] {d['label']}  report_id={rid}  v{ver}  {note}")

    if result.get("errors"):
        print("\n[seeder] Errors:", file=sys.stderr)
        for err in result["errors"]:
            print(f"  • {err}", file=sys.stderr)

    if result["failed"] > 0:
        sys.exit(1)


def seed_single(args) -> None:
    """POST a single bundle with explicit file paths."""
    json_path = Path(args.config)
    sql_path = Path(args.sql)
    tmpl_path = Path(args.template) if args.template else None

    payload = {
        "csi_id": args.csi_id,
        "region": args.region,
        "regulation": args.regulation,
        "json_config_filename": json_path.name,
        "json_config_content": _b64(json_path),
        "sql_file_filename": sql_path.name,
        "sql_file_content": _b64(sql_path),
    }
    if tmpl_path:
        payload["template_filename"] = tmpl_path.name
        payload["template_content"] = _b64(tmpl_path)
    if args.report_id:
        payload["report_id"] = args.report_id

    print(f"[seeder] Sending bundle to {SEEDER_BASE_URL}/api/seed/bundle ...")
    result = _post("/api/seed/bundle", payload)

    status_icon = {"created": "✅", "updated": "🔄", "skipped": "⏭"}.get(result.get("status"), "❓")
    print(f"  {status_icon} {result.get('status', '?').upper()}  "
          f"report_id={result.get('report_id')}  v{result.get('version')}  "
          f"({result.get('reason', '')})")


def main():
    parser = argparse.ArgumentParser(description="Call the central MongoDB Document Seeder from an external repo.")
    sub = parser.add_subparsers(dest="command")

    # manifest-based seeding
    manifest_parser = sub.add_parser("manifest", help="Seed from a seed.yaml manifest file")
    manifest_parser.add_argument("manifest_path", help="Path to seed.yaml")

    # single-bundle seeding
    single_parser = sub.add_parser("bundle", help="Seed a single bundle inline")
    single_parser.add_argument("--csi-id", required=True)
    single_parser.add_argument("--region", required=True)
    single_parser.add_argument("--regulation", required=True)
    single_parser.add_argument("--config", required=True, help="Path to JSON config file")
    single_parser.add_argument("--sql", required=True, help="Path to SQL file")
    single_parser.add_argument("--template", default=None, help="Path to template file (optional)")
    single_parser.add_argument("--report-id", dest="report_id", default=None, help="Existing report_id to update")

    args = parser.parse_args()

    if args.command == "manifest":
        seed_from_yaml(Path(args.manifest_path))
    elif args.command == "bundle":
        seed_single(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
