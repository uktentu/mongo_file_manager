"""Full functionality test — Atlas MongoDB."""
import sys, os, json, tempfile, hashlib
from pathlib import Path

PASS = FAIL = 0
def ok(name, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  ✅ {name}")
    else: FAIL += 1; print(f"  ❌ {name} — {detail}")

print(f"Python: {sys.version}\n")

# 1. IMPORTS
print("=" * 50); print("1. IMPORTS"); print("=" * 50)
try:
    from src.errors.exceptions import SeederError, ValidationError, FileNotFoundError_, DatabaseError, GridFSError, ChecksumMismatchError, RecordNotFoundError, DuplicateRecordError
    ok("exceptions", True)
except Exception as e: ok("exceptions", False, str(e))
try:
    from src.models.schemas import AuditEntry, OriginalFiles, FileContents, Checksums, FileSizes, MetadataDocument
    ok("schemas", True)
except Exception as e: ok("schemas", False, str(e))
try:
    from src.utils.checksum import compute_file_checksum, compute_bytes_checksum, verify_checksum
    ok("checksum", True)
except Exception as e: ok("checksum", False, str(e))
try:
    from src.utils.report_id import generate_report_id
    ok("report_id", True)
except Exception as e: ok("report_id", False, str(e))
try:
    from src.utils.retry import retry_on_failure
    ok("retry", True)
except Exception as e: ok("retry", False, str(e))
try:
    from src.utils.validator import validate_manifest_structure, validate_seed_bundle, validate_json_config, validate_sql_content, validate_file_exists
    ok("validator", True)
except Exception as e: ok("validator", False, str(e))
try:
    from src.config.settings import Settings, get_settings, SettingsError
    ok("settings", True)
except Exception as e: ok("settings", False, str(e))
try:
    from src.config.logging_config import configure_logging
    ok("logging_config", True)
except Exception as e: ok("logging_config", False, str(e))
try:
    from src.sdk.client import ReportGenClient, ReportGenHTTPClient
    ok("sdk.client", True)
except Exception as e: ok("sdk.client", False, str(e))
try:
    from src.config.database import get_db, reset_db
    ok("database", True)
except Exception as e: ok("database", False, str(e))
try:
    from src.services.seed_service import seed_from_manifest
    from src.services.export_service import export_bundle
    from src.services.fetch_service import fetch_active_by_report_id, fetch_by_csi_id, fetch_by_region, fetch_by_regulation, list_all_active, fetch_version_history
    from src.services.cleanup_service import purge_old_versions, purge_all_old_versions, purge_by_age
    from src.services.gridfs_service import GridFSOrphanTracker
    from src.services.audit_service import create_audit_entry
    ok("all services", True)
except Exception as e: ok("all services", False, str(e))

# 2. SETTINGS
print("\n" + "=" * 50); print("2. SETTINGS"); print("=" * 50)
s = get_settings()
ok("settings loads", s is not None)
ok("mongo_uri has atlas", "mongodb+srv" in s.mongo_uri)
ok("mongo_db_name", s.mongo_db_name == "doc_management")

# 3. OFFLINE UTILS
print("\n" + "=" * 50); print("3. CHECKSUM / REPORT_ID / RETRY"); print("=" * 50)
with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w") as f:
    f.write("test content"); tmp = f.name
cs = compute_file_checksum(tmp)
ok("file checksum", cs.startswith("sha256:"))
ok("bytes checksum match", compute_bytes_checksum(b"test content") == cs)
ok("verify pass", verify_checksum(tmp, cs))
ok("verify fail", not verify_checksum(tmp, "sha256:wrong"))
os.unlink(tmp)
rid1, rid2 = generate_report_id(), generate_report_id()
ok("UUID format", len(rid1) == 36 and "-" in rid1)
ok("unique", rid1 != rid2)

# 4. VALIDATORS
print("\n" + "=" * 50); print("4. VALIDATORS"); print("=" * 50)
ok("valid manifest", len(validate_manifest_structure({"bundles": [{"x": 1}]})) == 1)
try: validate_manifest_structure("bad"); ok("reject non-dict", False)
except ValidationError: ok("reject non-dict", True)
try: validate_manifest_structure({"bundles": []}); ok("reject empty", False)
except ValidationError: ok("reject empty", True)

with tempfile.TemporaryDirectory() as td:
    (Path(td)/"g.json").write_text(json.dumps({"report":{"name":"R"}}))
    config = validate_json_config(str(Path(td)/"g.json"))
    ok("valid json config", config["report"]["name"] == "R")
    (Path(td)/"b.json").write_text(json.dumps({"report":{}}))
    try: validate_json_config(str(Path(td)/"b.json")); ok("reject no name", False)
    except ValidationError: ok("reject no name", True)
    (Path(td)/"q.sql").write_text("SELECT 1;")
    validate_sql_content(str(Path(td)/"q.sql"))
    ok("valid SQL", True)
    (Path(td)/"e.sql").write_text("   \n ")
    try: validate_sql_content(str(Path(td)/"e.sql")); ok("reject empty SQL", False)
    except ValidationError: ok("reject empty SQL", True)

# Path traversal
with tempfile.TemporaryDirectory() as td:
    base = Path(td); (base/"ok.txt").write_text("x")
    ok("legit path OK", validate_file_exists(str(base/"ok.txt"), base_dir=base).exists())
    try: validate_file_exists(str(base/".."/".."/".."/"etc"/"passwd"), base_dir=base); ok("block traversal", False)
    except ValidationError: ok("block traversal", True)

# Bundle validation
with tempfile.TemporaryDirectory() as td:
    base = Path(td)
    (base/"c.json").write_text(json.dumps({"report":{"name":"R"}}))
    (base/"q.sql").write_text("SELECT 1;")
    r = validate_seed_bundle({"csi_id":"A","region":"B","regulation":"C","json_config":"c.json","sql_file":"q.sql"}, base)
    ok("valid bundle", "json_config" in r)
    try: validate_seed_bundle({"region":"X"}, base); ok("reject missing", False)
    except ValidationError: ok("reject missing", True)

# 5. SCHEMAS
print("\n" + "=" * 50); print("5. SCHEMAS"); print("=" * 50)
from datetime import datetime, timezone
a = AuditEntry(action="CREATED", details="test")
ok("AuditEntry", a.action == "CREATED" and a.timestamp is not None)
o = OriginalFiles(json_config="c.json", sql_file="q.sql")
ok("OriginalFiles", o.template is None)
m = MetadataDocument(report_id="x", csi_id="A", region="B", regulation="C", name="N",
    original_files=o, file_contents=FileContents(json_config_id="1",sql_file_id="2"),
    checksums=Checksums(json_config="a",sql_file="b"), file_sizes=FileSizes(json_config=1,sql_file=2))
ok("MetadataDocument", m.version == 1 and m.active)
ok("to_mongo_dict", "report_id" in m.to_mongo_dict())
e = create_audit_entry("MODIFIED", "v1→v2")
ok("audit_service", e["action"] == "MODIFIED")

# 6. FASTAPI APP
print("\n" + "=" * 50); print("6. FASTAPI APP + SECURITY"); print("=" * 50)
from src.api import app, _sanitize_filename, verify_api_key
import inspect
routes = [r.path for r in app.routes if hasattr(r,'path') and hasattr(r,'methods')]
ok("health route", "/api/health" in routes)
ok("records route", "/api/records" in routes)
ok("seed/bundle", "/api/seed/bundle" in routes)
ok("seed/manifest", "/api/seed/manifest" in routes)
ok("file stream (ReportGen)", "/api/records/{report_id}/files/{file_key}" in routes)
ok("file list (ReportGen)", "/api/records/{report_id}/files" in routes)
ok("sanitize strips ..", ".." not in _sanitize_filename("../../etc/passwd"))
ok("sanitize caps length", len(_sanitize_filename("A"*500)) <= 200)
ok("timing-safe auth", "compare_digest" in inspect.getsource(verify_api_key))

# 7. LIVE DB
print("\n" + "=" * 50); print("7. LIVE MONGODB (ATLAS)"); print("=" * 50)
configure_logging()
try:
    db = get_db()
    db.client.admin.command("ping")
    ok("Atlas connection", True)
    ok("metadata collection", db.metadata_collection is not None)
    ok("GridFS", db.fs is not None)
    ok("transactions", isinstance(db.supports_transactions, bool))
except Exception as e:
    ok("Atlas connection", False, str(e))
    print("  ⚠️  Skipping live tests"); print(f"\nRESULTS: {PASS} passed, {FAIL} failed"); sys.exit(1)

# 8. SEED
print("\n" + "=" * 50); print("8. SEED FROM MANIFEST"); print("=" * 50)
results = seed_from_manifest("seeds/seed.yaml")
ok("seed runs", results is not None)
ok("no failures", results.get("failed",0) == 0, f"failed={results.get('failed')}")
total = results["created"] + results["updated"] + results["skipped"]
ok("all processed", total == results["total"])
RID = results["details"][0]["report_id"] if results["details"] else None
ok("has report_id", RID is not None)
print(f"  📌 report_id={RID}")

# 9. FETCH
print("\n" + "=" * 50); print("9. FETCH SERVICE"); print("=" * 50)
rec = fetch_active_by_report_id(RID)
ok("fetch by report_id", rec["report_id"] == RID)
ok("has name", bool(rec.get("name")))
ok("is active", rec.get("active") is True)
ok("fetch by csi_id", len(fetch_by_csi_id(rec["csi_id"])) >= 1)
ok("fetch by region", len(fetch_by_region(rec["region"])) >= 1)
ok("fetch by regulation", len(fetch_by_regulation(rec["regulation"])) >= 1)
ok("list_all_active", len(list_all_active()) >= 1)
ok("version history", len(fetch_version_history(RID)) >= 1)

# 10. EXPORT
print("\n" + "=" * 50); print("10. EXPORT SERVICE"); print("=" * 50)
with tempfile.TemporaryDirectory() as td:
    r = export_bundle(RID, td)
    ok("export runs", r is not None)
    ok("has files", len(r.get("files",{})) >= 2)
    for k, v in r.get("files",{}).items():
        if not str(v).startswith("ERROR"):
            ok(f"  {k} exists", Path(v).exists())
    ok("checksums OK", all(v for v in r.get("checksum_verified",{}).values()))

# 11. SDK
print("\n" + "=" * 50); print("11. SDK DIRECT CLIENT"); print("=" * 50)
client = ReportGenClient.from_env()
ok("SDK get_record", client.get_record(RID) is not None)
jb = client.get_file_bytes(RID, "json_config")
ok("SDK json bytes", len(jb) > 0)
cfg = client.get_json_config(RID)
ok("SDK json_config parsed", "report" in cfg)
sql = client.get_sql_query(RID)
ok("SDK sql_query", len(sql.strip()) > 0)
af = client.get_all_files(RID)
ok("SDK all_files", len(af) >= 2)
lr = client.list_records(regulation=rec["regulation"])
ok("SDK list_records", len(lr) >= 1)
with tempfile.TemporaryDirectory() as td:
    ef = client.export_file(RID, "sql_file", td)
    ok("SDK export_file", ef.exists())
client.close()

# 12. CLEANUP
print("\n" + "=" * 50); print("12. CLEANUP (DRY RUN)"); print("=" * 50)
ok("purge_old dry", purge_old_versions(RID, dry_run=True).get("dry_run"))
ok("purge_all dry", purge_all_old_versions(dry_run=True).get("dry_run"))
ok("purge_age dry", purge_by_age(365, dry_run=True).get("dry_run"))

# 13. IDEMPOTENT
print("\n" + "=" * 50); print("13. IDEMPOTENT RE-SEED"); print("=" * 50)
r2 = seed_from_manifest("seeds/seed.yaml")
ok("skipped on re-run", r2.get("skipped",0) > 0)
ok("no new creates", r2.get("created",0) == 0)

reset_db()
print(f"\n{'='*50}\nRESULTS: {PASS} passed, {FAIL} failed\n{'='*50}")
sys.exit(1 if FAIL > 0 else 0)
