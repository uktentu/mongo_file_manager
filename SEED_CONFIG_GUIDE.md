# Seed Configuration Guide

> Complete reference for setting up regulation seed configs for the MongoDB Document Seeder.

---

## Table of Contents

1. [Overview](#overview)
2. [Directory Structure](#directory-structure)
3. [Seed Manifest (seed.yaml)](#seed-manifest)
4. [JSON Config File](#json-config-file)
5. [SQL File](#sql-file)
6. [Template File (Optional)](#template-file)
7. [Routing Rules](#routing-rules)
8. [Field Reference](#field-reference)
9. [Examples](#examples)
10. [Validation Rules](#validation-rules)
11. [Troubleshooting](#troubleshooting)

---

## Overview

The MongoDB Document Seeder manages regulatory document bundles. Each bundle
consists of three files:

| File | Required | Purpose |
|------|----------|---------|
| **JSON Config** | ✅ Yes | Report configuration — defines report name and metadata |
| **SQL File** | ✅ Yes | Query template used by ReportGen to fetch data |
| **Template** | ❌ Optional | Output template (HTML/TXT/CSV/Jinja2) for report rendering |

Bundles are identified by a **composite key**:
```
(csi_id, region, regulation, json_config filename)
```

You **never** supply a `report_id` — the system generates an internal UUID automatically.

---

## Directory Structure

Organize your regulation repo like this:

```
your-regulation-repo/
├── seeds/
│   ├── seed.yaml                    # ← Manifest file (defines all bundles)
│   ├── configs/
│   │   ├── mas_trm_report.json      # ← JSON config files
│   │   └── gdpr_audit_report.json
│   ├── sql/
│   │   ├── mas_trm_query.sql        # ← SQL query files
│   │   └── gdpr_audit_query.sql
│   └── templates/                   # ← Template files (optional)
│       ├── mas_trm_template.html
│       └── gdpr_audit_template.csv
└── integration/
    └── seed_caller.py               # ← CI/CD integration script
```

> **All file paths in `seed.yaml` are relative to the manifest file's directory.**

---

## Seed Manifest

The `seed.yaml` file declares all bundles to seed. The system processes
them automatically: new bundles are **created**, unchanged bundles are
**skipped**, and modified bundles get a **new version**.

### Minimal Example

```yaml
bundles:
  - csi_id: "CSI-001"
    region: "APAC"
    regulation: "MAS-TRM"
    json_config: "configs/mas_trm_report.json"
    sql_file: "sql/mas_trm_query.sql"
```

### Full Example (with template)

```yaml
bundles:
  - csi_id: "CSI-001"
    region: "APAC"
    regulation: "MAS-TRM"
    json_config: "configs/mas_trm_report.json"
    sql_file: "sql/mas_trm_query.sql"
    template: "templates/mas_trm_template.html"

  - csi_id: "CSI-002"
    region: "EU"
    regulation: "GDPR"
    json_config: "configs/gdpr_audit_report.json"
    sql_file: "sql/gdpr_audit_query.sql"
    template: "templates/gdpr_audit_template.csv"
```

### Multiple Configs per Regulation

The same `csi_id + region + regulation` can have **multiple bundles** as long
as each uses a **different json_config filename**:

```yaml
bundles:
  # Two different reports for the same regulation
  - csi_id: "CSI-001"
    region: "APAC"
    regulation: "MAS-TRM"
    json_config: "configs/mas_trm_summary.json"      # ← Different filename
    sql_file: "sql/mas_trm_summary.sql"

  - csi_id: "CSI-001"
    region: "APAC"
    regulation: "MAS-TRM"
    json_config: "configs/mas_trm_detailed.json"      # ← Different filename
    sql_file: "sql/mas_trm_detailed.sql"
```

---

## JSON Config File

Each JSON config **must** be a `.json` file with at minimum:

```json
{
  "report": {
    "name": "MAS TRM Compliance Report"
  }
}
```

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `report` | Object | Top-level report configuration block |
| `report.name` | String | Human-readable report name (non-empty) |

### Optional Fields (used by ReportGen)

You can add any additional fields your ReportGen project needs:

```json
{
  "report": {
    "name": "MAS TRM Compliance Report",
    "description": "Quarterly technology risk management assessment",
    "output_format": "pdf",
    "schedule": "quarterly"
  },
  "data_source": {
    "database": "analytics_db",
    "schema": "compliance"
  },
  "columns": [
    {"name": "control_id", "type": "string", "label": "Control ID"},
    {"name": "status", "type": "enum", "values": ["pass", "fail", "na"]},
    {"name": "evidence", "type": "text", "label": "Evidence"}
  ],
  "filters": {
    "date_range": true,
    "department": true
  }
}
```

> **The Seeder validates only `report.name` — all other fields are passed
> through untouched for ReportGen to consume.**

---

## SQL File

Must be a `.sql` file containing valid, non-empty SQL content (UTF-8 encoded).

```sql
SELECT
    c.control_id,
    c.control_name,
    a.assessment_date,
    a.status,
    a.evidence_notes
FROM compliance_controls c
JOIN assessments a ON c.id = a.control_id
WHERE a.assessment_date >= :start_date
  AND a.assessment_date <= :end_date
  AND c.regulation = :regulation
ORDER BY c.control_id;
```

> **Use parameterized queries (`:param` style) — ReportGen will substitute
> values at runtime.**

---

## Template File

Optional. Allowed extensions: `.txt`, `.html`, `.jinja`, `.j2`, `.tmpl`, `.xml`, `.csv`

```html
<!DOCTYPE html>
<html>
<head><title>{{ report.name }}</title></head>
<body>
  <h1>{{ report.name }}</h1>
  <p>Generated: {{ generated_at }}</p>
  <table>
    {% for row in data %}
    <tr>
      <td>{{ row.control_id }}</td>
      <td>{{ row.status }}</td>
    </tr>
    {% endfor %}
  </table>
</body>
</html>
```

---

## Routing Rules

The seeder uses **automatic routing** — you never need to specify whether
to create or update. The logic is:

```
For each bundle in the manifest:

1. Look up existing active record by composite key:
   (csi_id + region + regulation + json_config filename)

2. If NO active record found:
   → CREATE (new UUID assigned, version=1)

3. If active record found:
   a. Compute SHA-256 checksums for all files
   b. Compare against stored checksums
   c. ALL checksums match → SKIP (idempotent, no writes)
   d. ANY checksum changed → MODIFY (new version created, old deactivated)
```

This makes re-running `seed.yaml` completely **safe and idempotent**.

---

## Field Reference

### Bundle Fields

| Field | Required | Type | Constraints |
|-------|----------|------|-------------|
| `csi_id` | ✅ | String | Letters, digits, hyphens, underscores, dots only |
| `region` | ✅ | String | Letters, digits, hyphens, underscores, dots only |
| `regulation` | ✅ | String | Letters, digits, hyphens, underscores, dots only |
| `json_config` | ✅ | String | Relative path to `.json` file |
| `sql_file` | ✅ | String | Relative path to `.sql` file |
| `template` | ❌ | String | Relative path to template file |

### Allowed Characters for Identifiers

`csi_id`, `region`, and `regulation` must match: `^[A-Za-z0-9_\-\.]+$`

✅ Valid: `CSI-001`, `APAC`, `MAS-TRM`, `eu.gdpr.2024`
❌ Invalid: `CSI 001` (space), `APAC/EMEA` (slash), `MAS TRM` (space)

---

## Validation Rules

The seeder validates in this order:

1. **Manifest structure** — YAML root must have `bundles` list
2. **Bundle fields** — All required fields present and non-empty
3. **Identifier format** — Safe characters only (no spaces, no specials)
4. **File existence** — All referenced files must exist and be non-empty
5. **Path traversal** — File paths cannot escape the manifest directory
6. **File extensions** — `.json` for config, `.sql` for queries
7. **JSON parsing** — Config must be valid JSON with `report.name`
8. **SQL content** — Must be valid UTF-8 with non-whitespace content

> All bundles are pre-validated **before** any database operations begin.
> If bundle #3 has a validation error, bundles #1 and #2 are still processed.

---

## Examples

### Seeding via CLI

```bash
# From the regulation repo root:
python -m src.cli seed seeds/seed.yaml
```

### Seeding via API (from CI/CD)

```bash
# Using the integration script:
python integration/seed_caller.py manifest seeds/seed.yaml

# Or a single bundle:
python integration/seed_caller.py bundle \
  --csi-id CSI-001 \
  --region APAC \
  --regulation MAS-TRM \
  --config seeds/configs/mas_trm_report.json \
  --sql seeds/sql/mas_trm_query.sql
```

### Environment Variables for CI/CD

```bash
export SEEDER_BASE_URL=http://seeder.internal:8000
export SEEDER_API_KEY=your-secure-api-key
python integration/seed_caller.py manifest seeds/seed.yaml
```

---

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `missing required key 'csi_id'` | Bundle is missing a required field | Add the field to seed.yaml |
| `contains invalid characters` | Identifier has spaces or special chars | Use only `A-Z a-z 0-9 _ - .` |
| `JSON config not found` | File path is wrong or file doesn't exist | Check relative path from seed.yaml |
| `missing required field 'report.name'` | JSON config is missing `report.name` | Add `{"report": {"name": "..."}}` |
| `SQL file contains only whitespace` | SQL file is empty or whitespace-only | Add valid SQL content |
| `path escapes the base directory` | Path traversal attempt (`../..`) | Use only relative paths within the seeds directory |
| `An active record already exists` | Using `create` when record exists | Use `modify` or the manifest (auto-routes) |
