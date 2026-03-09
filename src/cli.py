"""CLI entry point for the MongoDB Document Seeder."""

import sys

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from src.config.database import get_db
from src.config.logging_config import configure_logging
from src.errors.exceptions import SeederError

console = Console()


def setup_logging(level: str = "INFO"):
    """Wire up logging from Settings; --verbose overrides the level only."""
    configure_logging(level=level)


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging.")
def cli(verbose):
    """MongoDB Document Seeder — Seed, manage, and track regulatory document bundles."""
    setup_logging("DEBUG" if verbose else None)


@cli.command()
@click.argument("manifest", type=click.Path(exists=True))
def seed(manifest):
    """Seed all bundles from a YAML manifest file."""
    from src.services.seed_service import seed_from_manifest

    try:
        db = get_db()
        console.print(Panel(f"[bold blue]Seeding from:[/] {manifest}", title="🌱 Seeder", border_style="blue"))
        results = seed_from_manifest(manifest)

        # ── Summary counts table ──────────────────────────────
        summary = Table(title="Seed Summary", show_header=True, box=None)
        summary.add_column("Status", style="bold")
        summary.add_column("Count", justify="right")
        summary.add_row("✅ Created", str(results["created"]), style="green")
        summary.add_row("🔄 Updated", str(results["updated"]), style="yellow")
        summary.add_row("⏭️  Skipped", str(results["skipped"]), style="dim")
        summary.add_row("❌ Failed",  str(results["failed"]),  style="red")
        summary.add_row("[bold]Total[/]",   str(results["total"]))
        console.print(summary)

        # ── Per-bundle detail table ───────────────────────────
        if results.get("details"):
            detail_table = Table(
                title="Bundle Details",
                show_header=True, show_lines=True,
            )
            detail_table.add_column("#",          justify="right",  style="dim", width=3)
            detail_table.add_column("Label",      style="bold",     max_width=20)
            detail_table.add_column("Status",     justify="center", width=10)
            detail_table.add_column("Report ID",  style="cyan",     min_width=36)
            detail_table.add_column("Ver",        justify="right",  width=4)
            detail_table.add_column("Reason / Error")

            status_style = {
                "created": "[green]CREATED[/]",
                "updated": "[yellow]UPDATED[/]",
                "skipped": "[dim]SKIPPED[/]",
                "failed":  "[bold red]FAILED[/]",
            }

            for d in results["details"]:
                st = d["status"]
                note = d["error"] if st == "failed" else d.get("reason", "")
                detail_table.add_row(
                    str(d["index"] + 1),
                    d["label"],
                    status_style.get(st, st),
                    d.get("report_id") or "—",
                    str(d["version"]) if d.get("version") else "—",
                    note or "—",
                )
            console.print(detail_table)

        # ── Errors block ──────────────────────────────────────
        if results["errors"]:
            console.print("\n[bold red]Errors:[/]")
            for err in results["errors"]:
                console.print(f"  • {err}", style="red")

    except SeederError as exc:
        console.print(f"[bold red]Error:[/] {exc.message}", style="red")
        sys.exit(1)
    finally:
        db.close()


@cli.command()
@click.option("--csi-id", required=True, help="Project CSI ID.")
@click.option("--region", required=True, help="Region code.")
@click.option("--regulation", required=True, help="Regulation code.")
@click.option("--config", "json_config", required=True, type=click.Path(exists=True), help="Path to JSON config.")
@click.option("--sql", "sql_file", required=True, type=click.Path(exists=True), help="Path to SQL file.")
@click.option("--template", type=click.Path(exists=True), default=None, help="Path to template file.")
def create(csi_id, region, regulation, json_config, sql_file, template):
    """Create a single new record."""
    from src.services.seed_service import create_single_record

    db = None
    try:
        db = get_db()
        report_id = create_single_record(
            csi_id=csi_id, region=region, regulation=regulation,
            json_config_path=json_config, sql_file_path=sql_file, template_path=template,
        )
        console.print(Panel(
            f"[bold green]Record created![/]\n\n[bold]Report ID:[/] {report_id}",
            title="✅ Created", border_style="green",
        ))
    except SeederError as exc:
        console.print(f"[bold red]Error:[/] {exc.message}", style="red")
        sys.exit(1)
    finally:
        if db:
            db.close()


@cli.command()
@click.option("--csi-id", required=True, help="CSI ID of the record to modify.")
@click.option("--region", required=True, help="Region code.")
@click.option("--regulation", required=True, help="Regulation code.")
@click.option("--config", "json_config", required=True, type=click.Path(exists=True), help="JSON config (always required — filename is the lookup key; content updated if changed).")
@click.option("--sql", "sql_file", type=click.Path(exists=True), default=None, help="New SQL file.")
@click.option("--template", type=click.Path(exists=True), default=None, help="New template file.")
def modify(csi_id, region, regulation, json_config, sql_file, template):
    """Modify an existing active record identified by composite key.

    --config is always required: its filename identifies the record.
    If the config content has changed it will be updated too.
    """
    from src.services.seed_service import modify_record_by_composite_key

    db = None
    try:
        db = get_db()
        new_version = modify_record_by_composite_key(
            csi_id=csi_id, region=region, regulation=regulation,
            json_config_path=json_config,
            sql_file_path=sql_file,
            template_path=template,
        )
        console.print(Panel(
            f"[bold green]Record modified![/]\n\n"
            f"[bold]CSI ID:[/] {csi_id}  [bold]Region:[/] {region}  [bold]Regulation:[/] {regulation}\n"
            f"[bold]New Version:[/] {new_version}",
            title="🔄 Modified", border_style="yellow",
        ))
    except SeederError as exc:
        console.print(f"[bold red]Error:[/] {exc.message}", style="red")
        sys.exit(1)
    finally:
        if db:
            db.close()


@cli.command("list")
@click.option("--all", "show_all", is_flag=True, help="Show all records including inactive.")
def list_records(show_all):
    """List all active records."""
    from src.services.fetch_service import list_all_active

    try:
        db = get_db()
        if show_all:
            records = list(db.metadata_collection.find(
                {}, {"report_id": 1, "csi_id": 1, "region": 1, "regulation": 1,
                     "name": 1, "version": 1, "active": 1, "uploaded_at": 1},
            ))
        else:
            records = list_all_active()

        if not records:
            console.print("[dim]No records found.[/]")
            return

        table = Table(title="Document Records", show_header=True, show_lines=True)
        table.add_column("Report ID", style="cyan", min_width=36)
        table.add_column("CSI ID", style="bold")
        table.add_column("Region")
        table.add_column("Regulation")
        table.add_column("Name")
        table.add_column("Ver", justify="right")
        table.add_column("Active", justify="center")
        table.add_column("Uploaded", style="dim")

        for rec in records:
            active_str = "✅" if rec.get("active", False) else "❌"
            uploaded = rec.get("uploaded_at", "")
            if hasattr(uploaded, "strftime"):
                uploaded = uploaded.strftime("%Y-%m-%d %H:%M")
            table.add_row(
                rec.get("report_id", ""), rec.get("csi_id", ""), rec.get("region", ""),
                rec.get("regulation", ""), rec.get("name", ""), str(rec.get("version", "")),
                active_str, str(uploaded),
            )

        console.print(table)
    except SeederError as exc:
        console.print(f"[bold red]Error:[/] {exc.message}", style="red")
        sys.exit(1)
    finally:
        db.close()


@cli.command()
@click.option("--report-id", "report_id", required=True, help="Report ID to show history for.")
def history(report_id):
    """Show all versions of a record."""
    from src.services.fetch_service import fetch_version_history

    db = None
    try:
        db = get_db()
        records = fetch_version_history(report_id)

        console.print(Panel(
            f"[bold]Report ID:[/] {report_id}\n[bold]Total versions:[/] {len(records)}",
            title="📜 Version History", border_style="blue",
        ))

        table = Table(show_header=True, show_lines=True)
        table.add_column("Version", justify="right", style="bold")
        table.add_column("Active", justify="center")
        table.add_column("Uploaded", style="dim")
        table.add_column("Files")
        table.add_column("Audit")

        for rec in records:
            active_str = "[green]✅ ACTIVE[/]" if rec.get("active") else "[dim]❌ inactive[/]"
            uploaded = rec.get("uploaded_at", "")
            if hasattr(uploaded, "strftime"):
                uploaded = uploaded.strftime("%Y-%m-%d %H:%M:%S")

            orig = rec.get("original_files", {})
            files = f"config: {orig.get('json_config', 'n/a')}"
            if orig.get("template"):
                files += f"\ntemplate: {orig.get('template')}"
            files += f"\nsql: {orig.get('sql_file', 'n/a')}"

            audit_entries = rec.get("audit_log", [])
            audit_str = "\n".join(f"[{a.get('action', '')}] {a.get('details', '')}" for a in audit_entries) or "—"

            table.add_row(str(rec.get("version", "")), active_str, str(uploaded), files, audit_str)

        console.print(table)
    except SeederError as exc:
        console.print(f"[bold red]Error:[/] {exc.message}", style="red")
        sys.exit(1)
    finally:
        if db:
            db.close()


@cli.command()
@click.option("--report-id", "report_id", default=None, help="Fetch by Report ID.")
@click.option("--csi-id", default=None, help="Fetch by CSI ID.")
@click.option("--region", default=None, help="Fetch by region.")
@click.option("--regulation", default=None, help="Fetch by regulation.")
def fetch(report_id, csi_id, region, regulation):
    """Fetch records by key."""
    from src.services.fetch_service import (
        fetch_active_by_report_id, fetch_by_csi_id, fetch_by_region, fetch_by_regulation,
    )

    db = None
    try:
        db = get_db()
        if report_id:
            record = fetch_active_by_report_id(report_id)
            _display_record_detail(record)
        elif csi_id:
            _display_records_summary(fetch_by_csi_id(csi_id), f"CSI ID: {csi_id}")
        elif region:
            _display_records_summary(fetch_by_region(region), f"Region: {region}")
        elif regulation:
            _display_records_summary(fetch_by_regulation(regulation), f"Regulation: {regulation}")
        else:
            console.print("[bold red]Error:[/] Provide at least one filter.", style="red")
            sys.exit(1)
    except SeederError as exc:
        console.print(f"[bold red]Error:[/] {exc.message}", style="red")
        sys.exit(1)
    finally:
        if db:
            db.close()


@cli.command()
@click.option("--report-id", "report_id", required=True, help="Report ID to export.")
@click.option("--output", "-o", "output_dir", required=True, type=click.Path(), help="Output directory.")
@click.option("--version", "-V", "version", default=None, type=int, help="Specific version (default: active).")
@click.option("--no-verify", is_flag=True, help="Skip checksum verification.")
@click.option("--force", is_flag=True, help="Export even if checksums don't match.")
@click.option(
    "--file", "file_keys", multiple=True,
    type=click.Choice(["json_config", "sql_file", "template"], case_sensitive=False),
    help="File(s) to export. Repeat to export multiple. Omit to export all.",
)
def export(report_id, output_dir, version, no_verify, force, file_keys):
    """Export a bundle's files from MongoDB back to disk.

    Examples:

    \b
      # Export all files (default)
      python -m src.cli export --report-id a1b2c3d4-e5f6-7890-abcd-ef1234567890 -o ./out

    \b
      # Export only the SQL file
      python -m src.cli export --report-id a1b2c3d4-e5f6-7890-abcd-ef1234567890 -o ./out --file sql_file

    \b
      # Export SQL + template only
      python -m src.cli export --report-id a1b2c3d4-e5f6-7890-abcd-ef1234567890 -o ./out --file sql_file --file template
    """
    from src.services.export_service import export_bundle

    db = None
    try:
        db = get_db()
        selected_files = set(file_keys) if file_keys else None
        result = export_bundle(
            report_id=report_id, output_dir=output_dir, version=version,
            verify_checksums=not no_verify, force=force, files=selected_files,
        )

        files_info = "\n".join(f"  [bold]{k}:[/] {v}" for k, v in result.get("files", {}).items())
        checksums_info = "\n".join(
            f"  [bold]{k}:[/] {'✅ verified' if v else '❌ MISMATCH'}"
            for k, v in result.get("checksum_verified", {}).items()
        )

        console.print(Panel(
            f"[bold green]Export complete![/]\n\n"
            f"[bold]Report ID:[/] {report_id}\n[bold]Version:[/] {result.get('version')}\n[bold]Output:[/] {output_dir}\n\n"
            f"[bold underline]Files:[/]\n{files_info}\n\n"
            f"[bold underline]Checksums:[/]\n{checksums_info or '  (skipped)'}",
            title="📦 Export", border_style="green",
        ))
    except SeederError as exc:
        console.print(f"[bold red]Error:[/] {exc.message}", style="red")
        sys.exit(1)
    finally:
        if db:
            db.close()


@cli.command()
@click.option("--report-id", "report_id", default=None, help="Purge old versions of a specific record.")
@click.option("--all", "purge_all", is_flag=True, help="Purge old versions across all records.")
@click.option("--keep", default=3, type=int, show_default=True, help="Versions to keep.")
@click.option("--max-age-days", default=None, type=int, help="Purge inactive records older than N days.")
@click.option("--dry-run", is_flag=True, help="Preview without deleting.")
def cleanup(report_id, purge_all, keep, max_age_days, dry_run):
    """Purge old versions to manage storage growth."""
    from src.services.cleanup_service import purge_old_versions, purge_all_old_versions, purge_by_age

    db = None
    try:
        db = get_db()

        if max_age_days:
            result = purge_by_age(max_age_days=max_age_days, dry_run=dry_run)
            console.print(Panel(
                f"[bold]{'[DRY RUN] ' if dry_run else ''}Age-based cleanup[/]\n\n"
                f"Records older than {max_age_days} days purged: [bold]{result['purged']}[/]",
                title="🧹 Cleanup", border_style="yellow",
            ))
        elif report_id:
            result = purge_old_versions(report_id, keep_versions=keep, dry_run=dry_run)
            console.print(Panel(
                f"[bold]{'[DRY RUN] ' if dry_run else ''}Version cleanup for: {report_id}[/]\n\n"
                f"Purged: [bold]{result['purged']}[/]\nKept: [bold]{result['kept']}[/]",
                title="🧹 Cleanup", border_style="yellow",
            ))
        elif purge_all:
            result = purge_all_old_versions(keep_versions=keep, dry_run=dry_run)
            console.print(Panel(
                f"[bold]{'[DRY RUN] ' if dry_run else ''}Global cleanup[/]\n\n"
                f"Records processed: [bold]{result['records_processed']}[/]\n"
                f"Total purged: [bold]{result['total_purged']}[/]",
                title="🧹 Cleanup", border_style="yellow",
            ))
        else:
            console.print("[bold red]Error:[/] Specify --report-id, --all, or --max-age-days.", style="red")
            sys.exit(1)
    except SeederError as exc:
        console.print(f"[bold red]Error:[/] {exc.message}", style="red")
        sys.exit(1)
    finally:
        if db:
            db.close()


def _display_record_detail(record: dict):
    orig = record.get("original_files", {})
    checksums = record.get("checksums", {})
    sizes = record.get("file_sizes", {})

    console.print(Panel(
        f"[bold]Report ID:[/]   {record.get('report_id')}\n"
        f"[bold]CSI ID:[/]      {record.get('csi_id')}\n"
        f"[bold]Region:[/]      {record.get('region')}\n"
        f"[bold]Regulation:[/]  {record.get('regulation')}\n"
        f"[bold]Name:[/]        {record.get('name')}\n"
        f"[bold]Version:[/]     {record.get('version')}\n"
        f"[bold]Active:[/]      {record.get('active')}\n"
        f"\n[bold underline]Files:[/]\n"
        f"  JSON Config:  {orig.get('json_config')} ({sizes.get('json_config', 0)} bytes)\n"
        f"  Template:     {orig.get('template', 'N/A')} ({sizes.get('template', 0)} bytes)\n"
        f"  SQL File:     {orig.get('sql_file')} ({sizes.get('sql_file', 0)} bytes)\n"
        f"\n[bold underline]Checksums:[/]\n"
        f"  JSON Config:  {checksums.get('json_config', 'N/A')}\n"
        f"  Template:     {checksums.get('template', 'N/A')}\n"
        f"  SQL File:     {checksums.get('sql_file', 'N/A')}",
        title="📄 Record Detail", border_style="cyan",
    ))


def _display_records_summary(records: list, label: str):
    if not records:
        console.print(f"[dim]No records found for {label}.[/]")
        return

    console.print(f"\n[bold]Results for {label}:[/] {len(records)} record(s)\n")

    table = Table(show_header=True, show_lines=True)
    table.add_column("Report ID", style="cyan", min_width=36)
    table.add_column("CSI ID", style="bold")
    table.add_column("Region")
    table.add_column("Regulation")
    table.add_column("Ver", justify="right")

    for rec in records:
        table.add_row(
            rec.get("report_id", ""), rec.get("csi_id", ""),
            rec.get("region", ""), rec.get("regulation", ""), str(rec.get("version", "")),
        )

    console.print(table)


if __name__ == "__main__":
    cli()
