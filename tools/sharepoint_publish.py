"""Publish generated report workbooks into the locally synced SharePoint folder.

This is intentionally filesystem-first. On this machine, the SharePoint library
already syncs into `C:\\Users\\Sunco\\Sunco Lighting\\Product - Manny Tools`.
Publishing to that folder gets the output into SharePoint without taking a hard
dependency on Graph credentials.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Any


DEFAULT_SYNC_ROOT = Path(r"C:\Users\Sunco\Sunco Lighting\Product - Manny Tools\PRD Research")
DEFAULT_REPORTS_DIRNAME = "Research Reports"


def reports_root_from_env_or_default() -> Path:
    """Return the default synced SharePoint reports root."""
    override = os.getenv("PRD_SHAREPOINT_REPORTS_ROOT")
    if override:
        return Path(override).expanduser().resolve()
    return (DEFAULT_SYNC_ROOT / DEFAULT_REPORTS_DIRNAME).resolve()


def combined_workbook_for_session(session_root: Path) -> Path:
    """Return the expected combined workbook path for a session."""
    return session_root / "reports" / f"{session_root.name}_completed_rows.xlsx"


def row_report_paths(session_root: Path) -> list[Path]:
    """Return all row-level report workbook paths for a session."""
    return sorted((session_root / "reports").glob("row_*_research_report.xlsx"))


def publish_session_reports(
    session_root: str,
    *,
    destination_root: str | None = None,
    include_row_reports: bool = False,
    combined_name: str | None = None,
    row_reports_subdir: str | None = None,
) -> dict[str, Any]:
    """Copy session report artifacts into the synced SharePoint reports folder."""
    root = Path(session_root).resolve()
    destination = Path(destination_root).resolve() if destination_root else reports_root_from_env_or_default()
    destination.mkdir(parents=True, exist_ok=True)

    combined_source = combined_workbook_for_session(root)
    if not combined_source.exists():
        raise FileNotFoundError(f"Combined workbook not found at {combined_source}.")

    combined_target = destination / (combined_name or combined_source.name)
    shutil.copy2(combined_source, combined_target)

    copied_row_reports: list[str] = []
    row_report_target_dir = None
    if include_row_reports:
        row_report_target_dir = destination / (row_reports_subdir or root.name)
        row_report_target_dir.mkdir(parents=True, exist_ok=True)
        for report_path in row_report_paths(root):
            target_path = row_report_target_dir / report_path.name
            shutil.copy2(report_path, target_path)
            copied_row_reports.append(str(target_path))

    return {
        "session_root": str(root),
        "destination_root": str(destination),
        "combined_source": str(combined_source),
        "combined_target": str(combined_target),
        "row_reports_included": include_row_reports,
        "row_report_target_dir": str(row_report_target_dir) if row_report_target_dir else None,
        "row_report_count": len(copied_row_reports),
        "row_report_targets": copied_row_reports,
    }


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""
    parser = argparse.ArgumentParser(
        description="Publish generated PRD research reports into the locally synced SharePoint folder."
    )
    parser.add_argument("session_root", help="Path to an initialized research session.")
    parser.add_argument(
        "--destination-root",
        default=None,
        help="Override the synced SharePoint reports folder. Defaults to PRD_SHAREPOINT_REPORTS_ROOT or the local Manny Tools sync root.",
    )
    parser.add_argument(
        "--include-row-reports",
        action="store_true",
        help="Also copy the row-level report workbooks into a session subfolder.",
    )
    parser.add_argument(
        "--combined-name",
        default=None,
        help="Optional filename override for the combined workbook in SharePoint.",
    )
    parser.add_argument(
        "--row-reports-subdir",
        default=None,
        help="Optional subfolder name for row-level reports. Defaults to the session name.",
    )
    return parser


def main() -> None:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args()
    result = publish_session_reports(
        args.session_root,
        destination_root=args.destination_root,
        include_row_reports=args.include_row_reports,
        combined_name=args.combined_name,
        row_reports_subdir=args.row_reports_subdir,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
