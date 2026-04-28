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
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any


DEFAULT_SYNC_ROOT = Path(r"C:\Users\Sunco\Sunco Lighting\Product - Manny Tools\PRD Research")
DEFAULT_REPORTS_DIRNAME = "Research Reports"
DEFAULT_GOALS_PATH = Path(r"C:\Users\Sunco\OneDrive - Sunco Lighting\Documents\Claude Workbook\Manny Sunco\GOALS.md")


def reports_root_from_env_or_default() -> Path:
    """Return the default synced SharePoint reports root."""
    override = os.getenv("PRD_SHAREPOINT_REPORTS_ROOT")
    if override:
        return Path(override).expanduser().resolve()
    return (DEFAULT_SYNC_ROOT / DEFAULT_REPORTS_DIRNAME).resolve()


def goals_path_from_env_or_default() -> Path:
    """Return the markdown file used for category-owner fallback lookup."""
    override = os.getenv("PRD_CATEGORY_OWNER_GOALS_FILE")
    if override:
        return Path(override).expanduser().resolve()
    return DEFAULT_GOALS_PATH.resolve()


def combined_workbook_for_session(session_root: Path) -> Path:
    """Return the expected combined workbook path for a session."""
    return session_root / "reports" / f"{session_root.name}_completed_rows.xlsx"


def packet_paths_for_session(session_root: Path) -> list[Path]:
    """Return all packet JSON files for a session."""
    return sorted((session_root / "packets").glob("row_*_packet.json"))


def row_report_paths(session_root: Path) -> list[Path]:
    """Return all row-level report workbook paths for a session."""
    return sorted((session_root / "reports").glob("row_*_research_report.xlsx"))


def read_json(path: Path) -> dict[str, Any]:
    """Read a JSON file into a dict."""
    return json.loads(path.read_text(encoding="utf-8"))


def unique_nonempty_texts(values: list[Any]) -> list[str]:
    """Return normalized non-empty strings while preserving input order."""
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        marker = text.casefold()
        if marker in seen:
            continue
        seen.add(marker)
        result.append(text)
    return result


def normalized_label_key(value: str | None) -> str:
    """Normalize loose category/subcategory labels for matching."""
    text = (value or "").strip().casefold()
    text = re.sub(r"[^a-z0-9]+", "", text)
    return text


def sanitize_filename_component(value: str | None, *, fallback: str) -> str:
    """Convert loose text into a filesystem-safe filename component."""
    text = (value or "").strip()
    if not text:
        return fallback
    text = re.sub(r"[^A-Za-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or fallback


def resolve_component_label(values: list[str], *, empty_fallback: str, mixed_fallback: str) -> str:
    """Resolve one publish-name component from a session-level value set."""
    unique_values = unique_nonempty_texts(values)
    if not unique_values:
        return empty_fallback
    if len(unique_values) > 1:
        return mixed_fallback
    return sanitize_filename_component(unique_values[0], fallback=empty_fallback)


def parse_goals_owner_map(goals_path: Path) -> dict[tuple[str, str | None], str]:
    """Parse the owner/category/subcategory markdown mapping from GOALS.md."""
    if not goals_path.exists():
        return {}

    owner_map: dict[tuple[str, str | None], str] = {}
    current_owner: str | None = None
    current_category: str | None = None

    for raw_line in goals_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        normalized_line = raw_line.replace("\u00a0", " ").rstrip()
        stripped_line = normalized_line.strip()
        if not stripped_line:
            continue

        owner_match = re.match(r"^([A-Za-z][A-Za-z .'-]*):\s*$", stripped_line)
        if owner_match:
            current_owner = owner_match.group(1).strip()
            current_category = None
            continue

        if not current_owner:
            continue

        bullet_match = re.match(r"^(\s*)- (.+?)\s*(?:\(.*\))?$", normalized_line)
        if not bullet_match:
            continue

        indent = len(bullet_match.group(1) or "")
        bullet_text = bullet_match.group(2).strip()
        if indent >= 2 and current_category:
            owner_map[
                (
                    normalized_label_key(current_category),
                    normalized_label_key(bullet_text),
                )
            ] = current_owner
            continue

        current_category = bullet_text
        owner_map[(normalized_label_key(current_category), None)] = current_owner

    return owner_map


def owner_from_goals_map(category: str | None, subcategory: str | None, owner_map: dict[tuple[str, str | None], str]) -> str | None:
    """Resolve an owner from GOALS.md using category/subcategory fallback matching."""
    category_key = normalized_label_key(category)
    subcategory_key = normalized_label_key(subcategory)
    if not category_key:
        return None
    if subcategory_key and (category_key, subcategory_key) in owner_map:
        return owner_map[(category_key, subcategory_key)]
    return owner_map.get((category_key, None))


def session_publish_metadata(session_root: Path, combined_source: Path) -> dict[str, Any]:
    """Infer owner/category/date metadata for publish naming."""
    owners: list[str] = []
    categories: list[str] = []
    subcategories: list[str] = []
    owner_lookup_source = "packet_identity"
    goals_owner_map = parse_goals_owner_map(goals_path_from_env_or_default())

    for packet_path in packet_paths_for_session(session_root):
        packet = read_json(packet_path)
        identity = packet.get("identity") or {}
        category = identity.get("category") or ""
        subcategory = identity.get("subcategory") or ""
        owner = identity.get("category_owner") or identity.get("owner") or ""
        if not owner:
            owner = owner_from_goals_map(category, subcategory, goals_owner_map) or ""
            if owner:
                owner_lookup_source = "goals_markdown_fallback"
        owners.append(owner)
        categories.append(category)
        subcategories.append(subcategory)

    run_date = datetime.fromtimestamp(combined_source.stat().st_mtime).strftime("%Y%m%d")
    owner_label = resolve_component_label(
        owners,
        empty_fallback="UnknownOwner",
        mixed_fallback="MultipleOwners",
    )
    category_label = resolve_component_label(
        categories,
        empty_fallback="Uncategorized",
        mixed_fallback="MixedCategories",
    )

    return {
        "category_owner": owner_label,
        "category": category_label,
        "run_date": run_date,
        "owner_values": unique_nonempty_texts(owners),
        "category_values": unique_nonempty_texts(categories),
        "subcategory_values": unique_nonempty_texts(subcategories),
        "owner_lookup_source": owner_lookup_source,
        "goals_path": str(goals_path_from_env_or_default()),
    }


def default_combined_publish_name(session_root: Path, combined_source: Path) -> str:
    """Build the default publish filename for a combined report workbook."""
    metadata = session_publish_metadata(session_root, combined_source)
    return f"{metadata['category_owner']}_{metadata['category']}_{metadata['run_date']}{combined_source.suffix}"


def next_available_path(target_path: Path) -> Path:
    """Return a non-destructive target path with a numeric suffix if needed."""
    if not target_path.exists():
        return target_path

    stem = target_path.stem
    suffix = target_path.suffix
    counter = 2
    while True:
        candidate = target_path.with_name(f"{stem}_{counter:02d}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


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

    naming_metadata = session_publish_metadata(root, combined_source)
    requested_name = combined_name or default_combined_publish_name(root, combined_source)
    combined_target = next_available_path(destination / requested_name)
    shutil.copy2(combined_source, combined_target)

    copied_row_reports: list[str] = []
    row_report_target_dir = None
    if include_row_reports:
        row_report_target_dir = destination / (row_reports_subdir or combined_target.stem)
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
        "combined_name_requested": requested_name,
        "combined_name_applied": combined_target.name,
        "naming_metadata": naming_metadata,
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
