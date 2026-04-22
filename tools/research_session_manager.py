"""
Step 4B: Resumable research workspace manager.

Usage:
  python tools/research_session_manager.py init "C:\\path\\to\\filled_workbook.xlsx"
  python tools/research_session_manager.py update "C:\\path\\to\\output\\research_sessions\\session_name"
  python tools/research_session_manager.py status "C:\\path\\to\\output\\research_sessions\\session_name"
  python tools/research_session_manager.py next-batch "C:\\path\\to\\output\\research_sessions\\session_name" --limit 3
  python tools/research_session_manager.py validate "C:\\path\\to\\output\\research_sessions\\session_name" --rows 6,7
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime, UTC
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from competitive_research_engine import build_research_packets
from template_parser import DEFAULT_WORKBOOK, load_postgres_payloads


TOOLS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TOOLS_DIR.parent
DOCS_SCHEMA_DIR = REPO_ROOT / "docs" / "schemas"
STEP4_PROMPT_TEMPLATE = REPO_ROOT / "docs" / "STEP4_PROMPT.md"
SCHEMA_VERSION = "2026-04-15"
PACKET_DIR = "packets"
INSTRUCTION_DIR = "instructions"
SCHEMA_DIR = "schemas"
RAW_STAGE_SEQUENCE = [
    "amazon_collection",
    "brick_and_mortar_collection",
    "brand_site_collection",
]
CHANNEL_BY_STAGE = {
    "amazon_collection": "amazon",
    "brick_and_mortar_collection": "brick_and_mortar",
    "brand_site_collection": "brand_sites",
}

DOMAIN_TO_CHANNEL = {
    "amazon.com": "amazon",
    "www.amazon.com": "amazon",
    "homedepot.com": "home_depot",
    "www.homedepot.com": "home_depot",
    "walmart.com": "walmart",
    "www.walmart.com": "walmart",
    "lowes.com": "lowes",
    "www.lowes.com": "lowes",
}

STAGE_DEFINITIONS = {
    "amazon_collection": {
        "artifact_type": "amazon_raw",
        "filename_suffix": "amazon_raw.json",
        "subdir": Path("raw") / "amazon",
        "expected_owner": "collector",
        "source_channel_group": "amazon",
    },
    "brick_and_mortar_collection": {
        "artifact_type": "brick_and_mortar_raw",
        "filename_suffix": "brick_and_mortar_raw.json",
        "subdir": Path("raw") / "brick_and_mortar",
        "expected_owner": "collector",
        "source_channel_group": "brick_and_mortar",
    },
    "brand_site_collection": {
        "artifact_type": "brand_sites_raw",
        "filename_suffix": "brand_sites_raw.json",
        "subdir": Path("raw") / "brand_sites",
        "expected_owner": "collector",
        "source_channel_group": "brand_sites",
    },
    "normalized": {
        "artifact_type": "competitors_normalized",
        "filename_suffix": "competitors_normalized.json",
        "subdir": Path("normalized"),
        "expected_owner": "codex",
        "source_channel_group": "all_channels",
    },
    "analyzed": {
        "artifact_type": "analysis",
        "filename_suffix": "analysis.json",
        "subdir": Path("analysis"),
        "expected_owner": "codex",
        "source_channel_group": "analysis",
    },
    "reported": {
        "artifact_type": "report",
        "filename_suffix": "research_report.xlsx",
        "subdir": Path("reports"),
        "expected_owner": "codex",
        "source_channel_group": "report",
    },
}


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def slugify(value: str) -> str:
    """Create a filesystem-friendly session slug."""
    text = value.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_") or "research_session"


def row_prefix(row_number: int) -> str:
    """Build the stable row prefix used in artifact file names."""
    return f"row_{row_number:03d}"


def write_json(path: Path, payload: Any) -> None:
    """Write JSON with a trailing newline."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    """Read a JSON file into a dict."""
    return json.loads(path.read_text(encoding="utf-8"))


def parse_rows_argument(value: str | None) -> list[int] | None:
    """Parse an optional comma-separated row list."""
    if not value:
        return None
    rows = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        rows.append(int(part))
    return rows or None


def normalize_string(value: Any) -> str | None:
    """Normalize loose string inputs."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def is_number(value: Any) -> bool:
    """Return whether a value is a non-bool number."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def infer_source_domain(url: Any) -> str | None:
    """Infer the hostname from a URL-like value."""
    if not isinstance(url, str) or not url.strip():
        return None
    hostname = urlparse(url.strip()).netloc.strip().lower()
    return hostname or None


def infer_source_channel(stage_key: str, source_domain: str | None) -> str:
    """Infer a source_channel fallback for raw artifact repair."""
    if stage_key == "amazon_collection":
        return "amazon"
    if stage_key == "brand_site_collection":
        return "brand_site"
    if source_domain:
        return DOMAIN_TO_CHANNEL.get(source_domain, "home_depot")
    return "home_depot"


def infer_brand_value(item: dict[str, Any]) -> str:
    """Return a safe non-empty brand label for malformed raw items."""
    title = normalize_string(item.get("product_title")) or ""
    lowered = title.lower()
    generic_prefixes = (
        "2x4 ",
        "2 pack ",
        "4 pack ",
        "6 pack ",
        "\"2x4\"",
        "led ",
    )
    if lowered.startswith(generic_prefixes):
        return "Generic / Unbranded"
    return "Generic / Unbranded"


def ensure_string_list(value: Any) -> list[str]:
    """Coerce loose values into a list of strings."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def infer_dimmable_value(value: Any) -> bool | None:
    """Infer a dimmable boolean from loose text or booleans."""
    if isinstance(value, bool):
        return value
    text = normalize_string(value)
    if not text:
        return None
    lowered = text.lower()
    if "non-dim" in lowered:
        return False
    if "0-10" in lowered or "triac" in lowered or "dimmable" in lowered:
        return True
    return None


def infer_dimming_type_value(value: Any) -> str | None:
    """Infer a normalized dimming type label from loose text."""
    text = normalize_string(value)
    if not text:
        return None
    lowered = text.lower()
    if "0-10" in lowered:
        return "0-10V"
    if "triac" in lowered:
        return "TRIAC"
    return None


def coerce_legacy_candidate_item(candidate: Any, stage_key: str, default_channel: str, default_collection_method: str) -> dict[str, Any] | None:
    """Convert a legacy Claude candidate draft into the shared competitor-result shape."""
    if not isinstance(candidate, dict):
        return None

    specs = candidate.get("specs")
    if not isinstance(specs, dict):
        specs = {}

    url = normalize_string(candidate.get("url"))
    source_domain = infer_source_domain(url)
    source_channel = normalize_string(candidate.get("source_channel")) or infer_source_channel(stage_key, source_domain)
    collection_method = normalize_string(candidate.get("collection_method")) or default_collection_method or "web_search"

    pack_quantity = candidate.get("pack_quantity", candidate.get("pack_qty"))
    if not is_number(pack_quantity):
        pack_quantity = None

    dimming_value = (
        normalize_string(candidate.get("dimming_type"))
        or infer_dimming_type_value(candidate.get("dimming"))
        or infer_dimming_type_value(specs.get("dimming"))
    )

    dimmable_value = candidate.get("dimmable")
    if not isinstance(dimmable_value, bool):
        dimmable_value = (
            infer_dimmable_value(candidate.get("dimming"))
            if candidate.get("dimming") is not None
            else infer_dimmable_value(specs.get("dimming"))
        )

    features = ensure_string_list(candidate.get("features"))
    mount_type = normalize_string(candidate.get("mount_type")) or normalize_string(specs.get("mount_type"))
    if mount_type:
        features.append(mount_type.replace("_", " "))
    if candidate.get("emergency_battery") is True:
        features.append("emergency battery")
    emergency_duration = candidate.get("emergency_duration_min")
    if is_number(emergency_duration):
        features.append(f"{int(emergency_duration)} minute emergency backup")
    country_of_origin = normalize_string(candidate.get("country_of_origin"))
    if country_of_origin:
        features.append(f"country of origin: {country_of_origin}")

    notes = ensure_string_list(candidate.get("notes"))
    extraction_notes = normalize_string(candidate.get("extraction_notes"))
    if not extraction_notes and notes:
        extraction_notes = "; ".join(notes)

    match_notes = (
        normalize_string(candidate.get("match_notes"))
        or normalize_string(candidate.get("match_rationale"))
        or normalize_string(candidate.get("confidence_rationale"))
    )

    item = {
        "candidate_id": normalize_string(candidate.get("candidate_id") or candidate.get("asin")),
        "source_channel": source_channel or default_channel,
        "source_domain": source_domain,
        "collection_method": collection_method,
        "brand": normalize_string(candidate.get("brand")) or infer_brand_value(candidate),
        "product_title": normalize_string(candidate.get("product_title") or candidate.get("title")),
        "model_number": normalize_string(candidate.get("model_number") or candidate.get("asin")),
        "sku": normalize_string(candidate.get("sku") or candidate.get("asin")),
        "variant": normalize_string(candidate.get("variant")),
        "pack_quantity": pack_quantity,
        "url": url,
        "price": candidate.get("price"),
        "currency": normalize_string(candidate.get("currency")) or "USD",
        "wattage": normalize_string(candidate.get("wattage") or specs.get("wattage")),
        "lumens": normalize_string(candidate.get("lumens") or specs.get("lumens")),
        "cct": normalize_string(candidate.get("cct") or specs.get("cct")),
        "cri": normalize_string(candidate.get("cri") or specs.get("cri")),
        "voltage": normalize_string(candidate.get("voltage") or specs.get("voltage")),
        "dimmable": dimmable_value,
        "dimming_type": dimming_value,
        "certifications": ensure_string_list(candidate.get("certifications") or specs.get("certifications")),
        "features": list(dict.fromkeys(features)),
        "rating": candidate.get("rating"),
        "review_count": candidate.get("review_count"),
        "availability": normalize_string(candidate.get("availability")),
        "match_confidence": candidate.get("match_confidence"),
        "match_notes": match_notes,
        "extraction_notes": extraction_notes,
    }

    return item


def default_output_root(workbook_path: str) -> Path:
    """Put resumable sessions next to the workbook clone, not the code clone."""
    workbook = Path(workbook_path).resolve()
    if workbook.parent.name.lower() == "templates":
        clone_root = workbook.parent.parent
    else:
        clone_root = workbook.parent
    return clone_root / "output" / "research_sessions"


def resolve_session_dir(
    workbook_path: str,
    output_root: str | None,
    session_name: str | None,
) -> Path:
    """Resolve the session directory for init mode."""
    root = Path(output_root).resolve() if output_root else default_output_root(workbook_path)
    workbook = Path(workbook_path).resolve()
    name = session_name or slugify(workbook.stem)
    return root / name


def packet_path_for(session_dir: Path, row_number: int) -> Path:
    """Return the packet path for a row."""
    return session_dir / PACKET_DIR / f"{row_prefix(row_number)}_packet.json"


def artifact_path_for(session_dir: Path, row_number: int, stage_key: str) -> Path:
    """Return the expected artifact path for a row and stage."""
    config = STAGE_DEFINITIONS[stage_key]
    return session_dir / config["subdir"] / f"{row_prefix(row_number)}_{config['filename_suffix']}"


def relative_path(path: Path, session_dir: Path) -> str:
    """Render a path relative to the session root."""
    return str(path.resolve().relative_to(session_dir.resolve()))


def build_artifact_placeholder(
    packet: dict[str, Any],
    session_dir: Path,
    batch_id: str,
    stage_key: str,
) -> dict[str, Any]:
    """Create an empty artifact shell for later Claude/Codex population."""
    config = STAGE_DEFINITIONS[stage_key]
    schema_name = (
        "schemas/analysis-artifact.schema.json"
        if stage_key == "analyzed"
        else "schemas/collection-artifact.schema.json"
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_type": config["artifact_type"],
        "artifact_status": "not_started",
        "batch_id": batch_id,
        "row_number": packet["row_number"],
        "ideation_name": packet["identity"].get("ideation_name"),
        "expected_owner": config["expected_owner"],
        "source_channel_group": config["source_channel_group"],
        "packet_file": relative_path(packet_path_for(session_dir, packet["row_number"]), session_dir),
        "queries_used": [],
        "items": [],
        "summary": {},
        "recommendations": [],
        "notes": [
            f"Populate this file using {schema_name}.",
        ],
        "blocking_issues": [],
        "updated_at": utc_now(),
    }
    if stage_key == "analyzed":
        payload["normalized_file"] = relative_path(
            artifact_path_for(session_dir, packet["row_number"], "normalized"),
            session_dir,
        )
        payload.pop("items")
    else:
        payload.pop("recommendations")
    return payload


def ensure_placeholder_artifacts(
    session_dir: Path,
    packets: list[dict[str, Any]],
    batch_id: str,
) -> None:
    """Create placeholder files for all resumable stages if they do not exist."""
    for packet in packets:
        for stage_key in STAGE_DEFINITIONS:
            if stage_key == "reported":
                artifact_path_for(session_dir, packet["row_number"], stage_key).parent.mkdir(
                    parents=True, exist_ok=True
                )
                continue
            artifact_path = artifact_path_for(session_dir, packet["row_number"], stage_key)
            if artifact_path.exists():
                continue
            write_json(
                artifact_path,
                build_artifact_placeholder(
                    packet=packet,
                    session_dir=session_dir,
                    batch_id=batch_id,
                    stage_key=stage_key,
                ),
            )


def copy_schema_files(session_dir: Path) -> None:
    """Copy the repo schema docs into the session for easy Claude/Codex access."""
    destination = session_dir / SCHEMA_DIR
    destination.mkdir(parents=True, exist_ok=True)
    for source in DOCS_SCHEMA_DIR.glob("*.schema.json"):
        destination.joinpath(source.name).write_text(
            source.read_text(encoding="utf-8"),
            encoding="utf-8",
        )


def copy_prompt_template(session_dir: Path) -> None:
    """Copy the shared Step 4 prompt template into the session instructions."""
    destination = session_dir / INSTRUCTION_DIR
    destination.mkdir(parents=True, exist_ok=True)
    destination.joinpath("STEP4_PROMPT.md").write_text(
        STEP4_PROMPT_TEMPLATE.read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def stage_status_from_file(path: Path) -> str:
    """Derive a manifest stage status from an artifact file."""
    if not path.exists():
        return "pending"
    if path.suffix.lower() != ".json":
        return "complete"

    try:
        payload = read_json(path)
    except Exception:
        return "blocked"

    status = str(payload.get("artifact_status", "")).strip().lower()
    mapping = {
        "not_started": "pending",
        "in_progress": "in_progress",
        "complete": "complete",
        "blocked": "blocked",
    }
    return mapping.get(status, "pending")


def build_row_manifest_entry(session_dir: Path, packet: dict[str, Any]) -> dict[str, Any]:
    """Build one manifest row from the packet file plus artifact statuses."""
    row_number = packet["row_number"]
    packet_status = packet.get("status")
    stages = {"packet_ready": "complete"}
    artifacts = {
        "packet": relative_path(packet_path_for(session_dir, row_number), session_dir),
    }

    for stage_key in STAGE_DEFINITIONS:
        artifact_path = artifact_path_for(session_dir, row_number, stage_key)
        stages[stage_key] = stage_status_from_file(artifact_path)
        artifacts[stage_key] = relative_path(artifact_path, session_dir)

    if all(status == "complete" for status in stages.values()):
        overall_status = "complete"
    elif any(status == "blocked" for status in stages.values()):
        overall_status = "blocked"
    elif any(status in {"in_progress", "complete"} for key, status in stages.items() if key != "packet_ready"):
        overall_status = "in_progress"
    elif packet_status == "ready_with_reference_warning":
        overall_status = "ready_for_collection_with_reference_warning"
    else:
        overall_status = "ready_for_collection"

    return {
        "row_number": row_number,
        "ideation_name": packet["identity"].get("ideation_name"),
        "packet_status": packet_status,
        "overall_status": overall_status,
        "packet_file": artifacts["packet"],
        "issues": packet.get("issues", []),
        "stages": stages,
        "artifacts": artifacts,
    }


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize row and stage completion counts."""
    overall_counts = Counter(row["overall_status"] for row in rows)
    stage_counts: dict[str, dict[str, int]] = {}

    stage_keys = ["packet_ready", *STAGE_DEFINITIONS.keys()]
    for stage_key in stage_keys:
        counter = Counter(row["stages"].get(stage_key, "pending") for row in rows)
        stage_counts[stage_key] = dict(counter)

    return {
        "row_count": len(rows),
        "overall_status_counts": dict(overall_counts),
        "stage_status_counts": stage_counts,
    }


def load_packets(session_dir: Path) -> list[dict[str, Any]]:
    """Load all row packet files from the session."""
    packets = []
    for path in sorted((session_dir / PACKET_DIR).glob("row_*_packet.json")):
        packets.append(read_json(path))
    return packets


def refresh_session_files(
    session_dir: Path,
    packets: list[dict[str, Any]],
    workbook_path: str,
    sheet_name: str,
    batch_id: str,
) -> dict[str, Any]:
    """Refresh all derived session files from packet/artifact state."""
    ensure_placeholder_artifacts(session_dir, packets, batch_id)
    copy_schema_files(session_dir)
    write_instruction_files(session_dir, workbook_path, batch_id)
    manifest = build_manifest(
        session_dir=session_dir,
        workbook_path=workbook_path,
        sheet_name=sheet_name,
        batch_id=batch_id,
    )
    write_json(session_dir / "manifest.json", manifest)
    return manifest


def fallback_refresh_from_existing_session(
    session_dir: Path,
    workbook_path: str,
    sheet_name: str,
    batch_id: str,
) -> dict[str, Any]:
    """Rebuild the session from existing packets when the workbook cannot be reopened."""
    packets = load_packets(session_dir)
    if not packets:
        raise FileNotFoundError(
            f"No existing packet files were found in '{session_dir}'. "
            "Cannot refresh the session without reopening the workbook."
        )
    manifest = refresh_session_files(
        session_dir=session_dir,
        packets=packets,
        workbook_path=workbook_path,
        sheet_name=sheet_name,
        batch_id=batch_id,
    )
    return {
        "session_root": str(session_dir.resolve()),
        "manifest_path": str((session_dir / "manifest.json").resolve()),
        "packet_count": len(packets),
        "warnings": [
            "Workbook could not be reopened. Refreshed the session from existing packet and artifact files instead."
        ],
        "summary": manifest["summary"],
        "refresh_mode": "existing_session_fallback",
    }


def build_manifest(
    session_dir: Path,
    workbook_path: str,
    sheet_name: str,
    batch_id: str,
) -> dict[str, Any]:
    """Build the full research manifest from packets plus artifact files."""
    packets = load_packets(session_dir)
    rows = [build_row_manifest_entry(session_dir, packet) for packet in packets]
    return {
        "schema_version": SCHEMA_VERSION,
        "batch_id": batch_id,
        "workbook_path": str(Path(workbook_path).resolve()),
        "session_root": str(session_dir.resolve()),
        "sheet_name": sheet_name,
        "updated_at": utc_now(),
        "summary": build_summary(rows),
        "rows": rows,
    }


def next_collection_tasks(
    session_dir: Path,
    rows: list[int] | None = None,
    limit: int = 3,
) -> list[dict[str, Any]]:
    """Return the next raw collection tasks from the manifest."""
    manifest = read_json(session_dir / "manifest.json")
    allowed_rows = set(rows or [row["row_number"] for row in manifest.get("rows", [])])
    tasks: list[dict[str, Any]] = []

    for row in manifest.get("rows", []):
        row_number = row["row_number"]
        if row_number not in allowed_rows:
            continue
        if row.get("overall_status") in {"complete", "blocked"}:
            continue
        for stage_key in RAW_STAGE_SEQUENCE:
            stage_status = row.get("stages", {}).get(stage_key, "pending")
            if stage_status not in {"pending", "in_progress"}:
                continue
            tasks.append(
                {
                    "row_number": row_number,
                    "ideation_name": row.get("ideation_name"),
                    "packet_status": row.get("packet_status"),
                    "overall_status": row.get("overall_status"),
                    "stage_key": stage_key,
                    "channel": CHANNEL_BY_STAGE[stage_key],
                    "artifact_status": stage_status,
                    "packet_file": row.get("artifacts", {}).get("packet"),
                    "output_file": row.get("artifacts", {}).get(stage_key),
                }
            )
            if len(tasks) >= limit:
                return tasks
    return tasks


def session_status(
    session_dir: str,
    rows: list[int] | None = None,
    limit: int = 3,
) -> dict[str, Any]:
    """Return a compact status summary plus the next raw collection tasks."""
    root = Path(session_dir).resolve()
    manifest = read_json(root / "manifest.json")
    return {
        "session_root": str(root),
        "manifest_path": str((root / "manifest.json").resolve()),
        "summary": manifest.get("summary", {}),
        "next_tasks": next_collection_tasks(root, rows=rows, limit=limit),
    }


def validate_string_list(
    value: Any,
    field_path: str,
    errors: list[str],
) -> None:
    """Validate an array of strings."""
    if not isinstance(value, list):
        errors.append(f"{field_path} must be an array.")
        return
    for index, item in enumerate(value):
        if not isinstance(item, str):
            errors.append(f"{field_path}[{index}] must be a string.")


def validate_competitor_result_item(item: Any, item_index: int) -> list[str]:
    """Validate one competitor-result object against the shared contract."""
    errors: list[str] = []
    path = f"items[{item_index}]"
    if not isinstance(item, dict):
        return [f"{path} must be an object."]

    allowed_keys = {
        "candidate_id",
        "source_channel",
        "source_domain",
        "collection_method",
        "brand",
        "product_title",
        "model_number",
        "sku",
        "variant",
        "pack_quantity",
        "url",
        "price",
        "currency",
        "wattage",
        "lumens",
        "cct",
        "cri",
        "voltage",
        "dimmable",
        "dimming_type",
        "certifications",
        "features",
        "rating",
        "review_count",
        "availability",
        "match_confidence",
        "match_notes",
        "extraction_notes",
        "raw_observations",
    }
    unexpected = sorted(set(item) - allowed_keys)
    for key in unexpected:
        errors.append(f"{path}.{key} is not allowed by competitor-result.schema.json.")

    required_strings = {
        "source_channel": {"amazon", "home_depot", "walmart", "lowes", "brand_site", "stackline_seed"},
        "collection_method": {"web_search", "web_fetch", "manual", "stackline_seed"},
        "brand": None,
        "product_title": None,
        "url": None,
    }
    for key, allowed_values in required_strings.items():
        value = item.get(key)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{path}.{key} must be a non-empty string.")
            continue
        if allowed_values and value not in allowed_values:
            errors.append(f"{path}.{key} must be one of {sorted(allowed_values)}.")

    optional_strings = [
        "candidate_id",
        "source_domain",
        "model_number",
        "sku",
        "variant",
        "currency",
        "wattage",
        "lumens",
        "cct",
        "cri",
        "voltage",
        "dimming_type",
        "availability",
        "match_notes",
        "extraction_notes",
    ]
    for key in optional_strings:
        value = item.get(key)
        if value is not None and not isinstance(value, str):
            errors.append(f"{path}.{key} must be a string or null.")

    for key in ["pack_quantity", "price", "rating", "review_count"]:
        value = item.get(key)
        if value is not None and not is_number(value):
            errors.append(f"{path}.{key} must be a number or null.")

    dimmable = item.get("dimmable")
    if dimmable is not None and not isinstance(dimmable, bool):
        errors.append(f"{path}.dimmable must be a boolean or null.")

    match_confidence = item.get("match_confidence")
    if match_confidence is not None:
        if not is_number(match_confidence):
            errors.append(f"{path}.match_confidence must be a number or null.")
        elif not 0 <= float(match_confidence) <= 1:
            errors.append(f"{path}.match_confidence must be between 0 and 1.")

    for key in ["certifications", "features", "raw_observations"]:
        value = item.get(key)
        if value is not None:
            validate_string_list(value, f"{path}.{key}", errors)

    return errors


def validate_collection_artifact_payload(
    payload: Any,
    expected_artifact_type: str,
) -> list[str]:
    """Validate one raw collection artifact payload."""
    errors: list[str] = []
    if not isinstance(payload, dict):
        return ["artifact must be a JSON object."]

    allowed_keys = {
        "schema_version",
        "artifact_type",
        "artifact_status",
        "batch_id",
        "row_number",
        "ideation_name",
        "expected_owner",
        "source_channel_group",
        "packet_file",
        "queries_used",
        "items",
        "summary",
        "recommendations",
        "notes",
        "blocking_issues",
        "updated_at",
        "normalized_file",
    }
    unexpected = sorted(set(payload) - allowed_keys)
    for key in unexpected:
        errors.append(f"{key} is not allowed by collection-artifact.schema.json.")

    required_fields = [
        "schema_version",
        "artifact_type",
        "artifact_status",
        "batch_id",
        "row_number",
        "ideation_name",
        "expected_owner",
        "updated_at",
    ]
    for key in required_fields:
        if key not in payload:
            errors.append(f"{key} is required.")

    if payload.get("artifact_type") != expected_artifact_type:
        errors.append(
            f"artifact_type must be '{expected_artifact_type}' for this stage."
        )
    if payload.get("expected_owner") not in {"claude", "codex", "collector"}:
        errors.append("expected_owner must be one of 'claude', 'codex', or 'collector' for raw collection artifacts.")
    artifact_status = payload.get("artifact_status")
    if artifact_status not in {"not_started", "in_progress", "complete", "blocked"}:
        errors.append("artifact_status must be one of not_started, in_progress, complete, blocked.")
    row_number = payload.get("row_number")
    if row_number is not None and not isinstance(row_number, int):
        errors.append("row_number must be an integer.")
    for key in ["schema_version", "batch_id", "ideation_name", "updated_at"]:
        value = payload.get(key)
        if value is not None and not isinstance(value, str):
            errors.append(f"{key} must be a string.")
    for key in ["source_channel_group", "packet_file"]:
        value = payload.get(key)
        if value is not None and not isinstance(value, str):
            errors.append(f"{key} must be a string or null.")

    if "queries_used" in payload:
        validate_string_list(payload.get("queries_used"), "queries_used", errors)
    if "notes" in payload:
        validate_string_list(payload.get("notes"), "notes", errors)
    if "blocking_issues" in payload:
        validate_string_list(payload.get("blocking_issues"), "blocking_issues", errors)

    if "items" not in payload:
        errors.append("items is required for raw collection artifacts.")
    elif not isinstance(payload.get("items"), list):
        errors.append("items must be an array.")
    else:
        for index, item in enumerate(payload.get("items", [])):
            errors.extend(validate_competitor_result_item(item, index))

    if artifact_status == "blocked" and not payload.get("blocking_issues"):
        errors.append("blocking_issues must be populated when artifact_status is blocked.")

    return errors


def repair_competitor_result_item(item: Any, stage_key: str) -> tuple[Any, list[str]]:
    """Repair common schema issues in one raw competitor item."""
    if not isinstance(item, dict):
        return item, []

    repaired = dict(item)
    fixes: list[str] = []

    source_domain = normalize_string(repaired.get("source_domain"))
    inferred_domain = infer_source_domain(repaired.get("url"))
    if not source_domain and inferred_domain:
        repaired["source_domain"] = inferred_domain
        source_domain = inferred_domain
        fixes.append("source_domain")

    source_channel = normalize_string(repaired.get("source_channel"))
    if not source_channel:
        repaired["source_channel"] = infer_source_channel(stage_key, source_domain)
        fixes.append("source_channel")

    collection_method = normalize_string(repaired.get("collection_method"))
    if not collection_method:
        repaired["collection_method"] = "manual"
        fixes.append("collection_method")

    brand = normalize_string(repaired.get("brand"))
    if not brand:
        repaired["brand"] = infer_brand_value(repaired)
        fixes.append("brand")

    if "queries_used" in repaired:
        repaired["queries_used"] = ensure_string_list(repaired.get("queries_used"))
    if "notes" in repaired:
        repaired["notes"] = ensure_string_list(repaired.get("notes"))
    if "blocking_issues" in repaired:
        repaired["blocking_issues"] = ensure_string_list(repaired.get("blocking_issues"))

    return repaired, fixes


def repair_collection_artifact_payload(payload: Any, stage_key: str) -> tuple[Any, list[str]]:
    """Repair common schema issues in one raw artifact payload."""
    if not isinstance(payload, dict):
        return payload, []

    repaired = dict(payload)
    fixes: list[str] = []
    config = STAGE_DEFINITIONS[stage_key]

    expected_owner = normalize_string(repaired.get("expected_owner"))
    if expected_owner not in {"claude", "codex", "collector"}:
        repaired["expected_owner"] = config["expected_owner"]
        fixes.append("expected_owner")

    artifact_status = normalize_string(repaired.get("artifact_status"))
    if artifact_status not in {"not_started", "in_progress", "complete", "blocked"}:
        if repaired.get("items"):
            repaired["artifact_status"] = "in_progress"
        else:
            repaired["artifact_status"] = "not_started"
        fixes.append("artifact_status")

    repaired["queries_used"] = ensure_string_list(repaired.get("queries_used"))
    repaired["notes"] = ensure_string_list(repaired.get("notes"))
    repaired["blocking_issues"] = ensure_string_list(repaired.get("blocking_issues"))

    if not normalize_string(repaired.get("ideation_name")):
        legacy_ideation_name = None
        row_reference = repaired.get("row_reference")
        if isinstance(row_reference, dict):
            legacy_ideation_name = normalize_string(
                row_reference.get("product_description") or row_reference.get("description")
            )
        row_spec_summary = repaired.get("row_spec_summary")
        if not legacy_ideation_name and isinstance(row_spec_summary, dict):
            legacy_ideation_name = normalize_string(row_spec_summary.get("description"))
        if legacy_ideation_name:
            repaired["ideation_name"] = legacy_ideation_name
            fixes.append("ideation_name")

    items = repaired.get("items")
    legacy_candidates = repaired.get("candidates")
    if not isinstance(items, list) and isinstance(legacy_candidates, list):
        default_channel = normalize_string(repaired.get("source_channel")) or CHANNEL_BY_STAGE.get(stage_key, "")
        default_collection_method = normalize_string(repaired.get("collection_method")) or "web_search"
        repaired_items = []
        for index, candidate in enumerate(legacy_candidates):
            coerced = coerce_legacy_candidate_item(candidate, stage_key, default_channel, default_collection_method)
            if coerced is None:
                continue
            repaired_items.append(coerced)
            fixes.append(f"items[{index}]")
        repaired["items"] = repaired_items
        items = repaired_items

        for legacy_field in (
            "candidates",
            "source_channel",
            "collection_method",
            "row_reference",
            "row_spec_summary",
            "recommendations",
            "row_spec_summary",
        ):
            if legacy_field in repaired:
                repaired.pop(legacy_field, None)
                fixes.append(f"drop:{legacy_field}")

    if isinstance(items, list):
        repaired_items = []
        for index, item in enumerate(items):
            repaired_item, item_fixes = repair_competitor_result_item(item, stage_key)
            repaired_items.append(repaired_item)
            fixes.extend([f"items[{index}].{field}" for field in item_fixes])
        repaired["items"] = repaired_items

    if fixes:
        repaired["updated_at"] = utc_now()

    return repaired, fixes


def repair_raw_artifacts(
    session_dir: str,
    rows: list[int] | None = None,
) -> dict[str, Any]:
    """Repair common raw-artifact schema issues so Codex can keep the pipeline moving."""
    root = Path(session_dir).resolve()
    manifest = read_json(root / "manifest.json")
    allowed_rows = set(rows or [row["row_number"] for row in manifest.get("rows", [])])
    repairs: list[dict[str, Any]] = []

    for row in manifest.get("rows", []):
        row_number = row["row_number"]
        if row_number not in allowed_rows:
            continue
        for stage_key in RAW_STAGE_SEQUENCE:
            artifact_rel = row.get("artifacts", {}).get(stage_key)
            if not artifact_rel:
                continue
            artifact_path = root / artifact_rel
            if not artifact_path.exists():
                continue
            try:
                payload = read_json(artifact_path)
            except Exception:
                continue

            repaired_payload, fixes = repair_collection_artifact_payload(payload, stage_key)
            if fixes:
                write_json(artifact_path, repaired_payload)
                repairs.append(
                    {
                        "row_number": row_number,
                        "stage_key": stage_key,
                        "artifact_file": str(artifact_path),
                        "fixes": fixes,
                    }
                )

    validation = validate_raw_artifacts(str(root), rows=rows)
    manifest = update_session(str(root))
    return {
        "session_root": str(root),
        "repair_count": len(repairs),
        "repairs": repairs,
        "validation": validation,
        "manifest_summary": manifest.get("summary", {}),
    }


def validate_raw_artifacts(
    session_dir: str,
    rows: list[int] | None = None,
) -> dict[str, Any]:
    """Validate raw collection artifacts in a session."""
    root = Path(session_dir).resolve()
    manifest = read_json(root / "manifest.json")
    allowed_rows = set(rows or [row["row_number"] for row in manifest.get("rows", [])])
    results = []

    for row in manifest.get("rows", []):
        row_number = row["row_number"]
        if row_number not in allowed_rows:
            continue
        for stage_key in RAW_STAGE_SEQUENCE:
            artifact_rel = row.get("artifacts", {}).get(stage_key)
            if not artifact_rel:
                continue
            artifact_path = root / artifact_rel
            stage_status = row.get("stages", {}).get(stage_key, "pending")
            if not artifact_path.exists():
                results.append(
                    {
                        "row_number": row_number,
                        "stage_key": stage_key,
                        "status": stage_status,
                        "valid": stage_status == "pending",
                        "errors": [] if stage_status == "pending" else ["artifact file is missing."],
                        "artifact_file": str(artifact_path),
                    }
                )
                continue
            try:
                payload = read_json(artifact_path)
            except Exception as exc:
                results.append(
                    {
                        "row_number": row_number,
                        "stage_key": stage_key,
                        "status": stage_status,
                        "valid": False,
                        "errors": [f"invalid JSON: {exc}"],
                        "artifact_file": str(artifact_path),
                    }
                )
                continue

            errors = validate_collection_artifact_payload(
                payload,
                expected_artifact_type=STAGE_DEFINITIONS[stage_key]["artifact_type"],
            )
            results.append(
                {
                    "row_number": row_number,
                    "stage_key": stage_key,
                    "status": stage_status,
                    "valid": not errors,
                    "errors": errors,
                    "artifact_file": str(artifact_path),
                }
            )

    invalid_count = sum(1 for result in results if not result["valid"])
    return {
        "session_root": str(root),
        "validated_artifact_count": len(results),
        "invalid_artifact_count": invalid_count,
        "results": results,
    }


def build_claude_instructions(session_dir: Path, workbook_path: str, batch_id: str) -> str:
    """Session-specific instructions for Claude collection work."""
    return f"""# Claude Next

Session root: `{session_dir}`
Workbook: `{Path(workbook_path).resolve()}`
Batch id: `{batch_id}`

You are the preferred collector for Step `4C`, `4D`, and `4E`, but Codex may repair or replace malformed raw artifacts if your session fails.

Do next:
1. Check the next actionable tasks with:
   - `python tools/research_session_manager.py next-batch "{session_dir}" --limit 3`
2. Use `instructions/STEP4_PROMPT.md` as the task template.
3. Work one task at a time: exactly `1 row x 1 channel`.
4. Write results only into:
   - `raw/amazon/row_###_amazon_raw.json`
   - `raw/brick_and_mortar/row_###_brick_and_mortar_raw.json`
   - `raw/brand_sites/row_###_brand_sites_raw.json`
5. Follow `schemas/collection-artifact.schema.json` and `schemas/competitor-result.schema.json`.
6. Set `artifact_status` to `in_progress`, `complete`, or `blocked`.
7. Validate the raw artifact shape before stopping:
   - `python tools/research_session_manager.py validate "{session_dir}" --rows <rows_touched>`
8. After finishing a batch, run:
   - `python tools/research_session_manager.py update "{session_dir}"`

Collection rules:
- Use the row packet's `research_plan` as the source of truth.
- Read channel queries from `research_plan.amazon`, `research_plan.brick_and_mortar`, and `research_plan.known_competitor_brands`.
- Use `research_plan.target_price_band`, `research_plan.brand_watchlist`, `research_plan.must_validate`, and `research_plan.collection_targets` to decide what belongs in the raw file.
- Keep raw collection faithful. Do not invent normalized values.
- When a match is weak, include it only if `match_confidence` and `match_notes` explain why.
- Record empty findings explicitly by setting `artifact_status` to `complete` with notes, or `blocked` with `blocking_issues`.

Do not do next:
- Do not edit `normalized/`
- Do not edit `analysis/`
- Do not edit `reports/`
"""


def build_collector_instructions(session_dir: Path, workbook_path: str, batch_id: str) -> str:
    """Session-specific instructions for whichever model is collecting raw competitors."""
    return f"""# Collector Next

Session root: `{session_dir}`
Workbook: `{Path(workbook_path).resolve()}`
Batch id: `{batch_id}`

You own exactly one raw-collection task at a time: `1 row x 1 channel`.

Do next:
1. Check the next actionable tasks with:
   - `python tools/research_session_manager.py next-batch "{session_dir}" --limit 3`
2. Use `instructions/STEP4_PROMPT.md` as the task template.
3. Pick exactly one task:
   - `amazon_collection`
   - `brick_and_mortar_collection`
   - `brand_site_collection`
4. Write results only into:
   - `raw/amazon/row_###_amazon_raw.json`
   - `raw/brick_and_mortar/row_###_brick_and_mortar_raw.json`
   - `raw/brand_sites/row_###_brand_sites_raw.json`
5. Validate the raw artifact before stopping:
   - `python tools/research_session_manager.py validate "{session_dir}" --rows <rows_touched>`
6. Refresh the manifest after each task:
   - `python tools/research_session_manager.py update "{session_dir}"`

Collector rules:
- Use the row packet's `research_plan` as the source of truth.
- Keep raw collection faithful. Do not normalize, dedupe, or analyze.
- If the artifact already says `complete` or `blocked`, skip it.
- If the session dies mid-task, leave the file as `in_progress` with whatever has been captured so far.
- If a raw artifact exists but fails schema validation, Codex may repair it after you stop.
"""


def build_codex_instructions(session_dir: Path, workbook_path: str, batch_id: str) -> str:
    """Session-specific instructions for Codex structuring and analysis work."""
    return f"""# Codex Next

Session root: `{session_dir}`
Workbook: `{Path(workbook_path).resolve()}`
Batch id: `{batch_id}`

You own Step `4B`, `4F`, `5A`, `5B`, `5C`, and `6A`, and you are the fallback owner for raw artifact repair and small-batch raw collection when the preferred collector is unavailable.

Do next:
1. Keep `packets/`, `schemas/`, `instructions/`, and `manifest.json` current.
2. Prefer completed raw collection artifacts under `raw/`, but if they are malformed repair them with:
   - `python tools/research_session_manager.py repair-raw "{session_dir}" --rows <rows_touched>`
3. Normalize raw collection artifacts with:
   - `python tools/competitor_normalizer.py "{session_dir}"`
4. Review and refine:
   - `normalized/row_###_competitors_normalized.json`
5. Analyze normalized competitors into:
   - `analysis/row_###_analysis.json`
   - `python tools/competitive_analysis.py "{session_dir}"`
6. Build row-level Excel report artifacts with:
   - `python tools/research_report_builder.py "{session_dir}"`
7. Build the combined workbook for completed rows with:
   - `python tools/research_report_builder.py "{session_dir}" --combined`
8. Validate raw collection artifacts when a collector hands off a batch:
    - `python tools/research_session_manager.py validate "{session_dir}" --rows <rows_touched>`
9. Refresh status after each pass:
    - `python tools/research_session_manager.py update "{session_dir}"`

Codex rules:
- Do not recollect competitors if the raw artifact already exists and is marked `complete`.
- Treat raw files as source material, not final truth.
- Normalize pack quantity, pricing units, wattage, lumens, CCT, CRI, certifications, and feature flags before analysis.
- Use `schemas/analysis-artifact.schema.json` for the row-level analysis contract.
- Keep the guiding star: estimate ideation performance and propose the best product configuration, not just summarize search output.

Do not do next:
- Do not overwrite a collector's raw files unless they are malformed and you are explicitly repairing the schema or taking over because the collection session failed.
- Do not skip the manifest. It is the handoff contract between models.
"""


def write_instruction_files(session_dir: Path, workbook_path: str, batch_id: str) -> None:
    """Write session-specific handoff docs."""
    instruction_root = session_dir / INSTRUCTION_DIR
    instruction_root.mkdir(parents=True, exist_ok=True)
    copy_prompt_template(session_dir)
    (instruction_root / "CLAUDE_NEXT.md").write_text(
        build_claude_instructions(session_dir, workbook_path, batch_id),
        encoding="utf-8",
    )
    (instruction_root / "CODEX_NEXT.md").write_text(
        build_codex_instructions(session_dir, workbook_path, batch_id),
        encoding="utf-8",
    )
    (instruction_root / "COLLECTOR_NEXT.md").write_text(
        build_collector_instructions(session_dir, workbook_path, batch_id),
        encoding="utf-8",
    )


def initialize_session(
    workbook_path: str,
    session_name: str | None,
    output_root: str | None,
    postgres_json: str | None,
    include_queries: bool,
    include_stackline_raw: bool,
    start_date: str | None,
    end_date: str | None,
    stackline_folder: str | None,
    stackline_brand: str,
    sheet_name: str,
) -> dict[str, Any]:
    """Initialize or refresh a resumable research session."""
    session_dir = resolve_session_dir(workbook_path, output_root, session_name)
    batch_id = session_dir.name
    resolved_workbook_path = str(Path(workbook_path).resolve())

    postgres_payloads = load_postgres_payloads(postgres_json)
    try:
        packet_bundle = build_research_packets(
            workbook_path=workbook_path,
            postgres_payloads=postgres_payloads,
            include_queries=include_queries,
            include_stackline_raw=include_stackline_raw,
            start_date=start_date,
            end_date=end_date,
            stackline_folder=stackline_folder,
            stackline_brand=stackline_brand,
            sheet_name=sheet_name,
        )
    except PermissionError:
        if not (session_dir / "manifest.json").exists():
            raise
        return fallback_refresh_from_existing_session(
            session_dir=session_dir,
            workbook_path=resolved_workbook_path,
            sheet_name=sheet_name,
            batch_id=batch_id,
        )
    except OSError as exc:
        error_text = str(exc).lower()
        if "permission denied" not in error_text and "being used by another process" not in error_text:
            raise
        if not (session_dir / "manifest.json").exists():
            raise
        return fallback_refresh_from_existing_session(
            session_dir=session_dir,
            workbook_path=resolved_workbook_path,
            sheet_name=sheet_name,
            batch_id=batch_id,
        )

    session_dir.mkdir(parents=True, exist_ok=True)
    for packet in packet_bundle["packets"]:
        write_json(packet_path_for(session_dir, packet["row_number"]), packet)

    manifest = refresh_session_files(
        session_dir=session_dir,
        packets=packet_bundle["packets"],
        workbook_path=packet_bundle["workbook_path"],
        sheet_name=packet_bundle["sheet_name"],
        batch_id=batch_id,
    )

    return {
        "session_root": str(session_dir.resolve()),
        "manifest_path": str((session_dir / "manifest.json").resolve()),
        "packet_count": packet_bundle["packet_count"],
        "warnings": packet_bundle.get("warnings", []),
        "summary": manifest["summary"],
        "refresh_mode": "full_rebuild",
    }


def update_session(session_dir: str) -> dict[str, Any]:
    """Refresh the manifest from the files already present in a session."""
    root = Path(session_dir).resolve()
    existing_manifest = read_json(root / "manifest.json")
    packets = load_packets(root)
    manifest = refresh_session_files(
        session_dir=root,
        packets=packets,
        workbook_path=existing_manifest["workbook_path"],
        sheet_name=existing_manifest.get("sheet_name") or "Ideations",
        batch_id=existing_manifest["batch_id"],
    )
    return {
        "session_root": str(root),
        "manifest_path": str((root / "manifest.json").resolve()),
        "summary": manifest["summary"],
        "refresh_mode": "session_files_only",
    }


def next_batch(
    session_dir: str,
    rows: list[int] | None = None,
    limit: int = 3,
) -> dict[str, Any]:
    """Return the next raw collection tasks for Claude."""
    root = Path(session_dir).resolve()
    tasks = next_collection_tasks(root, rows=rows, limit=limit)
    return {
        "session_root": str(root),
        "prompt_template": str((root / INSTRUCTION_DIR / "STEP4_PROMPT.md").resolve()),
        "task_count": len(tasks),
        "tasks": tasks,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Initialize or refresh a resumable Step 4 research session."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Create or refresh a research session.")
    init_parser.add_argument(
        "workbook",
        nargs="?",
        default=str(DEFAULT_WORKBOOK),
        help="Path to the filled PRD research workbook.",
    )
    init_parser.add_argument("--session-name", default=None, help="Optional stable session name.")
    init_parser.add_argument("--output-root", default=None, help="Optional custom session root.")
    init_parser.add_argument("--sheet", default="Ideations", help="Worksheet name to parse.")
    init_parser.add_argument("--postgres-json", default=None, help="Optional Postgres enrichment JSON.")
    init_parser.add_argument("--include-queries", action="store_true", help="Embed MCP query templates in packets.")
    init_parser.add_argument("--include-stackline-raw", action="store_true", help="Embed full Stackline analysis in packets.")
    init_parser.add_argument("--start-date", default=None, help="Override MCP start date.")
    init_parser.add_argument("--end-date", default=None, help="Override MCP end date.")
    init_parser.add_argument("--stackline-folder", default=None, help="Override Stackline export folder.")
    init_parser.add_argument("--stackline-brand", default="Sunco Lighting", help="Internal brand name for Stackline.")

    update_parser = subparsers.add_parser("update", help="Refresh manifest status for an existing session.")
    update_parser.add_argument("session_root", help="Path to an initialized research session.")

    status_parser = subparsers.add_parser("status", help="Show summary status plus next raw collection tasks.")
    status_parser.add_argument("session_root", help="Path to an initialized research session.")
    status_parser.add_argument("--rows", default=None, help="Optional comma-separated row numbers.")
    status_parser.add_argument("--limit", type=int, default=3, help="Maximum number of next tasks to return.")

    next_batch_parser = subparsers.add_parser("next-batch", help="Return the next raw collection tasks.")
    next_batch_parser.add_argument("session_root", help="Path to an initialized research session.")
    next_batch_parser.add_argument("--rows", default=None, help="Optional comma-separated row numbers.")
    next_batch_parser.add_argument("--limit", type=int, default=3, help="Maximum number of tasks to return.")

    validate_parser = subparsers.add_parser("validate", help="Validate raw collection artifacts.")
    validate_parser.add_argument("session_root", help="Path to an initialized research session.")
    validate_parser.add_argument("--rows", default=None, help="Optional comma-separated row numbers.")

    repair_parser = subparsers.add_parser("repair-raw", help="Repair common raw collection schema issues.")
    repair_parser.add_argument("session_root", help="Path to an initialized research session.")
    repair_parser.add_argument("--rows", default=None, help="Optional comma-separated row numbers.")

    args = parser.parse_args()

    if args.command == "init":
        result = initialize_session(
            workbook_path=args.workbook,
            session_name=args.session_name,
            output_root=args.output_root,
            postgres_json=args.postgres_json,
            include_queries=args.include_queries,
            include_stackline_raw=args.include_stackline_raw,
            start_date=args.start_date,
            end_date=args.end_date,
            stackline_folder=args.stackline_folder,
            stackline_brand=args.stackline_brand,
            sheet_name=args.sheet,
        )
    elif args.command == "update":
        result = update_session(args.session_root)
    elif args.command == "status":
        result = session_status(
            args.session_root,
            rows=parse_rows_argument(args.rows),
            limit=args.limit,
        )
    elif args.command == "next-batch":
        result = next_batch(
            args.session_root,
            rows=parse_rows_argument(args.rows),
            limit=args.limit,
        )
    elif args.command == "repair-raw":
        result = repair_raw_artifacts(
            args.session_root,
            rows=parse_rows_argument(args.rows),
        )
    else:
        result = validate_raw_artifacts(
            args.session_root,
            rows=parse_rows_argument(args.rows),
        )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
