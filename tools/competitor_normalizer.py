"""
Step 4F: Normalize raw competitor artifacts into comparable row-level outputs.

Usage:
  python tools/competitor_normalizer.py "C:\\path\\to\\research_session"
  python tools/competitor_normalizer.py "C:\\path\\to\\research_session" --rows 3,4
"""

from __future__ import annotations

import argparse
import hashlib
import re
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from research_session_manager import (
    SCHEMA_VERSION,
    STAGE_DEFINITIONS,
    artifact_path_for,
    packet_path_for,
    read_json,
    update_session,
    utc_now,
    write_json,
)


COLLECTION_STAGE_KEYS = [
    "amazon_collection",
    "brick_and_mortar_collection",
    "brand_site_collection",
]

KNOWN_CERTIFICATIONS = [
    "UL",
    "DLC",
    "ETL",
    "FCC",
    "RoHS",
    "Energy Star",
]

IGNORED_ARTIFACT_NOTES = {
    "populate this file using schemas/collection-artifact.schema.json.",
}


def normalize_text(value: Any) -> str | None:
    """Normalize optional values into stripped strings."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "Yes" if value else "No"
    text = str(value).strip()
    return text or None


def unique_preserve_order(values: list[str]) -> list[str]:
    """Return unique values while preserving order."""
    seen = set()
    result = []
    for value in values:
        if not value:
            continue
        marker = value.lower()
        if marker in seen:
            continue
        seen.add(marker)
        result.append(value)
    return result


def clean_artifact_notes(values: list[str], artifact_status: str) -> list[str]:
    """Drop scaffold notes and ignore untouched placeholder artifacts."""
    if artifact_status in {"not_started", "missing"}:
        return []

    cleaned: list[str] = []
    for value in values:
        text = normalize_text(value)
        if not text:
            continue
        if text.lower() in IGNORED_ARTIFACT_NOTES:
            continue
        cleaned.append(text)
    return cleaned


def compact_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Drop null / empty values from a dict."""
    return {
        key: value
        for key, value in data.items()
        if value not in (None, "", [], {})
    }


def parse_number(value: Any) -> float | None:
    """Parse numeric-ish text into a float."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    cleaned = (
        text.replace("$", "")
        .replace("%", "")
        .replace(",", "")
        .replace("x", "")
        .replace("X", "")
    )
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_bool(value: Any) -> bool | None:
    """Parse common boolean-like values."""
    if isinstance(value, bool):
        return value
    text = normalize_text(value)
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"yes", "true", "1"}:
        return True
    if lowered in {"no", "false", "0"}:
        return False
    return None


def listify(value: Any) -> list[str]:
    """Normalize strings / arrays into a cleaned list."""
    if value is None:
        return []
    if isinstance(value, list):
        items = [normalize_text(item) for item in value]
        return [item for item in items if item]
    text = normalize_text(value)
    if not text:
        return []
    return [
        part.strip()
        for part in re.split(r",|;", text)
        if part and part.strip()
    ]


def detect_source_domain(url: str | None) -> str | None:
    """Extract the hostname from a URL-like string."""
    if not url:
        return None
    parsed = urlparse(url)
    return parsed.netloc or None


def infer_pack_quantity(*values: Any) -> float | None:
    """Infer pack quantity from explicit fields or title text."""
    for value in values:
        number = parse_number(value)
        if number is not None and number > 0:
            return number

    combined = " ".join([text for text in [normalize_text(value) for value in values] if text])
    if not combined:
        return None

    patterns = [
        r"\b(\d+)\s*(?:pack|pk)\b",
        r"\bpack of (\d+)\b",
        r"\b(\d+)pack\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, combined, flags=re.IGNORECASE)
        if match:
            return float(match.group(1))
    return None


def extract_all(pattern: str, text: str | None) -> list[str]:
    """Return all regex matches from a text string."""
    if not text:
        return []
    return re.findall(pattern, text, flags=re.IGNORECASE)


def format_series(values: list[str], suffix: str = "") -> str | None:
    """Normalize slash-separated numeric series into a stable string."""
    ordered = unique_preserve_order([value.strip() for value in values if value and value.strip()])
    if not ordered:
        return None
    joined = "/".join(ordered)
    return f"{joined}{suffix}" if suffix else joined


def infer_wattage(value: Any, title: str | None) -> str | None:
    """Infer wattage text."""
    explicit = normalize_text(value)
    if explicit:
        return explicit
    title_text = title or ""
    series_match = re.search(
        r"\b((?:\d+(?:\.\d+)?\s*w?\s*/\s*)+\d+(?:\.\d+)?)\s*w\b",
        title_text,
        flags=re.IGNORECASE,
    )
    if series_match:
        numbers = re.findall(r"\d+(?:\.\d+)?", series_match.group(0))
        return format_series(numbers, "W")

    matches = extract_all(r"(\d+(?:\.\d+)?)\s*w\b", title_text)
    if not matches:
        return None
    return format_series(matches, "W")


def infer_lumens(value: Any, title: str | None) -> str | None:
    """Infer lumen text."""
    explicit = normalize_text(value)
    if explicit:
        return explicit
    matches = extract_all(r"(\d[\d,]{2,6})\s*(?:lm|lumens?)\b", title)
    if not matches:
        return None
    ordered = unique_preserve_order([match.replace(",", "") for match in matches])
    if len(ordered) == 1:
        return ordered[0]
    return "/".join(ordered)


def infer_cct(value: Any, title: str | None) -> str | None:
    """Infer color temperature text."""
    explicit = normalize_text(value)
    if explicit:
        return explicit
    title_text = title or ""
    series_match = re.search(
        r"\b((?:\d{4}\s*k?\s*/\s*)+\d{4})\s*k\b",
        title_text,
        flags=re.IGNORECASE,
    )
    if series_match:
        numbers = re.findall(r"\d{4}", series_match.group(0))
        return format_series([f"{number}K" for number in numbers])

    matches = extract_all(r"\b(\d{4})\s*k\b", title_text)
    if not matches:
        return None
    return format_series([f"{match}K" for match in matches])


def infer_cri(value: Any, title: str | None) -> str | None:
    """Infer CRI text."""
    explicit = normalize_text(value)
    if explicit:
        return explicit
    match = re.search(r"\b(\d{2,3}\+?)\s*cri\b", title or "", flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def infer_voltage(value: Any, title: str | None) -> str | None:
    """Infer voltage text."""
    explicit = normalize_text(value)
    if explicit:
        return explicit
    search_text = re.sub(r"\b0\s*-\s*10\s*v\b", " ", title or "", flags=re.IGNORECASE)
    search_text = re.sub(r"\b10\s*-\s*100\s*%\b", " ", search_text, flags=re.IGNORECASE)
    range_match = re.search(
        r"\b(?:ac\s*)?(\d{2,4})\s*-\s*(\d{2,4})\s*v\b",
        search_text,
        flags=re.IGNORECASE,
    )
    if range_match:
        return f"{range_match.group(1)}-{range_match.group(2)}V"
    single_match = re.search(
        r"\b(?:ac\s*)?(\d{2,4})\s*v\b",
        search_text,
        flags=re.IGNORECASE,
    )
    if single_match:
        return f"{single_match.group(1)}V"
    return None


def infer_dimming_type(value: Any, title: str | None) -> str | None:
    """Infer dimming type."""
    explicit = normalize_text(value)
    if explicit:
        return explicit
    title_text = title or ""
    if "0-10v" in title_text.lower():
        return "0-10V"
    if "triac" in title_text.lower():
        return "Triac"
    return None


def infer_dimmable(value: Any, title: str | None, dimming_type: str | None) -> bool | None:
    """Infer whether a product is dimmable."""
    explicit = parse_bool(value)
    if explicit is not None:
        return explicit
    title_text = (title or "").lower()
    if "non-dimmable" in title_text:
        return False
    if "dimmable" in title_text or dimming_type:
        return True
    return None


def infer_certifications(value: Any, title: str | None) -> list[str]:
    """Infer certifications from explicit fields or title text."""
    explicit = listify(value)
    if explicit:
        return unique_preserve_order(explicit)
    title_text = title or ""
    found = []
    for label in KNOWN_CERTIFICATIONS:
        if label.lower() in title_text.lower():
            found.append(label)
    return unique_preserve_order(found)


def infer_features(existing: Any, title: str | None) -> list[str]:
    """Infer a compact feature list from explicit fields or title text."""
    features = listify(existing)
    title_text = (title or "").lower()

    candidates = [
        ("dimmable", "dimmable"),
        ("0-10V dimming", "0-10v"),
        ("motion sensor", "motion sensor"),
        ("emergency battery backup", "emergency"),
        ("selectable wattage", "30w/40w/50w"),
        ("selectable cct", "3500k/4000k/5000k"),
        ("wet rated", "wet"),
        ("damp rated", "damp"),
        ("dry rated", "dry"),
    ]
    for label, token in candidates:
        if token in title_text:
            features.append(label)
    return unique_preserve_order(features)


def build_candidate_id(record: dict[str, Any]) -> str:
    """Build a stable candidate id for dedupe / traceability."""
    parts = [
        normalize_text(record.get("source_channel")) or "",
        normalize_text(record.get("url")) or "",
        normalize_text(record.get("brand")) or "",
        normalize_text(record.get("model_number")) or "",
        normalize_text(record.get("product_title")) or "",
    ]
    payload = "|".join(parts).encode("utf-8")
    return hashlib.md5(payload).hexdigest()[:12]


def normalize_record(item: dict[str, Any], fallback_channel: str) -> dict[str, Any] | None:
    """Convert a raw / seed item into the shared competitor-result shape."""
    title = normalize_text(item.get("product_title") or item.get("title"))
    brand = normalize_text(item.get("brand"))
    source_channel = normalize_text(item.get("source_channel")) or fallback_channel
    collection_method = normalize_text(item.get("collection_method"))
    if not title or not brand or not source_channel:
        return None

    url = normalize_text(item.get("url"))
    if not url and source_channel == "stackline_seed":
        synthetic = build_candidate_id(
            {
                "source_channel": source_channel,
                "brand": brand,
                "model_number": item.get("model_number"),
                "product_title": title,
                "url": "",
            }
        )
        url = f"stackline://seed/{synthetic}"

    if not url:
        return None

    dimming_type = infer_dimming_type(item.get("dimming_type"), title)
    record = {
        "candidate_id": normalize_text(item.get("candidate_id")),
        "source_channel": source_channel,
        "source_domain": normalize_text(item.get("source_domain")) or detect_source_domain(url),
        "collection_method": collection_method or ("manual" if source_channel != "stackline_seed" else "stackline_seed"),
        "brand": brand,
        "product_title": title,
        "model_number": normalize_text(item.get("model_number")),
        "sku": normalize_text(item.get("sku")),
        "variant": normalize_text(item.get("variant")),
        "pack_quantity": infer_pack_quantity(
            item.get("pack_quantity"),
            item.get("variant"),
            title,
        ),
        "url": url,
        "price": parse_number(item.get("price") or item.get("avg_retail_price")),
        "currency": normalize_text(item.get("currency")) or "USD",
        "wattage": infer_wattage(item.get("wattage"), title),
        "lumens": infer_lumens(item.get("lumens"), title),
        "cct": infer_cct(item.get("cct"), title),
        "cri": infer_cri(item.get("cri"), title),
        "voltage": infer_voltage(item.get("voltage"), title),
        "dimmable": infer_dimmable(item.get("dimmable"), title, dimming_type),
        "dimming_type": dimming_type,
        "certifications": infer_certifications(item.get("certifications"), title),
        "features": infer_features(item.get("features"), title),
        "rating": parse_number(item.get("rating")),
        "review_count": parse_number(item.get("review_count")),
        "availability": normalize_text(item.get("availability")),
        "match_confidence": parse_number(item.get("match_confidence")),
        "match_notes": normalize_text(item.get("match_notes")),
        "extraction_notes": normalize_text(item.get("extraction_notes")),
        "raw_observations": unique_preserve_order(
            listify(item.get("raw_observations"))
            + listify(item.get("notes"))
        ),
    }
    record["candidate_id"] = record["candidate_id"] or build_candidate_id(record)
    return compact_dict(record)


def merge_records(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    """Merge two likely-duplicate competitor records."""
    result = dict(base)
    for key, value in incoming.items():
        if value in (None, "", [], {}):
            continue
        if key in {"certifications", "features", "raw_observations"}:
            merged = list(result.get(key, [])) + list(value)
            result[key] = unique_preserve_order(merged)
            continue
        if key == "match_confidence":
            current = parse_number(result.get(key))
            candidate = parse_number(value)
            if candidate is not None and (current is None or candidate > current):
                result[key] = candidate
            continue
        if result.get(key) in (None, "", [], {}):
            result[key] = value
    return result


def dedupe_key(record: dict[str, Any]) -> str:
    """Return a dedupe key that prefers real URLs over loose title matches."""
    url = normalize_text(record.get("url"))
    if url and not url.startswith("stackline://"):
        return f"url::{url.lower()}"
    brand = normalize_text(record.get("brand")) or ""
    model = normalize_text(record.get("model_number")) or ""
    title = normalize_text(record.get("product_title")) or ""
    pack = normalize_text(record.get("pack_quantity")) or ""
    if model:
        return f"model::{brand.lower()}::{model.lower()}::{pack}"
    return f"title::{brand.lower()}::{title.lower()}::{pack}"


def dedupe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge likely duplicate records."""
    merged: dict[str, dict[str, Any]] = {}
    for record in records:
        key = dedupe_key(record)
        if key not in merged:
            merged[key] = record
            continue
        merged[key] = merge_records(merged[key], record)
    return list(merged.values())


def load_artifact_items(path: Path, fallback_channel: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Load and normalize items from one artifact file."""
    if not path.exists():
        return [], {"artifact_status": "missing", "queries_used": [], "blocking_issues": []}
    payload = read_json(path)
    items = []
    for item in payload.get("items", []):
        if not isinstance(item, dict):
            continue
        normalized = normalize_record(item, fallback_channel=fallback_channel)
        if normalized:
            items.append(normalized)
    return items, {
        "artifact_status": payload.get("artifact_status"),
        "queries_used": payload.get("queries_used", []),
        "blocking_issues": payload.get("blocking_issues", []),
        "notes": payload.get("notes", []),
    }


def build_stackline_seed_records(packet: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert packet-level Stackline seeds into normalized competitor records."""
    seeds = []
    for seed in packet.get("research_plan", {}).get("amazon", {}).get("competitor_seeds", []):
        if not isinstance(seed, dict):
            continue
        record = normalize_record(
            {
                "source_channel": "stackline_seed",
                "collection_method": "stackline_seed",
                "brand": seed.get("brand"),
                "product_title": seed.get("title"),
                "model_number": seed.get("model_number"),
                "price": seed.get("avg_retail_price"),
                "raw_observations": [
                    f"Stackline units_sold={seed.get('units_sold')}",
                    f"Stackline sales_share_pct={seed.get('sales_share_pct')}",
                ],
            },
            fallback_channel="stackline_seed",
        )
        if record:
            seeds.append(record)
    return seeds


def derive_normalized_status(
    raw_stage_statuses: list[str],
    item_count: int,
    stackline_seed_count: int,
    blocking_issues: list[str],
) -> str:
    """Derive the normalized artifact status from upstream state."""
    if any(status == "blocked" for status in raw_stage_statuses) and item_count == 0:
        return "blocked"
    if all(status == "complete" for status in raw_stage_statuses):
        return "complete"
    if item_count > 0 or stackline_seed_count > 0:
        return "in_progress"
    if blocking_issues:
        return "blocked"
    return "not_started"


def build_summary(
    raw_stage_statuses: dict[str, str],
    raw_counts: dict[str, int],
    stackline_seed_count: int,
    pre_dedupe_count: int,
    final_count: int,
) -> dict[str, Any]:
    """Build normalized artifact summary fields."""
    return {
        "raw_stage_statuses": raw_stage_statuses,
        "raw_item_counts": raw_counts,
        "stackline_seed_count": stackline_seed_count,
        "pre_dedupe_count": pre_dedupe_count,
        "final_item_count": final_count,
    }


def build_normalized_artifact(session_dir: Path, row_number: int) -> dict[str, Any] | None:
    """Build one normalized artifact from the row packet plus raw artifacts."""
    packet = read_json(packet_path_for(session_dir, row_number))

    raw_records: list[dict[str, Any]] = []
    raw_stage_statuses: dict[str, str] = {}
    raw_counts: dict[str, int] = {}
    queries_used: list[str] = []
    blocking_issues: list[str] = []
    notes: list[str] = []

    stage_to_channel = {
        "amazon_collection": "amazon",
        "brick_and_mortar_collection": "home_depot",
        "brand_site_collection": "brand_site",
    }

    for stage_key in COLLECTION_STAGE_KEYS:
        artifact_path = artifact_path_for(session_dir, row_number, stage_key)
        records, meta = load_artifact_items(artifact_path, fallback_channel=stage_to_channel[stage_key])
        raw_records.extend(records)
        artifact_status = str(meta.get("artifact_status") or "missing")
        raw_stage_statuses[stage_key] = artifact_status
        raw_counts[stage_key] = len(records)
        queries_used.extend(meta.get("queries_used", []))
        blocking_issues.extend(meta.get("blocking_issues", []))
        notes.extend(clean_artifact_notes(meta.get("notes", []), artifact_status))

    stackline_seed_records = build_stackline_seed_records(packet)
    combined = raw_records + stackline_seed_records

    if not combined and all(status in {"not_started", "missing"} for status in raw_stage_statuses.values()):
        return None

    deduped = dedupe_records(combined)
    normalized_status = derive_normalized_status(
        raw_stage_statuses=list(raw_stage_statuses.values()),
        item_count=len(deduped),
        stackline_seed_count=len(stackline_seed_records),
        blocking_issues=blocking_issues,
    )

    if not deduped and normalized_status == "not_started":
        return None

    packet_rel = str(packet_path_for(session_dir, row_number).resolve().relative_to(session_dir.resolve()))
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "artifact_type": STAGE_DEFINITIONS["normalized"]["artifact_type"],
        "artifact_status": normalized_status,
        "batch_id": session_dir.name,
        "row_number": row_number,
        "ideation_name": packet["identity"].get("ideation_name"),
        "expected_owner": "codex",
        "source_channel_group": "all_channels",
        "packet_file": packet_rel,
        "queries_used": unique_preserve_order(queries_used),
        "items": deduped,
        "summary": build_summary(
            raw_stage_statuses=raw_stage_statuses,
            raw_counts=raw_counts,
            stackline_seed_count=len(stackline_seed_records),
            pre_dedupe_count=len(combined),
            final_count=len(deduped),
        ),
        "notes": unique_preserve_order(
            notes
            + (
                ["Seeded with Stackline competitor context from the row packet."]
                if stackline_seed_records
                else []
            )
            + (
                ["No normalized competitors available yet; waiting on raw collection."]
                if not deduped
                else []
            )
        ),
        "blocking_issues": unique_preserve_order(blocking_issues),
        "updated_at": utc_now(),
    }
    return compact_dict(artifact)


def parse_rows_argument(value: str | None) -> list[int] | None:
    """Parse a comma-separated list of row numbers."""
    if not value:
        return None
    rows = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        rows.append(int(part))
    return rows or None


def normalize_session(session_root: str, rows: list[int] | None = None) -> dict[str, Any]:
    """Normalize selected rows for a session and refresh its manifest."""
    session_dir = Path(session_root).resolve()
    manifest = read_json(session_dir / "manifest.json")
    target_rows = set(rows or [row["row_number"] for row in manifest.get("rows", [])])

    written_rows = []
    skipped_rows = []
    total_items = 0

    for row in manifest.get("rows", []):
        row_number = row["row_number"]
        if row_number not in target_rows:
            continue
        artifact = build_normalized_artifact(session_dir, row_number)
        if artifact is None:
            skipped_rows.append(row_number)
            continue
        write_json(artifact_path_for(session_dir, row_number, "normalized"), artifact)
        written_rows.append(row_number)
        total_items += len(artifact.get("items", []))

    update_result = update_session(str(session_dir))
    return {
        "session_root": str(session_dir),
        "rows_requested": sorted(target_rows),
        "rows_written": written_rows,
        "rows_skipped": skipped_rows,
        "normalized_item_count": total_items,
        "manifest_summary": update_result["summary"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Normalize raw competitor collection artifacts for a session."
    )
    parser.add_argument(
        "session_root",
        help="Path to an initialized research session.",
    )
    parser.add_argument(
        "--rows",
        default=None,
        help="Optional comma-separated row numbers to normalize.",
    )
    args = parser.parse_args()

    result = normalize_session(
        session_root=args.session_root,
        rows=parse_rows_argument(args.rows),
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
