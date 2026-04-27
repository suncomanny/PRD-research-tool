"""Audit merged reference SKU baselines for a research session.

This tool reads packet-level ``reference_baseline`` objects, optionally compares
them against the raw Postgres payload template, and writes both JSON and
Markdown audit artifacts into the session root.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


POSTGRES_PAYLOAD_FILENAME = "reference_postgres_payload_template.json"
JSON_OUTPUT_FILENAME = "reference_baseline_audit.json"
MARKDOWN_OUTPUT_FILENAME = "reference_baseline_audit.md"


@dataclass
class RowReference:
    row_number: int
    ideation_name: str | None


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def normalize_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def first_non_empty(values: list[Any]) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def packet_files(session_root: Path) -> list[Path]:
    return sorted((session_root / "packets").glob("row_*_packet.json"))


def build_packet_index(session_root: Path) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}

    for packet_path in packet_files(session_root):
        packet = load_json(packet_path)
        baseline = packet.get("reference_baseline") or {}
        sku = (baseline.get("sku") or "").strip()
        if not sku:
            continue

        identity = packet.get("identity") or {}
        row_ref = RowReference(
            row_number=int(packet.get("row_number")),
            ideation_name=identity.get("ideation_name"),
        )

        entry = grouped.setdefault(
            sku,
            {
                "sku": sku,
                "family": baseline.get("family"),
                "rows": [],
                "baseline_candidates": defaultdict(list),
            },
        )
        entry["rows"].append(row_ref)

        for field in (
            "title",
            "title_source",
            "listing_price",
            "listing_price_source",
            "listing_price_note",
            "listing_price_candidate_postgres",
            "shopify_revenue_12mo",
            "shopify_units_12mo",
            "amazon_revenue_12mo",
            "amazon_units_12mo",
            "shopify_data_source",
            "amazon_data_source",
            "reference_data_source",
            "sales_period_label",
            "image_url",
            "reference_sku_source",
        ):
            entry["baseline_candidates"][field].append(baseline.get(field))

    packet_index: dict[str, dict[str, Any]] = {}
    for sku, grouped_entry in grouped.items():
        rows: list[RowReference] = grouped_entry["rows"]
        candidates: defaultdict[str, list[Any]] = grouped_entry["baseline_candidates"]
        baseline = {
            field: first_non_empty(values)
            for field, values in candidates.items()
        }

        packet_index[sku] = {
            "sku": sku,
            "family": grouped_entry.get("family"),
            "row_numbers": sorted(row.row_number for row in rows),
            "ideations": [
                {
                    "row_number": row.row_number,
                    "ideation_name": row.ideation_name,
                }
                for row in sorted(rows, key=lambda item: item.row_number)
            ],
            "baseline": baseline,
        }

    return packet_index


def load_postgres_payload_index(session_root: Path) -> dict[str, dict[str, Any]]:
    payload_path = session_root / POSTGRES_PAYLOAD_FILENAME
    if not payload_path.exists():
        return {}

    payload = load_json(payload_path)
    return {
        item["sku"]: item
        for item in payload
        if isinstance(item, dict) and item.get("sku")
    }


def classify_sku(packet_entry: dict[str, Any], postgres_entry: dict[str, Any] | None) -> dict[str, Any]:
    baseline = packet_entry["baseline"]
    postgres_entry = postgres_entry or {}

    title = baseline.get("title")
    listing_price = normalize_number(baseline.get("listing_price"))
    shopify_revenue = normalize_number(baseline.get("shopify_revenue_12mo"))
    amazon_revenue = normalize_number(baseline.get("amazon_revenue_12mo"))

    title_present = bool(title)
    listing_present = listing_price is not None
    shopify_present = shopify_revenue is not None
    amazon_present = amazon_revenue is not None

    listing_note = baseline.get("listing_price_note")
    listing_source = baseline.get("listing_price_source")
    reference_source = baseline.get("reference_data_source")
    shopify_source = baseline.get("shopify_data_source")
    amazon_source = baseline.get("amazon_data_source")

    reasons: list[str] = []
    if not title_present:
        reasons.append("Missing reference title")
    if not listing_present:
        reasons.append("Missing listing price")
    if not shopify_present:
        reasons.append("Missing Shopify sales baseline")
    if not amazon_present:
        reasons.append("Missing Amazon sales baseline")
    if listing_note:
        reasons.append("Postgres listing price was rejected and fallback price was kept")
    if listing_present and listing_source != "postgres_mcp":
        reasons.append(f"Listing price relies on fallback source: {listing_source}")
    if shopify_present and shopify_source not in (None, "postgres_mcp"):
        reasons.append(f"Shopify sales rely on fallback source: {shopify_source}")
    if amazon_present and amazon_source not in (None, "postgres_mcp"):
        reasons.append(f"Amazon sales rely on fallback source: {amazon_source}")
    if reference_source and reference_source != "postgres_mcp_plus_metadata":
        reasons.append(f"Reference data source is fallback-oriented: {reference_source}")

    has_any_sales = shopify_present or amazon_present
    fully_trusted = (
        title_present
        and listing_present
        and listing_source == "postgres_mcp"
        and not listing_note
        and shopify_present
        and amazon_present
    )
    unresolved = (
        (not title_present and not listing_present and not has_any_sales)
        or (not title_present and not listing_present)
    )

    if fully_trusted:
        classification = "fully_trusted"
        recommended_action = "No action needed."
        reasons = ["Listing price and both major sales channels are backed by accepted Postgres data."]
    elif unresolved:
        classification = "unresolved_manual_followup"
        recommended_action = (
            "Correct the reference SKU or supply a manual baseline; current Postgres and fallback sources are insufficient."
        )
    else:
        classification = "trusted_with_fallback"
        recommended_action = (
            "Usable baseline exists, but keep the fallback/provenance notes in mind for pricing or channel comparisons."
        )

    return {
        "sku": packet_entry["sku"],
        "family": packet_entry.get("family") or postgres_entry.get("family"),
        "row_numbers": packet_entry["row_numbers"],
        "ideations": packet_entry["ideations"],
        "classification": classification,
        "recommended_action": recommended_action,
        "reasons": reasons,
        "merged_baseline": baseline,
        "postgres_payload": postgres_entry,
    }


def build_audit(session_root: Path) -> dict[str, Any]:
    packet_index = build_packet_index(session_root)
    postgres_index = load_postgres_payload_index(session_root)

    items = [
        classify_sku(packet_entry, postgres_index.get(sku))
        for sku, packet_entry in sorted(packet_index.items())
    ]

    summary_counts = defaultdict(int)
    for item in items:
        summary_counts[item["classification"]] += 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "session_root": str(session_root),
        "summary": {
            "total_unique_reference_skus": len(items),
            "fully_trusted": summary_counts["fully_trusted"],
            "trusted_with_fallback": summary_counts["trusted_with_fallback"],
            "unresolved_manual_followup": summary_counts["unresolved_manual_followup"],
        },
        "items": items,
    }


def format_currency(value: Any) -> str:
    numeric = normalize_number(value)
    if numeric is None:
        return "-"
    return f"${numeric:,.2f}"


def format_markdown(audit: dict[str, Any]) -> str:
    lines = [
        "# Reference Baseline Audit",
        "",
        f"- Generated: `{audit['generated_at']}`",
        f"- Session: `{audit['session_root']}`",
        f"- Unique reference SKUs: `{audit['summary']['total_unique_reference_skus']}`",
        f"- Fully trusted: `{audit['summary']['fully_trusted']}`",
        f"- Trusted with fallback: `{audit['summary']['trusted_with_fallback']}`",
        f"- Unresolved / manual follow-up: `{audit['summary']['unresolved_manual_followup']}`",
        "",
    ]

    grouped_items: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in audit["items"]:
        grouped_items[item["classification"]].append(item)

    section_order = [
        ("fully_trusted", "Fully Trusted"),
        ("trusted_with_fallback", "Trusted With Fallback"),
        ("unresolved_manual_followup", "Unresolved / Manual Follow-Up"),
    ]

    for key, title in section_order:
        lines.extend([f"## {title}", ""])
        items = grouped_items.get(key, [])
        if not items:
            lines.extend(["None.", ""])
            continue

        for item in items:
            baseline = item["merged_baseline"]
            row_numbers = ", ".join(str(number) for number in item["row_numbers"])
            lines.append(f"### {item['sku']}")
            lines.append("")
            lines.append(f"- Rows: `{row_numbers}`")
            lines.append(
                "- Ideations: "
                + "; ".join(
                    f"`{ref['row_number']}` {ref['ideation_name']}"
                    for ref in item["ideations"]
                )
            )
            lines.append(f"- Listing price: `{format_currency(baseline.get('listing_price'))}` via `{baseline.get('listing_price_source') or '-'}`")
            lines.append(
                f"- Sales coverage: Shopify `{format_currency(baseline.get('shopify_revenue_12mo'))}`, Amazon `{format_currency(baseline.get('amazon_revenue_12mo'))}`"
            )
            lines.append(f"- Recommended action: {item['recommended_action']}")
            if item["reasons"]:
                lines.append("- Notes:")
                for reason in item["reasons"]:
                    lines.append(f"  - {reason}")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def write_outputs(audit: dict[str, Any], json_path: Path, markdown_path: Path) -> None:
    json_path.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    markdown_path.write_text(format_markdown(audit), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit reference baseline quality for a research session.")
    parser.add_argument("session_root", help="Path to the research session root.")
    parser.add_argument(
        "--output-json",
        default=None,
        help=f"Optional JSON output path. Defaults to <session_root>\\{JSON_OUTPUT_FILENAME}.",
    )
    parser.add_argument(
        "--output-md",
        default=None,
        help=f"Optional Markdown output path. Defaults to <session_root>\\{MARKDOWN_OUTPUT_FILENAME}.",
    )
    args = parser.parse_args()

    session_root = Path(args.session_root)
    json_path = Path(args.output_json) if args.output_json else session_root / JSON_OUTPUT_FILENAME
    markdown_path = Path(args.output_md) if args.output_md else session_root / MARKDOWN_OUTPUT_FILENAME

    audit = build_audit(session_root)
    write_outputs(audit, json_path, markdown_path)
    print(json.dumps({"json": str(json_path), "markdown": str(markdown_path), "summary": audit["summary"]}, indent=2))


if __name__ == "__main__":
    main()
