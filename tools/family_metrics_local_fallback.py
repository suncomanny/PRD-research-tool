"""
Backfill family-metrics payloads from local sales exports when Postgres is unavailable.

This is intentionally narrow:
- fills Amazon monthly sales rows from the FY2025 local Amazon export
- does not attempt customer concentration
- does not fabricate Shopify monthly history

Usage:
  python tools/family_metrics_local_fallback.py "C:\\path\\to\\family_metrics_payload_template.json"
  python tools/family_metrics_local_fallback.py "C:\\path\\to\\family_metrics_payload_template.json" --output enriched.json
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sku_lookup import AMAZON_SALES_FILE, LOCAL_SALES_PERIOD_LABEL, load_amazon_sales


AMAZON_CHANNEL_ID = 11929


def utc_now() -> str:
    """Return an ISO timestamp for artifact generation."""
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def normalize_text(value: Any) -> str:
    """Normalize arbitrary values into comparable uppercase text."""
    if value is None:
        return ""
    return str(value).strip().upper()


def load_payload_rows(path: Path) -> list[dict[str, Any]]:
    """Load the merge-ready family-metrics payload template."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("family-metrics payload must be a list of row payload objects.")
    return [row for row in data if isinstance(row, dict)]


def merge_monthly_rows(
    existing_rows: list[dict[str, Any]],
    new_rows: list[dict[str, Any]],
    channel_id: int,
) -> list[dict[str, Any]]:
    """Merge replacement rows for one channel while preserving other channel data."""
    kept_rows = []
    for row in existing_rows:
        if not isinstance(row, dict):
            continue
        try:
            row_channel = int(float(row.get("sales_channel_id")))
        except (TypeError, ValueError):
            row_channel = None
        if row_channel == channel_id:
            continue
        kept_rows.append(row)
    return kept_rows + new_rows


def build_amazon_monthly_rows(family: str) -> tuple[list[dict[str, Any]], dict[str, str] | None]:
    """Aggregate monthly Amazon revenue and units for a SKU family from local export data."""
    sales_df = load_amazon_sales()
    if sales_df.empty:
        return [], None

    normalized_family = normalize_text(family)
    family_rows = sales_df[sales_df["family"].astype(str).str.upper() == normalized_family].copy()
    if family_rows.empty:
        return [], None

    family_rows["Year parsed"] = family_rows["Year"].astype(int)
    family_rows["Month parsed"] = family_rows["Month"].astype(int)

    grouped = (
        family_rows.groupby(["Year parsed", "Month parsed"], as_index=False)
        .agg(
            revenue=("Sales parsed", "sum"),
            units=("Units parsed", "sum"),
        )
        .sort_values(["Year parsed", "Month parsed"])
    )

    monthly_rows = []
    for record in grouped.to_dict(orient="records"):
        year = int(record["Year parsed"])
        month = int(record["Month parsed"])
        monthly_rows.append(
            {
                "month": f"{year:04d}-{month:02d}-01",
                "sales_channel_id": AMAZON_CHANNEL_ID,
                "revenue": round(float(record.get("revenue") or 0.0), 2),
                "units": int(round(float(record.get("units") or 0.0))),
                "distinct_customers": None,
            }
        )

    if not monthly_rows:
        return [], None

    period = {
        "start_date": monthly_rows[0]["month"],
        "end_date": monthly_rows[-1]["month"],
    }
    return monthly_rows, period


def enrich_payload_rows(payload_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fill local Amazon monthly-sales fallback rows into the payload."""
    matched_families = 0
    unmatched_families = []

    for row in payload_rows:
        family = normalize_text(row.get("family"))
        monthly_rows, period = build_amazon_monthly_rows(family)
        notes = row.setdefault("family_metrics_notes", [])
        if not isinstance(notes, list):
            notes = []
            row["family_metrics_notes"] = notes

        if not monthly_rows:
            unmatched_families.append(family)
            notes.append(
                f"No local Amazon monthly fallback rows were found in {AMAZON_SALES_FILE} for family {family}."
            )
            continue

        matched_families += 1
        row["monthly_sales"] = merge_monthly_rows(
            row.get("monthly_sales") if isinstance(row.get("monthly_sales"), list) else [],
            monthly_rows,
            AMAZON_CHANNEL_ID,
        )

        existing_source = normalize_text(row.get("family_metrics_source"))
        if existing_source and existing_source != "POSTGRES_MCP":
            row["family_metrics_source"] = "mixed_local_amazon_export_fallback"
        else:
            row["family_metrics_source"] = "local_amazon_export_fallback"

        periods = row.setdefault("family_metrics_period_label", {})
        if not isinstance(periods, dict):
            periods = {}
            row["family_metrics_period_label"] = periods
        periods["amazon_local_export_fallback"] = {
            "label": LOCAL_SALES_PERIOD_LABEL,
            "start_date": period["start_date"],
            "end_date": period["end_date"],
        }

        notes.append(
            "Amazon monthly sales were backfilled from the FY2025 local export fallback. "
            "This is a proxy source for Q2/Q4 and does not include customer concentration or full Postgres windows."
        )

    summary = {
        "generated_at": utc_now(),
        "matched_family_count": matched_families,
        "unmatched_family_count": len(unmatched_families),
        "unmatched_families": unmatched_families,
    }
    return payload_rows, summary


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Backfill family-metrics payloads from local Amazon sales export.")
    parser.add_argument("payload_json", help="Path to family_metrics_payload_template.json")
    parser.add_argument(
        "--output",
        help="Optional output JSON path. Defaults to overwriting the input payload file.",
    )
    return parser.parse_args()


def main() -> int:
    """CLI entry point."""
    args = parse_args()
    payload_path = Path(args.payload_json).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve() if args.output else payload_path

    payload_rows = load_payload_rows(payload_path)
    enriched_rows, summary = enrich_payload_rows(payload_rows)
    output_path.write_text(json.dumps(enriched_rows, indent=2), encoding="utf-8")
    print(json.dumps({"output_path": str(output_path), **summary}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
