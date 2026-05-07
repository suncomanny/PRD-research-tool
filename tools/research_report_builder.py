"""
Step 6A: Build Excel research reports from completed analysis artifacts.

Usage:
  python tools/research_report_builder.py "C:\\path\\to\\research_session"
  python tools/research_report_builder.py "C:\\path\\to\\research_session" --rows 3,4,5
  python tools/research_report_builder.py "C:\\path\\to\\research_session" --combined
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill

from research_session_manager import artifact_path_for, packet_path_for, read_json, update_session


HEADER_FILL = PatternFill(fill_type="solid", fgColor="1F4E78")
SUBHEADER_FILL = PatternFill(fill_type="solid", fgColor="D9E2F3")
ACCENT_FILL = PatternFill(fill_type="solid", fgColor="EAF2F8")
HEADER_FONT = Font(color="FFFFFF", bold=True, size=12)
SUBHEADER_FONT = Font(bold=True)
TITLE_FONT = Font(bold=True, size=15)
HYPERLINK_FONT = Font(color="0563C1", underline="single")
WRAP_ALIGNMENT = Alignment(vertical="top", wrap_text=True)
DEFAULT_COLUMN_WIDTHS = {
    "A": 22,
    "B": 28,
    "C": 18,
    "D": 18,
    "E": 18,
    "F": 18,
    "G": 18,
    "H": 24,
    "I": 18,
    "J": 36,
    "K": 14,
}
ALTERNATE_ROW_FILL = PatternFill(fill_type="solid", fgColor="F7FBFF")
AMAZON_CHANNELS = {"amazon"}
BM_DIRECT_CHANNELS = {"home_depot", "walmart", "lowes", "brand_site"}
CHANNEL_DISPLAY_NAMES = {
    "amazon": "Amazon",
    "home_depot": "Home Depot",
    "walmart": "Walmart",
    "lowes": "Lowe's",
    "brand_site": "Brand Site",
    "stackline_seed": "Stackline",
}

SUMMARY_METRIC_GUIDE_ROWS = [
    [
        "Outlook",
        "Directional recommendation for the ideation in its current configuration, such as favorable, mixed, or cautious.",
        "Given the category, competitor, pricing, and feature context, should this concept look attractive to pursue as configured?",
    ],
    [
        "Confidence",
        "Overall confidence in the launch outlook based on the quality, consistency, and completeness of the supporting research.",
        "How much trust should I place in the outlook recommendation?",
    ],
    [
        "Amazon G2",
        "Weighted Amazon Gate 2 readiness score on a 0-10 scale using the current rubric and available evidence.",
        "How ready is this concept to move forward for Amazon Gate 2 review?",
    ],
    [
        "Amazon Evidence",
        "Confidence label for the Amazon G2 score based on how direct, complete, and well-supported the underlying data is.",
        "How strong is the evidence behind the Amazon G2 score?",
    ],
]


def resolved_source_channel(item: dict[str, Any]) -> str:
    """Return the effective source channel, preferring explicit retailer domains."""
    raw_channel = (optional_text(item.get("source_channel")) or "").lower()
    url = optional_text(item.get("url")) or ""
    detected_domain = urlparse(url).netloc.lower() if "://" in url else ""
    domain = detected_domain or (optional_text(item.get("source_domain")) or "")
    if domain:
        host = urlparse(domain).netloc.lower() if "://" in domain else domain.lower()
        host = host.replace("www.", "")
        if host == "amazon.com" or host.endswith(".amazon.com"):
            return "amazon"
        if host == "homedepot.com" or host.endswith(".homedepot.com"):
            return "home_depot"
        if host == "walmart.com" or host.endswith(".walmart.com"):
            return "walmart"
        if host == "lowes.com" or host.endswith(".lowes.com"):
            return "lowes"
    return raw_channel


def normalize_text(value: Any) -> str:
    """Render values as readable worksheet strings."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, list):
        return ", ".join([normalize_text(item) for item in value if normalize_text(item)])
    return str(value)


def as_dict(value: Any) -> dict[str, Any]:
    """Coerce optional mappings into dicts."""
    if isinstance(value, dict):
        return value
    return {}


def as_list(value: Any) -> list[Any]:
    """Coerce optional sequences into lists."""
    if isinstance(value, list):
        return value
    return []


def set_default_layout(ws) -> None:
    """Apply shared column widths and wrapping."""
    ws.freeze_panes = "A4"
    for column, width in DEFAULT_COLUMN_WIDTHS.items():
        ws.column_dimensions[column].width = width


def safe_sheet_title(base: str, used: set[str]) -> str:
    """Create a workbook-safe, unique worksheet title."""
    text = (base or "Report").replace("/", " ").replace("\\", " ").replace(":", " ").strip()
    text = text[:31] or "Report"
    candidate = text
    suffix = 2
    while candidate in used:
        trimmed = text[: max(0, 31 - len(f" ({suffix})"))]
        candidate = f"{trimmed} ({suffix})"
        suffix += 1
    used.add(candidate)
    return candidate


def section_header(ws, row: int, title: str, end_column: int = 10) -> int:
    """Write a section header row and return the next row index."""
    ws.cell(row=row, column=1, value=title)
    ws.cell(row=row, column=1).fill = HEADER_FILL
    ws.cell(row=row, column=1).font = HEADER_FONT
    ws.cell(row=row, column=1).alignment = WRAP_ALIGNMENT
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=end_column)
    return row + 1


def key_value_rows(ws, row: int, pairs: list[tuple[str, Any]], columns: int = 2) -> int:
    """Write compact key/value pairs across the sheet."""
    index = 0
    while index < len(pairs):
        for block in range(columns):
            if index >= len(pairs):
                break
            label, value = pairs[index]
            base_col = (block * 2) + 1
            ws.cell(row=row, column=base_col, value=label)
            ws.cell(row=row, column=base_col).font = SUBHEADER_FONT
            ws.cell(row=row, column=base_col).fill = SUBHEADER_FILL
            ws.cell(row=row, column=base_col + 1, value=normalize_text(value))
            ws.cell(row=row, column=base_col + 1).alignment = WRAP_ALIGNMENT
            index += 1
        row += 1
    return row


def gate_snapshot(gate_readiness: dict[str, Any], channel: str, gate: str) -> dict[str, Any]:
    """Return the matching gate snapshot when present."""
    target_channel = channel.strip().lower()
    target_gate = gate.strip().upper()
    for snapshot in as_list(gate_readiness.get("snapshots")):
        snapshot = as_dict(snapshot)
        if (
            normalize_text(snapshot.get("channel")).strip().lower() == target_channel
            and normalize_text(snapshot.get("gate")).strip().upper() == target_gate
        ):
            return snapshot
    return {}


def issue_sentence(parts: list[str]) -> str:
    """Join short issue fragments into one readable sentence."""
    parts = [part for part in parts if part]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f"{parts[0]} and {parts[1]}"
    return f"{', '.join(parts[:-1])}, and {parts[-1]}"


def blocker_action_summary(packet: dict[str, Any], analysis: dict[str, Any]) -> dict[str, Any]:
    """Build a compact top-of-report blocker/action summary."""
    identity = as_dict(packet.get("identity"))
    performance = as_dict(analysis.get("performance_estimation"))
    pricing = as_dict(analysis.get("pricing_analysis"))
    spec_coverage = as_dict(analysis.get("spec_coverage"))
    gate_readiness = as_dict(analysis.get("gate_readiness"))

    suggested = as_dict(pricing.get("suggested_msrp_range"))
    margin_targets = as_dict(pricing.get("margin_targets"))
    amazon_margin = as_dict(margin_targets.get("amazon"))
    amazon_snapshot = gate_snapshot(gate_readiness, "amazon", "G2")

    outlook = normalize_text(performance.get("launch_outlook")).strip().lower() or "mixed"
    confidence = normalize_text(performance.get("confidence")).strip().lower() or "medium"
    posture = normalize_text(performance.get("posture")).strip().lower() or "undetermined"
    positioning = normalize_text(suggested.get("positioning")).strip().lower() or "undetermined"
    evidence_label = normalize_text(amazon_snapshot.get("evidence_label")).strip().lower() or "unknown"

    target_msrp = pricing.get("target_msrp")
    target_vendor_cost = pricing.get("target_vendor_cost")
    margin_conflict = bool(suggested.get("margin_conflict"))
    minimum_margin_safe_price = suggested.get("minimum_margin_safe_price")
    recommended_ceiling = suggested.get("recommended_ceiling")
    notable_gaps = [normalize_text(item) for item in as_list(spec_coverage.get("notable_gaps")) if normalize_text(item)]
    gap_count = len(notable_gaps)
    feature_coverage = [as_dict(item) for item in as_list(spec_coverage.get("feature_coverage"))]
    certification_coverage = [as_dict(item) for item in as_list(spec_coverage.get("certification_coverage"))]
    zero_match_features = [entry for entry in feature_coverage if entry.get("matched_count") == 0]
    zero_match_certs = [entry for entry in certification_coverage if entry.get("matched_count") == 0]
    whitespace_labels = [
        normalize_text(entry.get("label"))
        for entry in zero_match_features
        if normalize_text(entry.get("signal")).strip().lower() == "whitespace"
    ]
    weak_signal_labels = [
        normalize_text(entry.get("label"))
        for entry in feature_coverage
        if entry.get("matched_count", 0) > 0 and normalize_text(entry.get("evidence_strength")).strip().lower() == "weak"
    ]
    zero_match_cert_labels = [normalize_text(entry.get("label")) for entry in zero_match_certs]

    actions: list[dict[str, str]] = []

    if target_msrp is None or target_vendor_cost is None:
        missing_bits = []
        if target_msrp is None:
            missing_bits.append("target MSRP")
        if target_vendor_cost is None:
            missing_bits.append("landed vendor cost")
        actions.append(
            {
                "category": "commercial",
                "issue": "Pricing inputs are incomplete",
                "why": f"Without {issue_sentence(missing_bits)}, the model has to infer key commercial inputs instead of scoring them directly.",
                "action": "Enter a provisional MSRP and landed vendor cost for this row, then rerun the report.",
                "impact": "Price Fit; Margin Viability; Outlook",
            }
        )

    if margin_conflict:
        why = "Current target pricing conflicts with the minimum margin-safe price."
        if minimum_margin_safe_price and recommended_ceiling:
            why = (
                f"The current minimum margin-safe price ({normalize_text(minimum_margin_safe_price)}) "
                f"sits above the recommended market ceiling ({normalize_text(recommended_ceiling)})."
            )
        actions.append(
            {
                "category": "commercial",
                "issue": "Margin conflict is still unresolved",
                "why": why,
                "action": "Lower vendor cost, raise MSRP, or trim feature scope until the target clears the safety floor.",
                "impact": "Margin Viability; Price Fit; Outlook",
            }
        )
    elif positioning == "premium":
        actions.append(
            {
                "category": "commercial",
                "issue": "Target pricing is above the market band",
                "why": "The current MSRP is positioned above the observed comparable range and needs stronger feature justification.",
                "action": "Either lower MSRP or strengthen the differentiator and certification story enough to defend a premium position.",
                "impact": "Price Fit; Outlook",
            }
        )

    if posture == "no_stackline_context":
        actions.append(
            {
                "category": "evidence",
                "issue": "Market growth context is missing",
                "why": "This row is missing usable Stackline market-growth context, which caps how strongly the outlook can score.",
                "action": "Confirm the row is mapped to the correct Stackline segment and refresh the run before using Outlook as a go / no-go signal.",
                "impact": "Market Support; Outlook",
            }
        )

    if gap_count >= 3:
        highlighted_labels = [label for label in whitespace_labels + zero_match_cert_labels if label][:3]
        gap_preview = issue_sentence(highlighted_labels[:2]) or issue_sentence(notable_gaps[:2])
        actions.append(
            {
                "category": "evidence",
                "issue": "Competitor evidence does not yet validate several proposed attributes",
                "why": (
                    f"The current caution is being driven by evidence blind spots, not proof the concept is wrong. "
                    f"Right now competitor records do not clearly confirm attributes such as {gap_preview}."
                ),
                "action": (
                    "Treat these as validation items, not automatic negatives: keep intentional innovations, "
                    "confirm whether each item is a real customer requirement or compliance need, and only cut it if it adds cost without demand support."
                ),
                "impact": "Confidence; Market Support; Outlook",
            }
        )
    elif gap_count > 0 and outlook != "favorable":
        highlighted_labels = [label for label in whitespace_labels + zero_match_cert_labels if label][:3]
        gap_preview = issue_sentence(highlighted_labels[:2]) or issue_sentence(notable_gaps[:2])
        actions.append(
            {
                "category": "evidence",
                "issue": "A few proposed attributes still need market validation",
                "why": (
                    f"The concept includes attributes that are not yet well confirmed in competitor evidence, including {gap_preview}."
                ),
                "action": (
                    "Decide whether these are intentional innovations, documentation-only blind spots, or true must-have requirements before treating them as blockers."
                ),
                "impact": "Confidence; Market Support",
            }
        )
    elif weak_signal_labels and outlook != "favorable":
        weak_preview = issue_sentence(weak_signal_labels[:2])
        actions.append(
            {
                "category": "evidence",
                "issue": "Some differentiator claims still rest on thin evidence",
                "why": f"The market set only weakly supports items such as {weak_preview}, so the report should treat them as directional rather than proven gaps or advantages.",
                "action": "Use these as hypotheses to validate, not reasons to downscore the concept unless stronger competitor evidence later contradicts them.",
                "impact": "Confidence; Outlook",
            }
        )

    if evidence_label in {"low", "none"}:
        actions.append(
            {
                "category": "evidence",
                "issue": "Amazon evidence is still thin",
                "why": "The current Amazon Gate 2 score is supported by limited direct evidence, so the score is less trustworthy than the research confidence alone suggests.",
                "action": "Treat the Amazon G2 score as directional until more direct listing / competitor evidence is collected.",
                "impact": "Amazon Evidence; Confidence",
            }
        )

    if not actions:
        actions.append(
            {
                "category": "execution",
                "issue": "Protect the current favorable read",
                "why": "Current market, pricing, and competitor signals are supportive enough that the concept now reads cleanly.",
                "action": "Lock the vendor quote, confirm the key certifications, and preserve the current feature set unless cost changes materially.",
                "impact": "Outlook; Margin Viability; Execution Readiness",
            }
        )

    actions = actions[:3]
    action_categories = [entry.get("category", "") for entry in actions]
    evidence_heavy = bool(actions) and all(category in {"evidence", "market_context"} for category in action_categories)

    subcategory_label = normalize_text(identity.get("subcategory")).strip().lower()

    if outlook == "favorable":
        overall_read = (
            f"This {'concept in ' + subcategory_label if subcategory_label else 'concept'} currently looks favorable to pursue "
            f"with {confidence} research confidence."
        )
        remaining_watchouts = [entry["issue"].lower() for entry in actions if entry.get("category") != "execution"]
        if remaining_watchouts:
            why_not_stronger = (
                "The concept is already favorable, but the remaining watchouts are "
                f"{issue_sentence(remaining_watchouts)}."
            )
        else:
            why_not_stronger = "The concept is already favorable; the main task now is to protect that position as quotes and compliance details firm up."
    elif outlook == "mixed":
        if evidence_heavy:
            overall_read = (
                "This concept looks viable, and the main constraint right now is incomplete market confirmation rather than a clearly weak product position."
            )
            why_not_stronger = (
                "It is not stronger yet because the report still needs clearer evidence around "
                f"{issue_sentence([entry['issue'].lower() for entry in actions])}."
            )
        else:
            overall_read = (
                "This concept looks viable, but it still needs targeted commercial or evidence cleanup before it becomes a strong go-forward candidate."
            )
            why_not_stronger = (
                "It is not stronger yet because "
                f"{issue_sentence([entry['issue'].lower() for entry in actions])}."
            )
    elif outlook == "cautious":
        if evidence_heavy:
            overall_read = (
                "This concept has enough support to analyze, and the current caution is being driven more by evidence ambiguity than by proof the product is weak."
            )
            why_not_stronger = (
                "The main watchouts right now are "
                f"{issue_sentence([entry['issue'].lower() for entry in actions])}."
            )
        else:
            overall_read = (
                "This concept has enough support to analyze, but the current configuration is still being held back by one or more commercial or evidence blockers."
            )
            why_not_stronger = (
                "The main blockers right now are "
                f"{issue_sentence([entry['issue'].lower() for entry in actions])}."
            )
    else:
        overall_read = "This concept still needs more complete evidence before the report can support a strong directional call."
        why_not_stronger = (
            "The current read is still limited because "
            f"{issue_sentence([entry['issue'].lower() for entry in actions])}."
        )

    table_rows = [
        [f"P{index}", entry["issue"], entry["why"], entry["action"], entry["impact"]]
        for index, entry in enumerate(actions, start=1)
    ]

    return {
        "overall_read": overall_read,
        "why_not_stronger": why_not_stronger,
        "actions": table_rows,
    }


def merged_text_row(ws, row: int, label: str, value: Any) -> int:
    """Write one labeled merged text row."""
    ws.cell(row=row, column=1, value=label)
    ws.cell(row=row, column=1).font = SUBHEADER_FONT
    ws.cell(row=row, column=1).fill = SUBHEADER_FILL
    ws.cell(row=row, column=2, value=normalize_text(value))
    ws.cell(row=row, column=2).alignment = WRAP_ALIGNMENT
    ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=10)
    return row + 1


def write_list_section(ws, row: int, title: str, values: list[str]) -> int:
    """Write a simple bulleted list section."""
    row = section_header(ws, row, title)
    if not values:
        ws.cell(row=row, column=1, value="No items.")
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=8)
        return row + 2
    for value in values:
        ws.cell(row=row, column=1, value=f"- {value}")
        ws.cell(row=row, column=1).alignment = WRAP_ALIGNMENT
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=8)
        row += 1
    return row + 1


def write_table(ws, row: int, title: str, headers: list[str], rows: list[list[Any]]) -> int:
    """Write a basic table and return the next row index."""
    row = section_header(ws, row, title, end_column=max(10, len(headers)))
    for col_index, header in enumerate(headers, start=1):
        cell = ws.cell(row=row, column=col_index, value=header)
        cell.font = SUBHEADER_FONT
        cell.fill = SUBHEADER_FILL
        cell.alignment = WRAP_ALIGNMENT
    row += 1

    if not rows:
        ws.cell(row=row, column=1, value="No rows.")
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=len(headers))
        return row + 2

    for row_offset, values in enumerate(rows):
        for col_index, value in enumerate(values, start=1):
            cell = ws.cell(row=row, column=col_index)
            apply_table_cell(cell, value)
            cell.alignment = WRAP_ALIGNMENT
            if row_offset % 2 == 1:
                cell.fill = ALTERNATE_ROW_FILL
        row += 1
    return row + 1


def apply_table_cell(cell, value: Any) -> None:
    """Write a table cell, optionally attaching a hyperlink."""
    hyperlink = None
    text_value = value
    if isinstance(value, dict):
        text_value = value.get("value")
        hyperlink = value.get("hyperlink")

    cell.value = normalize_text(text_value)
    if hyperlink:
        cell.hyperlink = hyperlink
        cell.font = HYPERLINK_FONT


def optional_text(value: Any) -> str | None:
    """Return a stripped string or None."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def looks_like_amazon_asin(value: str | None) -> bool:
    """Return whether a value looks like an Amazon ASIN."""
    return bool(value and re.fullmatch(r"[A-Z0-9]{10}", value.upper()))


def source_channel_label(item: dict[str, Any]) -> str:
    """Render a human-friendly source channel label."""
    channel = resolved_source_channel(item)
    return CHANNEL_DISPLAY_NAMES.get(channel, channel.replace("_", " ").title() or "Unknown")


def discovery_source_label(item: dict[str, Any]) -> str:
    """Render the original discovery source for inferred competitors."""
    domain = optional_text(item.get("discovery_source_domain"))
    if domain:
        host = urlparse(domain if "://" in domain else f"//{domain}").netloc or domain
        return host.replace("www.", "")
    channel = optional_text(item.get("discovery_source_channel"))
    if channel:
        lowered = channel.lower()
        return CHANNEL_DISPLAY_NAMES.get(lowered, lowered.replace("_", " ").title())
    domain = optional_text(item.get("source_domain"))
    if domain:
        host = urlparse(domain if "://" in domain else f"//{domain}").netloc or domain
        return host.replace("www.", "")
    return source_channel_label(item)


def listing_identifier_from_url(url: str | None, source_channel: str | None) -> str | None:
    """Best-effort identifier fallback derived from the listing URL."""
    if not url:
        return None

    channel = (source_channel or "").lower()
    patterns = []
    if channel == "amazon":
        patterns = [(r"/dp/([A-Z0-9]{10})(?:[/?]|$)", "ASIN {match}")]
    elif channel == "home_depot":
        patterns = [(r"/p/(?:[^/]+/)?(\d+)(?:[/?]|$)", "Item {match}")]
    elif channel == "walmart":
        patterns = [(r"/ip/(?:[^/]+/)?(\d+)(?:[/?]|$)", "Item {match}")]
    elif channel == "lowes":
        patterns = [(r"/pd/[^/]+/(\d+)(?:[/?]|$)", "Item {match}")]

    for pattern, template in patterns:
        match = re.search(pattern, url, flags=re.IGNORECASE)
        if match:
            return template.format(match=match.group(1))
    return None


def listing_identifier(item: dict[str, Any]) -> str:
    """Return the best channel-aware identifier label for one competitor listing."""
    source_channel = resolved_source_channel(item)
    sku = optional_text(item.get("sku"))
    model_number = optional_text(item.get("model_number"))
    url = optional_text(item.get("url"))

    if source_channel == "amazon":
        asin = sku if looks_like_amazon_asin(sku) else None
        if not asin and looks_like_amazon_asin(model_number):
            asin = model_number
        if not asin and url:
            derived = listing_identifier_from_url(url, source_channel)
            if derived and derived.startswith("ASIN "):
                asin = derived.replace("ASIN ", "", 1)
        if asin:
            return f"ASIN {asin}"
        if model_number:
            return f"Model {model_number}"

    if model_number:
        return f"Model {model_number}"

    if sku:
        if source_channel == "brand_site":
            return f"Part {sku}"
        if source_channel in {"home_depot", "walmart", "lowes"}:
            return f"Item {sku}"
        return f"SKU {sku}"

    derived = listing_identifier_from_url(url, source_channel)
    return derived or ""


def listing_link_cell(item: dict[str, Any]) -> dict[str, str] | str:
    """Return a hyperlink cell payload for a competitor source listing."""
    url = optional_text(item.get("url"))
    if not url or url.startswith("stackline://"):
        return ""
    return {
        "value": source_channel_label(item),
        "hyperlink": url,
    }


def verification_status(item: dict[str, Any]) -> str:
    """Return the normalized verification status for one competitor record."""
    status = (optional_text(item.get("verification_status")) or "").lower()
    if status in {"verified_listing", "inferred_competitor"}:
        return status
    return "verified_listing"


def verification_label(item: dict[str, Any]) -> str:
    """Render a user-facing verification label."""
    return "Verified Listing" if verification_status(item) == "verified_listing" else "Inferred Competitor"


def sort_candidates(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort candidate records by confidence, then by price."""
    def sort_key(item: dict[str, Any]) -> tuple[float, float, str]:
        confidence = item.get("match_confidence")
        if not isinstance(confidence, (int, float)):
            confidence = 0
        price = item.get("price")
        if not isinstance(price, (int, float)):
            price = 0
        title = normalize_text(item.get("product_title"))
        return (-confidence, price, title)

    return sorted(items, key=sort_key)


def candidate_rows(
    normalized_items: list[dict[str, Any]],
    channels: set[str],
    limit: int = 10,
    verification_filter: str | None = "verified_listing",
) -> list[list[Any]]:
    """Build a compact competitor table filtered by source channel."""
    rows = []
    for item in sort_candidates(normalized_items):
        if resolved_source_channel(item) not in channels:
            continue
        if verification_filter and verification_status(item) != verification_filter:
            continue
        rows.append(
            [
                item.get("brand"),
                item.get("product_title"),
                listing_identifier(item),
                source_channel_label(item),
                item.get("price"),
                item.get("wattage"),
                item.get("lumens"),
                item.get("cct"),
                item.get("cri"),
                listing_link_cell(item),
                item.get("match_confidence"),
            ]
        )
        if len(rows) >= limit:
            break
    return rows


def inferred_competitor_rows(
    normalized_items: list[dict[str, Any]],
    limit: int = 12,
) -> list[list[Any]]:
    """Build a table of inferred competitors that need listing verification."""
    rows = []
    for item in sort_candidates(normalized_items):
        if verification_status(item) != "inferred_competitor":
            continue
        rows.append(
            [
                item.get("brand"),
                item.get("product_title"),
                source_channel_label(item),
                discovery_source_label(item),
                listing_link_cell(item),
                item.get("verification_reason") or item.get("extraction_notes") or item.get("match_notes"),
                item.get("match_confidence"),
            ]
        )
        if len(rows) >= limit:
            break
    return rows


def top_brand_rows(brands: list[dict[str, Any]]) -> list[list[Any]]:
    """Format summarized brand rows."""
    rows = []
    for brand in brands:
        rows.append(
            [
                brand.get("brand"),
                brand.get("candidate_count"),
                normalize_text(brand.get("source_channels")),
                brand.get("median_unit_price"),
            ]
        )
    return rows


def coverage_rows(entries: list[dict[str, Any]]) -> list[list[Any]]:
    """Format feature/certification coverage rows."""
    rows = []
    for entry in entries:
        rows.append([entry.get("label"), entry.get("matched_count"), entry.get("coverage_pct")])
    return rows


def benchmark_rows(pricing: dict[str, Any]) -> list[list[Any]]:
    """Format multi-metric pricing benchmark rows."""
    metrics = [
        ("Raw Price", as_dict(pricing.get("price_benchmarks"))),
        ("Unit Price", as_dict(pricing.get("unit_price_benchmarks"))),
        ("Unit Price / Watt", as_dict(pricing.get("unit_price_per_watt_benchmarks"))),
        ("Unit Price / Lumen", as_dict(pricing.get("unit_price_per_lumen_benchmarks"))),
    ]
    rows = []
    for label, metric in metrics:
        rows.append(
            [
                label,
                metric.get("sample_size"),
                metric.get("min"),
                metric.get("p25"),
                metric.get("median"),
                metric.get("mean"),
                metric.get("p75"),
                metric.get("max"),
            ]
        )
    return rows


def pricing_position_rows(pricing: dict[str, Any]) -> list[list[Any]]:
    """Format pricing positioning rows."""
    suggested = as_dict(pricing.get("suggested_msrp_range"))
    target_position = as_dict(pricing.get("target_price_position"))
    return [
        ["Target MSRP", pricing.get("target_msrp")],
        ["Evaluated Price", target_position.get("evaluated_value")],
        ["Evaluated Price Source", target_position.get("evaluated_value_source")],
        ["Target Price Percentile", target_position.get("percentile")],
        ["Target Price Bucket", target_position.get("bucket")],
        ["Target vs Market Median %", target_position.get("vs_median_pct")],
        ["Observed Unit Price Floor (P25)", suggested.get("observed_unit_price_floor")],
        ["Observed Unit Price Ceiling (P75)", suggested.get("observed_unit_price_ceiling")],
        ["Recommended Floor", suggested.get("recommended_floor")],
        ["Recommended Ceiling", suggested.get("recommended_ceiling")],
        ["Minimum Margin-Safe MSRP", suggested.get("minimum_margin_safe_price")],
        ["Suggested Positioning", suggested.get("positioning")],
        ["Margin Conflict", suggested.get("margin_conflict")],
    ]


def margin_rows(pricing: dict[str, Any]) -> list[list[Any]]:
    """Format channel-specific margin guidance rows."""
    rows = []
    for channel in ["shopify", "amazon"]:
        entry = as_dict(as_dict(pricing.get("margin_targets")).get(channel))
        if not entry:
            continue
        rows.append(
            [
                channel.title(),
                entry.get("target_margin_pct"),
                entry.get("minimum_viable_msrp"),
                entry.get("vs_target_msrp_pct"),
                entry.get("vs_market_median_pct"),
            ]
        )
    return rows


def value_position_rows(pricing: dict[str, Any]) -> list[list[Any]]:
    """Format value-ranking rows for unit price, price per watt, and price per lumen."""
    rows = []
    metrics = [
        ("Unit Price", as_dict(pricing.get("target_price_position"))),
        ("Unit Price / Watt", as_dict(pricing.get("target_price_per_watt_position"))),
        ("Unit Price / Lumen", as_dict(pricing.get("target_price_per_lumen_position"))),
    ]
    for label, metric in metrics:
        if not metric:
            continue
        rows.append(
            [
                label,
                metric.get("evaluated_value"),
                metric.get("percentile"),
                metric.get("bucket"),
                metric.get("vs_median_pct"),
            ]
        )
    return rows


def channel_comparison_rows(performance: dict[str, Any]) -> list[list[Any]]:
    """Format Stackline channel comparison rows for Section A."""
    comparison = as_dict(performance.get("channel_comparison"))
    channels = as_dict(comparison.get("channels"))
    rows = []
    for channel_name, channel in channels.items():
        channel = as_dict(channel)
        rows.append(
            [
                channel_name,
                channel.get("retail_sales"),
                channel.get("units_sold"),
                channel.get("avg_retail_price"),
                channel.get("retail_sales_growth_pct"),
                channel.get("sunco_sales_share_pct"),
            ]
        )
    return rows


def spec_action_rows(spec_coverage: dict[str, Any]) -> list[list[Any]]:
    """Format actionable feature/certification coverage rows."""
    rows = []
    for entry in as_list(spec_coverage.get("feature_coverage")):
        rows.append(
            [
                "Feature",
                entry.get("label"),
                entry.get("signal"),
                entry.get("evidence_strength"),
                entry.get("coverage_pct"),
                entry.get("matched_count"),
                entry.get("recommended_action"),
            ]
        )
    for entry in as_list(spec_coverage.get("certification_coverage")):
        rows.append(
            [
                "Certification",
                entry.get("label"),
                entry.get("signal"),
                entry.get("evidence_strength"),
                entry.get("coverage_pct"),
                entry.get("matched_count"),
                entry.get("recommended_action"),
            ]
        )
    return rows


def numeric_guidance_rows(spec_coverage: dict[str, Any]) -> list[list[Any]]:
    """Format numeric target-positioning rows."""
    rows = []
    for entry in as_list(spec_coverage.get("numeric_guidance")):
        rows.append(
            [
                entry.get("label"),
                entry.get("target_value"),
                entry.get("median"),
                entry.get("p75"),
                entry.get("target_percentile"),
                entry.get("recommended_action"),
            ]
        )
    return rows


def gate_readiness_snapshot_rows(gate_readiness: dict[str, Any]) -> list[list[Any]]:
    """Format gate/channel readiness rows for the report."""
    rows = []
    for snapshot in as_list(gate_readiness.get("snapshots")):
        evidence = as_dict(snapshot.get("evidence_confidence"))
        rows.append(
            [
                snapshot.get("channel"),
                snapshot.get("gate"),
                snapshot.get("family_state"),
                snapshot.get("weighted_score"),
                evidence.get("score"),
                evidence.get("label"),
                f"{evidence.get('implemented_questions')}/{evidence.get('methodology_active_questions')}",
            ]
        )
    return rows


def gate_readiness_pillar_rows(gate_readiness: dict[str, Any]) -> list[list[Any]]:
    """Format pillar-level rollups from the primary G2 channel snapshot."""
    primary_channel = normalize_text(gate_readiness.get("primary_channel"))
    for snapshot in as_list(gate_readiness.get("snapshots")):
        if snapshot.get("channel") == primary_channel and snapshot.get("gate") == "G2":
            rows = []
            for pillar in as_list(snapshot.get("pillar_scores")):
                rows.append(
                    [
                        pillar.get("label"),
                        pillar.get("base_weight"),
                        pillar.get("effective_weight"),
                        pillar.get("average_score"),
                        f"{pillar.get('scored_question_count')}/{pillar.get('question_count')}",
                        pillar.get("status"),
                    ]
                )
            return rows
    return []


def vendor_request_rows(items: list[dict[str, Any]]) -> list[list[Any]]:
    """Format vendor optimization requests."""
    rows = []
    for item in items:
        rows.append(
            [
                item.get("priority"),
                item.get("linked_metric"),
                item.get("request"),
                item.get("reason"),
            ]
        )
    return rows


def optimization_driver_rows(items: list[dict[str, Any]]) -> list[list[Any]]:
    """Format category-aware decision drivers for the report."""
    rows = []
    for item in items:
        rows.append(
            [
                item.get("tier"),
                item.get("label"),
                item.get("driver_type"),
                item.get("signal"),
                item.get("reason"),
            ]
        )
    return rows


def low_signal_rows(items: list[dict[str, Any]]) -> list[list[Any]]:
    """Format lower-signal attributes that should be validated before over-weighting."""
    rows = []
    for item in items:
        rows.append(
            [
                item.get("label"),
                item.get("driver_type"),
                item.get("signal"),
                item.get("reason"),
            ]
        )
    return rows


def optimization_modifier_rows(items: list[dict[str, Any]]) -> list[list[Any]]:
    """Format active optimization modifiers for the report."""
    rows = []
    for item in items:
        rows.append(
            [
                item.get("label"),
                ", ".join(as_list(item.get("matched_keywords"))),
                item.get("match_score"),
            ]
        )
    return rows


def optimization_scorecard_rows(scorecard: dict[str, Any]) -> list[list[Any]]:
    """Format optimization score components for the report."""
    rows = []
    for item in as_list(scorecard.get("components")):
        rows.append(
            [
                item.get("component"),
                item.get("score"),
                item.get("weight"),
                item.get("reason"),
            ]
        )
    return rows


def report_filename(row_number: int) -> str:
    """Return the stable report filename for a row."""
    return f"row_{row_number:03d}_research_report.xlsx"


def prd_prefill_pairs(packet: dict[str, Any], analysis: dict[str, Any]) -> list[tuple[str, Any]]:
    """Build the PRD-oriented prefill block from packet targets."""
    identity = as_dict(packet.get("identity"))
    target_profile = as_dict(packet.get("target_profile"))
    electrical = as_dict(target_profile.get("electrical"))
    physical = as_dict(target_profile.get("physical"))
    business = as_dict(target_profile.get("business_case"))
    reference = as_dict(packet.get("reference_baseline"))

    return [
        ("Ideation Name", identity.get("ideation_name")),
        ("Reference Image URL", reference.get("image_url")),
        ("Voltage", electrical.get("voltage")),
        ("Wattage Primary", electrical.get("wattage_primary")),
        ("Wattage Max", electrical.get("wattage_max")),
        ("Selectable Wattage", electrical.get("selectable_wattage")),
        ("CCT Primary", electrical.get("cct_primary")),
        ("CCT Max", electrical.get("cct_max")),
        ("Selectable CCT", electrical.get("selectable_cct")),
        ("CRI", electrical.get("cri")),
        ("Lumens Target", electrical.get("lumens_target")),
        ("Dimmable", electrical.get("dimmable")),
        ("Dimming Type", electrical.get("dimming_type")),
        ("Size / Form Factor", physical.get("size_form_factor")),
        ("Mounting Type", physical.get("mounting_type")),
        ("Material", physical.get("material")),
        ("Finish / Color", physical.get("finish_color")),
        ("IP Rating", physical.get("ip_rating")),
        ("Moisture Rating", physical.get("moisture_rating")),
        ("Target MSRP", as_dict(analysis.get("pricing_analysis")).get("target_msrp")),
        ("Target Vendor Cost", as_dict(analysis.get("pricing_analysis")).get("target_vendor_cost")),
        ("Certifications", business.get("certifications")),
        ("Lifetime Hours", business.get("lifetime_hours")),
        ("Warranty", business.get("warranty")),
    ]


def render_row_sheet(
    ws,
    row_number: int,
    packet: dict[str, Any],
    analysis: dict[str, Any],
    normalized: dict[str, Any],
) -> None:
    """Render one ideation sheet into an existing worksheet."""
    set_default_layout(ws)

    identity = as_dict(packet.get("identity"))
    reference = as_dict(packet.get("reference_baseline"))
    performance = as_dict(analysis.get("performance_estimation"))
    pricing = as_dict(analysis.get("pricing_analysis"))
    summary = as_dict(analysis.get("summary"))
    spec_coverage = as_dict(analysis.get("spec_coverage"))
    reference_anchor = as_dict(analysis.get("reference_anchor_context"))
    gate_readiness = as_dict(analysis.get("gate_readiness"))
    vendor_requests = as_list(analysis.get("highest_impact_vendor_requests"))
    ideation_optimization = as_dict(analysis.get("ideation_optimization"))
    optimization_scorecard = as_dict(ideation_optimization.get("optimization_scorecard"))
    optimization_modifiers = as_list(ideation_optimization.get("active_modifiers"))
    normalized_summary = as_dict(normalized.get("summary"))
    action_summary = blocker_action_summary(packet, analysis)

    ws.cell(row=1, column=1, value=identity.get("ideation_name"))
    ws.cell(row=1, column=1).font = TITLE_FONT
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=10)
    ws.cell(row=2, column=1, value=f"Row {row_number} Research Report")
    ws.cell(row=2, column=1).fill = ACCENT_FILL
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=10)

    row = 4
    row = section_header(ws, row, "What To Do Next")
    row = merged_text_row(ws, row, "Overall Read", action_summary.get("overall_read"))
    row = merged_text_row(ws, row, "Why Not Stronger", action_summary.get("why_not_stronger"))
    row = write_table(
        ws,
        row,
        "Priority Actions",
        ["Priority", "Issue", "Why It Matters", "Recommended Action", "Expected Impact"],
        as_list(action_summary.get("actions")),
    )
    row = section_header(ws, row, "Section A - Ideation + Reference Anchor Context")
    row = key_value_rows(
        ws,
        row,
        [
            ("Category Owner", identity.get("category_owner")),
            ("Category", identity.get("category")),
            ("Subcategory", identity.get("subcategory")),
            ("Strategy", identity.get("strategy")),
            ("Reference Anchor SKU", identity.get("sunco_reference_sku")),
            ("Launch Outlook", performance.get("launch_outlook")),
            ("Confidence", performance.get("confidence")),
            ("Anchor Data Quality", reference_anchor.get("data_quality")),
            ("Anchor Title", reference.get("title")),
            ("Anchor Title Source", reference.get("title_source")),
            ("Anchor Listing Price", reference.get("listing_price")),
            ("Listing Price Source", reference.get("listing_price_source")),
            ("Listing Price Note", reference.get("listing_price_note")),
            ("Anchor Shopify Revenue 12mo", reference.get("shopify_revenue_12mo")),
            ("Anchor Shopify Units 12mo", reference.get("shopify_units_12mo")),
            ("Shopify Data Source", reference.get("shopify_data_source")),
            ("Anchor Amazon Revenue 12mo", reference.get("amazon_revenue_12mo")),
            ("Anchor Amazon Units 12mo", reference.get("amazon_units_12mo")),
            ("Amazon Data Source", reference.get("amazon_data_source")),
            ("Anchor Data Source", reference.get("reference_data_source")),
            ("Anchor Sales Period", reference.get("sales_period_label")),
        ],
    )
    row = merged_text_row(ws, row, "Reference Anchor Role", reference_anchor.get("primary_use"))
    row = merged_text_row(ws, row, "Reference Anchor Secondary Use", reference_anchor.get("secondary_use"))
    row = merged_text_row(ws, row, "Reference Anchor Caution", reference_anchor.get("caution"))
    row = merged_text_row(ws, row, "Reference Anchor Guardrail", reference_anchor.get("do_not_overweight"))
    row = merged_text_row(ws, row, "Reference Anchor Image URL", reference.get("image_url"))
    row = merged_text_row(
        ws,
        row,
        "Performance Rationale",
        " | ".join(as_list(performance.get("rationale"))),
    )
    row = key_value_rows(
        ws,
        row,
        [
            ("Verified Listings", normalized_summary.get("verified_listing_count")),
            ("Inferred Competitors", normalized_summary.get("inferred_competitor_count")),
        ],
        columns=2,
    )
    row = write_table(
        ws,
        row,
        "Stackline Channel Comparison",
        ["Channel", "Retail Sales", "Units Sold", "Avg Price", "Sales Growth %", "Sunco Share %"],
        channel_comparison_rows(performance),
    )
    row = merged_text_row(ws, row, "Gate Readiness Summary", gate_readiness.get("summary"))
    row = write_table(
        ws,
        row,
        "Gate Readiness by Channel / Gate",
        ["Channel", "Gate", "Family State", "Score", "Evidence Score", "Evidence Label", "Implemented Questions"],
        gate_readiness_snapshot_rows(gate_readiness),
    )
    row = write_table(
        ws,
        row,
        "Primary G2 Pillar Rollup",
        ["Pillar", "Base Weight", "Effective Weight", "Avg Score", "Scored Questions", "Status"],
        gate_readiness_pillar_rows(gate_readiness),
    )

    row = write_table(
        ws,
        row,
        "Section B - Amazon Competitors (Verified Listings)",
        ["Brand", "Product", "Identifier", "Channel", "Price", "Wattage", "Lumens", "CCT", "CRI", "Source Link", "Confidence"],
        candidate_rows(as_list(normalized.get("items")), AMAZON_CHANNELS, limit=10, verification_filter="verified_listing"),
    )

    row = write_table(
        ws,
        row,
        "Section C - Brick-and-Mortar / Direct Competitors (Verified Listings)",
        ["Brand", "Product", "Identifier", "Channel", "Price", "Wattage", "Lumens", "CCT", "CRI", "Source Link", "Confidence"],
        candidate_rows(as_list(normalized.get("items")), BM_DIRECT_CHANNELS, limit=12, verification_filter="verified_listing"),
    )

    row = write_table(
        ws,
        row,
        "Section D - Inferred Competitors / Needs Verification",
        ["Brand", "Product", "Likely Channel", "Supporting Source", "Source Link", "Why Inferred", "Confidence"],
        inferred_competitor_rows(as_list(normalized.get("items")), limit=12),
    )

    row = write_table(
        ws,
        row,
        "Section E - Pricing Position",
        ["Metric", "Value"],
        pricing_position_rows(pricing),
    )
    row = write_table(
        ws,
        row,
        "Pricing Benchmarks",
        ["Metric", "Samples", "Min", "P25", "Median", "Mean", "P75", "Max"],
        benchmark_rows(pricing),
    )
    row = write_table(
        ws,
        row,
        "Margin Targets",
        ["Channel", "Target Margin %", "Min MSRP", "Vs Target MSRP %", "Vs Market Median %"],
        margin_rows(pricing),
    )
    row = write_table(
        ws,
        row,
        "Value Ranking",
        ["Metric", "Target", "Percentile", "Bucket", "Vs Median %"],
        value_position_rows(pricing),
    )
    row = write_table(
        ws,
        row,
        "Top Brands",
        ["Brand", "Candidate Count", "Channels", "Median Unit Price"],
        top_brand_rows(as_list(summary.get("top_brands"))),
    )

    row = write_table(
        ws,
        row,
        "Section F - Feature / Certification Signals",
        ["Type", "Label", "Signal", "Evidence", "Coverage %", "Matched", "Recommendation"],
        spec_action_rows(spec_coverage),
    )
    row = write_table(
        ws,
        row,
        "Numeric Target Positioning",
        ["Metric", "Target", "Median", "P75", "Percentile", "Recommendation"],
        numeric_guidance_rows(spec_coverage),
    )
    row = merged_text_row(ws, row, "Category Optimization Summary", ideation_optimization.get("summary"))
    row = key_value_rows(
        ws,
        row,
        [
            ("Optimization Profile", ideation_optimization.get("profile_label")),
            ("Profile Match Basis", ", ".join(as_list(ideation_optimization.get("matched_taxonomy"))) or ", ".join(as_list(ideation_optimization.get("matched_keywords")))),
            ("Active Modifiers", ", ".join(item.get("label") for item in optimization_modifiers if item.get("label"))),
            ("Optimization Score", optimization_scorecard.get("score")),
            ("Optimization Label", optimization_scorecard.get("label")),
            ("Optimization Confidence", optimization_scorecard.get("confidence")),
        ],
    )
    row = write_table(
        ws,
        row,
        "Active Variant Modifiers",
        ["Modifier", "Matched Keywords", "Match Score"],
        optimization_modifier_rows(optimization_modifiers),
    )
    row = write_table(
        ws,
        row,
        "Optimization Scorecard",
        ["Component", "Score", "Weight", "Reason"],
        optimization_scorecard_rows(optimization_scorecard),
    )
    row = write_table(
        ws,
        row,
        "Primary Decision Drivers",
        ["Tier", "Driver", "Type", "Signal", "Why It Matters"],
        optimization_driver_rows(as_list(ideation_optimization.get("primary_decision_drivers"))),
    )
    row = write_table(
        ws,
        row,
        "Secondary Decision Drivers",
        ["Tier", "Driver", "Type", "Signal", "Why It Matters"],
        optimization_driver_rows(as_list(ideation_optimization.get("secondary_decision_drivers"))),
    )
    row = write_table(
        ws,
        row,
        "Validate Before Over-Weighting",
        ["Driver", "Type", "Signal", "Reason"],
        low_signal_rows(as_list(ideation_optimization.get("low_signal_attributes"))),
    )
    row = write_table(
        ws,
        row,
        "Highest-Impact Vendor Requests",
        ["Priority", "Area", "Request", "Reason"],
        vendor_request_rows(vendor_requests),
    )
    row = write_list_section(ws, row, "Recommendations", as_list(analysis.get("recommendations")))
    row = write_list_section(ws, row, "Notes", as_list(analysis.get("notes")))

    row = section_header(ws, row, "Section G - PRD Generator Pre-Fill")
    row = key_value_rows(ws, row, prd_prefill_pairs(packet, analysis))


def completed_row_payloads(
    session_dir: Path,
    rows: list[int] | None = None,
) -> list[tuple[int, dict[str, Any], dict[str, Any], dict[str, Any]]]:
    """Load completed packet/analysis/normalized payloads for selected rows."""
    manifest = read_json(session_dir / "manifest.json")
    target_rows = set(rows or [row["row_number"] for row in manifest.get("rows", [])])
    payloads = []

    for row in manifest.get("rows", []):
        row_number = row["row_number"]
        if row_number not in target_rows:
            continue
        if row.get("stages", {}).get("analyzed") != "complete":
            continue
        packet = read_json(packet_path_for(session_dir, row_number))
        analysis = read_json(artifact_path_for(session_dir, row_number, "analyzed"))
        normalized = read_json(artifact_path_for(session_dir, row_number, "normalized"))
        payloads.append((row_number, packet, analysis, normalized))

    return payloads


def build_report(session_dir: Path, row_number: int) -> Path | None:
    """Build one Excel report for a completed analyzed row."""
    payloads = completed_row_payloads(session_dir, rows=[row_number])
    if not payloads:
        return None

    _, packet, analysis, normalized = payloads[0]
    wb = Workbook()
    ws = wb.active
    ws.title = safe_sheet_title(normalize_text(as_dict(packet.get("identity")).get("ideation_name")), set())
    render_row_sheet(ws, row_number, packet, analysis, normalized)

    report_path = artifact_path_for(session_dir, row_number, "reported")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(report_path)
    return report_path


def build_summary_sheet(ws, payloads: list[tuple[int, dict[str, Any], dict[str, Any], dict[str, Any]]]) -> None:
    """Build a summary sheet for the combined workbook."""
    set_default_layout(ws)
    ws.title = "Summary"
    ws.cell(row=1, column=1, value="Completed Research Rows")
    ws.cell(row=1, column=1).font = TITLE_FONT
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=9)
    row = key_value_rows(
        ws,
        3,
        [
            ("Completed Row Count", len(payloads)),
            ("Workbook Scope", "Completed analyzed rows only"),
        ],
        columns=1,
    )
    row = write_table(
        ws,
        row + 1,
        "Summary Metric Guide",
        ["Metric", "What It Means", "Question It Answers"],
        SUMMARY_METRIC_GUIDE_ROWS,
    )
    write_table(
        ws,
        row,
        "Completed Rows",
        ["Row", "Ideation", "Category Owner", "Category", "Outlook", "Confidence", "Amazon G2", "Amazon Evidence", "Report File"],
        [
            [
                row_number,
                as_dict(packet.get("identity")).get("ideation_name"),
                as_dict(packet.get("identity")).get("category_owner"),
                as_dict(packet.get("identity")).get("category"),
                as_dict(analysis.get("performance_estimation")).get("launch_outlook"),
                as_dict(analysis.get("performance_estimation")).get("confidence"),
                next(
                    (
                        snapshot.get("weighted_score")
                        for snapshot in as_list(as_dict(analysis.get("gate_readiness")).get("snapshots"))
                        if snapshot.get("channel") == "amazon" and snapshot.get("gate") == "G2"
                    ),
                    None,
                ),
                next(
                    (
                        as_dict(snapshot.get("evidence_confidence")).get("label")
                        for snapshot in as_list(as_dict(analysis.get("gate_readiness")).get("snapshots"))
                        if snapshot.get("channel") == "amazon" and snapshot.get("gate") == "G2"
                    ),
                    None,
                ),
                report_filename(row_number),
            ]
            for row_number, packet, analysis, _ in payloads
        ],
    )


def build_combined_workbook(
    session_dir: Path,
    rows: list[int] | None = None,
    output_path: str | None = None,
) -> Path | None:
    """Build one workbook with a summary sheet plus one sheet per completed row."""
    payloads = completed_row_payloads(session_dir, rows=rows)
    if not payloads:
        return None

    wb = Workbook()
    summary_ws = wb.active
    build_summary_sheet(summary_ws, payloads)

    used_titles = {summary_ws.title}
    for row_number, packet, analysis, normalized in payloads:
        title = safe_sheet_title(normalize_text(as_dict(packet.get("identity")).get("ideation_name")), used_titles)
        ws = wb.create_sheet(title=title)
        render_row_sheet(ws, row_number, packet, analysis, normalized)

    if output_path:
        destination = Path(output_path).resolve()
    else:
        destination = session_dir / "reports" / f"{session_dir.name}_completed_rows.xlsx"
    destination.parent.mkdir(parents=True, exist_ok=True)
    wb.save(destination)
    return destination


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


def build_reports(
    session_root: str,
    rows: list[int] | None = None,
    combined: bool = False,
    output_path: str | None = None,
) -> dict[str, Any]:
    """Build report artifacts for selected session rows."""
    session_dir = Path(session_root).resolve()

    if combined:
        combined_path = build_combined_workbook(session_dir, rows=rows, output_path=output_path)
        update_result = update_session(str(session_dir))
        return {
            "session_root": str(session_dir),
            "rows_requested": sorted(rows or []),
            "combined_workbook": str(combined_path) if combined_path else None,
            "manifest_summary": update_result["summary"],
        }

    manifest = read_json(session_dir / "manifest.json")
    target_rows = set(rows or [row["row_number"] for row in manifest.get("rows", [])])

    written_rows = []
    skipped_rows = []
    report_files = []

    for row in manifest.get("rows", []):
        row_number = row["row_number"]
        if row_number not in target_rows:
            continue
        report_path = build_report(session_dir, row_number)
        if report_path is None:
            skipped_rows.append(row_number)
            continue
        written_rows.append(row_number)
        report_files.append(str(report_path))

    update_result = update_session(str(session_dir))
    return {
        "session_root": str(session_dir),
        "rows_requested": sorted(target_rows),
        "rows_written": written_rows,
        "rows_skipped": skipped_rows,
        "report_files": report_files,
        "manifest_summary": update_result["summary"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build Excel report artifacts for completed analyzed rows."
    )
    parser.add_argument("session_root", help="Path to an initialized research session.")
    parser.add_argument(
        "--rows",
        default=None,
        help="Optional comma-separated row numbers to report.",
    )
    parser.add_argument(
        "--combined",
        action="store_true",
        help="Build one combined workbook with one sheet per completed row.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional explicit output path for the combined workbook.",
    )
    args = parser.parse_args()

    result = build_reports(
        args.session_root,
        rows=parse_rows_argument(args.rows),
        combined=args.combined,
        output_path=args.output,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
