"""
Step 5A/5B/5C: Analyze normalized competitor artifacts into row-level guidance.

Usage:
  python tools/competitive_analysis.py "C:\\path\\to\\research_session"
  python tools/competitive_analysis.py "C:\\path\\to\\research_session" --rows 3,4
"""

from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from research_session_manager import (
    SCHEMA_VERSION,
    artifact_path_for,
    packet_path_for,
    read_json,
    update_session,
    utc_now,
    write_json,
)


RAW_STAGE_KEYS = [
    "amazon_collection",
    "brick_and_mortar_collection",
    "brand_site_collection",
]


def normalize_text(value: Any) -> str | None:
    """Normalize optional values into stripped strings."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "Yes" if value else "No"
    text = str(value).strip()
    return text or None


def compact_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Drop empty values from a dict."""
    return {
        key: value
        for key, value in data.items()
        if value not in (None, "", [], {})
    }


def as_dict(value: Any) -> dict[str, Any]:
    """Coerce optional mapping-like values into dicts."""
    if isinstance(value, dict):
        return value
    return {}


def as_list(value: Any) -> list[Any]:
    """Coerce optional sequence-like values into lists."""
    if isinstance(value, list):
        return value
    return []


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


def round_money(value: float | None) -> float | None:
    """Round monetary values to two decimals."""
    if value is None:
        return None
    return round(value + 1e-9, 2)


def parse_series_numbers(value: Any) -> list[float]:
    """Extract numeric series from strings like 30/40/50W or 80+."""
    text = normalize_text(value)
    if not text:
        return []
    matches = re.findall(r"\d+(?:\.\d+)?", text)
    return [float(match) for match in matches]


def percentile(values: list[float], ratio: float) -> float | None:
    """Return a simple interpolated percentile from a list of floats."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * ratio
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    lower_value = ordered[lower]
    upper_value = ordered[upper]
    return lower_value + (upper_value - lower_value) * (position - lower)


def summarize_numeric(values: list[float]) -> dict[str, Any]:
    """Build benchmark stats for a numeric series."""
    if not values:
        return {}
    ordered = sorted(values)
    return compact_dict(
        {
            "sample_size": len(ordered),
            "min": round_money(ordered[0]),
            "p25": round_money(percentile(ordered, 0.25)),
            "median": round_money(percentile(ordered, 0.5)),
            "mean": round_money(sum(ordered) / len(ordered)),
            "p75": round_money(percentile(ordered, 0.75)),
            "max": round_money(ordered[-1]),
        }
    )


def safe_pct_delta(current: float | None, baseline: float | None) -> float | None:
    """Return a percent delta if both values are present."""
    if current is None or baseline in (None, 0):
        return None
    return round(((current - baseline) / baseline) * 100, 2)


def unit_price(record: dict[str, Any]) -> float | None:
    """Calculate per-unit price using pack quantity when available."""
    price = parse_number(record.get("price"))
    if price is None:
        return None
    pack_quantity = parse_number(record.get("pack_quantity"))
    if pack_quantity and pack_quantity > 0:
        return price / pack_quantity
    return price


def representative_value(value: Any, reducer: str = "max") -> float | None:
    """Pick a representative value from a numeric series string."""
    numbers = parse_series_numbers(value)
    if not numbers:
        return None
    if reducer == "min":
        return min(numbers)
    if reducer == "median":
        return percentile(numbers, 0.5)
    return max(numbers)


def channel_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    """Count normalized candidates by source channel."""
    counts = Counter()
    for item in items:
        channel = normalize_text(item.get("source_channel"))
        if channel:
            counts[channel] += 1
    return dict(counts)


def top_brands(items: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    """Summarize the most common normalized brands."""
    by_brand: dict[str, dict[str, Any]] = {}
    unit_price_samples: dict[str, list[float]] = defaultdict(list)
    for item in items:
        brand = normalize_text(item.get("brand"))
        if not brand:
            continue
        entry = by_brand.setdefault(
            brand,
            {
                "brand": brand,
                "candidate_count": 0,
                "source_channels": [],
            },
        )
        entry["candidate_count"] += 1
        channel = normalize_text(item.get("source_channel"))
        if channel:
            entry["source_channels"].append(channel)
        current_unit_price = unit_price(item)
        if current_unit_price is not None:
            unit_price_samples[brand].append(current_unit_price)

    ranked = sorted(
        by_brand.values(),
        key=lambda entry: (-entry["candidate_count"], entry["brand"].lower()),
    )
    result = []
    for entry in ranked[:limit]:
        brand = entry["brand"]
        result.append(
            compact_dict(
                {
                    "brand": brand,
                    "candidate_count": entry["candidate_count"],
                    "source_channels": unique_preserve_order(entry["source_channels"]),
                    "median_unit_price": round_money(
                        percentile(unit_price_samples.get(brand, []), 0.5)
                    ),
                }
            )
        )
    return result


def build_pricing_analysis(packet: dict[str, Any], items: list[dict[str, Any]]) -> dict[str, Any]:
    """Build pricing benchmarks and a provisional MSRP recommendation."""
    price_samples = []
    unit_price_samples = []
    unit_price_per_watt = []
    unit_price_per_lumen = []
    multi_pack_count = 0

    for item in items:
        price = parse_number(item.get("price"))
        if price is not None:
            price_samples.append(price)

        current_unit_price = unit_price(item)
        if current_unit_price is not None:
            unit_price_samples.append(current_unit_price)

            wattage = representative_value(item.get("wattage"), reducer="max")
            lumens = representative_value(item.get("lumens"), reducer="max")
            if wattage not in (None, 0):
                unit_price_per_watt.append(current_unit_price / wattage)
            if lumens not in (None, 0):
                unit_price_per_lumen.append(current_unit_price / lumens)

        pack_quantity = parse_number(item.get("pack_quantity"))
        if pack_quantity and pack_quantity > 1:
            multi_pack_count += 1

    target_profile = as_dict(packet.get("target_profile"))
    business_case = as_dict(target_profile.get("business_case"))
    research_plan = as_dict(packet.get("research_plan"))
    target_msrp = parse_number(business_case.get("target_msrp"))
    target_vendor_cost = parse_number(business_case.get("target_vendor_cost"))
    search_band = as_dict(research_plan.get("target_price_band"))
    search_floor = parse_number(search_band.get("search_floor"))
    search_ceiling = parse_number(search_band.get("search_ceiling"))
    unit_price_stats = summarize_numeric(unit_price_samples)
    unit_price_median = parse_number(unit_price_stats.get("median"))
    observed_floor = parse_number(unit_price_stats.get("p25")) or parse_number(unit_price_stats.get("min"))
    observed_ceiling = parse_number(unit_price_stats.get("p75")) or parse_number(unit_price_stats.get("max"))

    recommended_floor = observed_floor
    recommended_ceiling = observed_ceiling
    if search_floor is not None and recommended_floor is not None:
        recommended_floor = max(recommended_floor, search_floor)
    if search_ceiling is not None and recommended_ceiling is not None:
        recommended_ceiling = min(recommended_ceiling, search_ceiling)
    if (
        recommended_floor is not None
        and recommended_ceiling is not None
        and recommended_floor > recommended_ceiling
    ):
        recommended_floor = observed_floor
        recommended_ceiling = observed_ceiling

    if target_msrp is not None and recommended_floor is not None and recommended_ceiling is not None:
        if target_msrp < recommended_floor:
            positioning = "aggressive"
            anchor = recommended_floor
        elif target_msrp > recommended_ceiling:
            positioning = "premium"
            anchor = recommended_ceiling
        else:
            positioning = "aligned"
            anchor = target_msrp
    else:
        positioning = "undetermined"
        anchor = unit_price_median

    recommendation = compact_dict(
        {
            "observed_unit_price_floor": round_money(recommended_floor),
            "observed_unit_price_ceiling": round_money(recommended_ceiling),
            "anchor": round_money(anchor),
            "positioning": positioning,
        }
    )

    return compact_dict(
        {
            "price_benchmarks": summarize_numeric(price_samples),
            "unit_price_benchmarks": unit_price_stats,
            "unit_price_per_watt_benchmarks": summarize_numeric(unit_price_per_watt),
            "unit_price_per_lumen_benchmarks": summarize_numeric(unit_price_per_lumen),
            "target_msrp": round_money(target_msrp),
            "target_vendor_cost": round_money(target_vendor_cost),
            "target_vs_unit_price_median_pct": safe_pct_delta(target_msrp, unit_price_median),
            "suggested_msrp_range": recommendation,
            "collection_price_band": compact_dict(
                {
                    "search_floor": round_money(search_floor),
                    "search_ceiling": round_money(search_ceiling),
                }
            ),
            "pack_mix": {
                "multi_pack_candidate_count": multi_pack_count,
                "single_unit_candidate_count": max(len(unit_price_samples) - multi_pack_count, 0),
            },
        }
    )


def feature_matches(item: dict[str, Any], label: str) -> bool:
    """Apply lightweight feature matching heuristics."""
    lowered = label.lower()
    title = (normalize_text(item.get("product_title")) or "").lower()
    features = " ".join(item.get("features", [])).lower()
    dimming_type = (normalize_text(item.get("dimming_type")) or "").lower()

    if "0-10v" in lowered:
        return "0-10v" in title or "0-10v" in features or dimming_type == "0-10v"
    if "dimm" in lowered:
        return item.get("dimmable") is True or "dimmable" in title or "dimmable" in features
    if "motion sensor" in lowered:
        return "motion sensor" in title or "motion sensor" in features or "sensor receptacle" in title
    if "auto dim" in lowered or "daylight" in lowered:
        return "auto dim" in title or "daylight" in title or "auto dim" in features
    if "selectable wattage" in lowered:
        return "/" in (normalize_text(item.get("wattage")) or "") or "selectable wattage" in features
    if "selectable cct" in lowered:
        return "/" in (normalize_text(item.get("cct")) or "") or "selectable cct" in features
    if lowered in {"dry", "damp", "wet"}:
        return lowered in title or f"{lowered} rated" in features
    if lowered.startswith("ip"):
        return lowered in title or lowered in features

    tokens = [token for token in re.split(r"[^a-z0-9]+", lowered) if len(token) > 2]
    if not tokens:
        return False
    haystack = f"{title} {features}"
    return all(token in haystack for token in tokens)


def certification_matches(item: dict[str, Any], label: str) -> bool:
    """Match certification labels against normalized certification fields."""
    lowered = label.lower()
    certifications = [normalize_text(value) or "" for value in item.get("certifications", [])]
    if any(lowered == value.lower() for value in certifications):
        return True
    title = (normalize_text(item.get("product_title")) or "").lower()
    return lowered in title


def build_spec_coverage(packet: dict[str, Any], items: list[dict[str, Any]]) -> dict[str, Any]:
    """Measure how well the normalized set covers target features and certifications."""
    research_plan = as_dict(packet.get("research_plan"))
    must_validate = as_dict(research_plan.get("must_validate"))
    certifications = unique_preserve_order(
        [normalize_text(value) for value in as_list(must_validate.get("certifications")) if normalize_text(value)]
    )
    feature_watchlist = []
    for value in as_list(must_validate.get("features")):
        text = normalize_text(value)
        if not text:
            continue
        if "," in text and any(cert.lower() in text.lower() for cert in certifications):
            continue
        feature_watchlist.append(text)
    feature_watchlist = unique_preserve_order(feature_watchlist)

    feature_coverage = []
    for label in feature_watchlist:
        matched_count = sum(1 for item in items if feature_matches(item, label))
        feature_coverage.append(
            {
                "label": label,
                "matched_count": matched_count,
                "coverage_pct": round((matched_count / len(items)) * 100, 2) if items else 0,
            }
        )

    certification_coverage = []
    for label in certifications:
        matched_count = sum(1 for item in items if certification_matches(item, label))
        certification_coverage.append(
            {
                "label": label,
                "matched_count": matched_count,
                "coverage_pct": round((matched_count / len(items)) * 100, 2) if items else 0,
            }
        )

    notable_gaps = []
    for entry in feature_coverage:
        if entry["matched_count"] == 0:
            notable_gaps.append(f"No normalized competitors currently show '{entry['label']}'.")
    for entry in certification_coverage:
        if entry["matched_count"] == 0:
            notable_gaps.append(f"No normalized competitors currently show '{entry['label']}' certification.")

    return {
        "feature_watchlist": feature_watchlist,
        "certification_watchlist": certifications,
        "feature_coverage": feature_coverage,
        "certification_coverage": certification_coverage,
        "notable_gaps": notable_gaps,
    }


def derive_confidence(
    raw_stage_statuses: dict[str, str],
    total_candidates: int,
    non_seed_candidates: int,
) -> str:
    """Estimate how trustworthy the current row analysis is."""
    if total_candidates == 0:
        return "none"
    if non_seed_candidates == 0:
        return "low"
    if all(status == "complete" for status in raw_stage_statuses.values()) and non_seed_candidates >= 8:
        return "high"
    return "medium"


def derive_launch_outlook(
    packet: dict[str, Any],
    pricing_analysis: dict[str, Any],
    spec_coverage: dict[str, Any],
    total_candidates: int,
) -> str:
    """Create a simple outlook label for the current ideation."""
    if total_candidates == 0:
        return "insufficient_data"

    estimation_focus = as_dict(packet.get("estimation_focus"))
    demand_hypothesis = as_dict(estimation_focus.get("demand_hypothesis"))
    sales_growth = parse_number(demand_hypothesis.get("segment_sales_growth_pct"))
    unit_growth = parse_number(demand_hypothesis.get("segment_units_growth_pct"))
    positioning = (
        pricing_analysis.get("suggested_msrp_range", {}).get("positioning")
    )
    gap_count = len(spec_coverage.get("notable_gaps", []))

    if sales_growth is not None and unit_growth is not None:
        if sales_growth > 0 and unit_growth > 0 and positioning in {"aligned", "aggressive"} and gap_count <= 2:
            return "favorable"
        if sales_growth < 0 or unit_growth < 0 or positioning == "premium":
            return "cautious"
    if gap_count >= 3:
        return "cautious"
    return "mixed"


def build_performance_estimation(
    packet: dict[str, Any],
    pricing_analysis: dict[str, Any],
    spec_coverage: dict[str, Any],
    raw_stage_statuses: dict[str, str],
    total_candidates: int,
    non_seed_candidates: int,
) -> dict[str, Any]:
    """Summarize row-level performance outlook using packet + normalized data."""
    market_context = as_dict(as_dict(packet.get("market_context")).get("performance_estimation_context"))
    demand_hypothesis = as_dict(as_dict(packet.get("estimation_focus")).get("demand_hypothesis"))
    segment = as_dict(market_context.get("segment"))
    segment_snapshot = as_dict(segment.get("market_snapshot"))
    market_momentum = as_dict(segment.get("market_momentum_pct"))
    sunco_position = as_dict(market_context.get("sunco_position"))
    reference_family = as_dict(market_context.get("reference_family"))
    posture = normalize_text(demand_hypothesis.get("posture")) or "undetermined"
    confidence = derive_confidence(raw_stage_statuses, total_candidates, non_seed_candidates)
    launch_outlook = derive_launch_outlook(packet, pricing_analysis, spec_coverage, total_candidates)

    rationale = []
    sales_growth = parse_number(demand_hypothesis.get("segment_sales_growth_pct"))
    if sales_growth is not None:
        rationale.append(f"Segment sales growth is {sales_growth:.2f}%.")
    traffic_growth = parse_number(market_momentum.get("traffic"))
    if traffic_growth is not None:
        rationale.append(f"Segment traffic growth is {traffic_growth:.2f}%.")
    sunco_share = parse_number(sunco_position.get("sales_share_pct"))
    if sunco_share is not None:
        rationale.append(f"Sunco sales share in the segment is {sunco_share:.2f}%.")
    if reference_family.get("found") is False:
        rationale.append("The exact reference family is absent from the current Stackline segment bundle.")
    if non_seed_candidates == 0 and total_candidates > 0:
        rationale.append("Current analysis is seeded from Stackline competitors only; channel collection is still pending.")

    return compact_dict(
        {
            "posture": posture,
            "confidence": confidence,
            "launch_outlook": launch_outlook,
            "market_snapshot": compact_dict(
                {
                    "segment_name": segment.get("name"),
                    "segment_retail_sales": round_money(parse_number(segment_snapshot.get("retail_sales"))),
                    "segment_units_sold": round_money(parse_number(segment_snapshot.get("units_sold"))),
                    "segment_sales_growth_pct": parse_number(demand_hypothesis.get("segment_sales_growth_pct")),
                    "segment_units_growth_pct": parse_number(demand_hypothesis.get("segment_units_growth_pct")),
                    "sunco_sales_share_pct": parse_number(sunco_position.get("sales_share_pct")),
                    "reference_family_found_in_stackline": reference_family.get("found"),
                }
            ),
            "rationale": rationale,
        }
    )


def build_recommendations(
    packet: dict[str, Any],
    summary: dict[str, Any],
    pricing_analysis: dict[str, Any],
    spec_coverage: dict[str, Any],
    performance_estimation: dict[str, Any],
) -> list[str]:
    """Generate concise next-step recommendations for this ideation row."""
    recommendations = []
    target_msrp = parse_number(pricing_analysis.get("target_msrp"))
    suggested = pricing_analysis.get("suggested_msrp_range", {})
    suggested_floor = parse_number(suggested.get("observed_unit_price_floor"))
    suggested_ceiling = parse_number(suggested.get("observed_unit_price_ceiling"))
    positioning = normalize_text(suggested.get("positioning"))
    if target_msrp is not None and suggested_floor is not None and suggested_ceiling is not None:
        if positioning == "premium":
            recommendations.append(
                f"Target MSRP ${target_msrp:.2f} sits above the current comparable unit-price band of ${suggested_floor:.2f}-${suggested_ceiling:.2f}; validate premium feature justification or lower price."
            )
        elif positioning == "aggressive":
            recommendations.append(
                f"Target MSRP ${target_msrp:.2f} is below the current comparable unit-price band of ${suggested_floor:.2f}-${suggested_ceiling:.2f}; confirm margin resilience before positioning as a value play."
            )
        elif positioning == "aligned":
            recommendations.append(
                f"Target MSRP ${target_msrp:.2f} sits inside the observed comparable unit-price band of ${suggested_floor:.2f}-${suggested_ceiling:.2f}."
            )

    gaps = spec_coverage.get("notable_gaps", [])
    if gaps:
        recommendations.append(gaps[0])

    raw_stage_statuses = summary.get("raw_stage_statuses", {})
    if summary.get("non_seed_candidate_count", 0) == 0 and summary.get("candidate_count", 0) > 0:
        recommendations.append(
            "Prioritize Claude collection next. Current recommendations are provisional because the normalized set is seeded from Stackline only."
        )
    elif any(status in {"not_started", "in_progress", "missing"} for status in raw_stage_statuses.values()):
        recommendations.append(
            "Complete the remaining raw channel collection before treating this row as final."
        )

    posture = normalize_text(performance_estimation.get("posture"))
    if posture == "defend_existing_share":
        recommendations.append("Use competitor collection to protect Sunco share, not just to chase incremental specs.")
    elif posture == "capture_share":
        recommendations.append("Bias collection toward underpenetrated brands and price points that can win incremental share.")

    return unique_preserve_order(recommendations)


def derive_analysis_status(
    normalized_status: str,
    raw_stage_statuses: dict[str, str],
    total_candidates: int,
    blocking_issues: list[str],
) -> str:
    """Derive the artifact status for the analysis file."""
    if total_candidates == 0 and blocking_issues:
        return "blocked"
    if total_candidates == 0 or normalized_status in {"not_started", "blocked"}:
        return "blocked" if blocking_issues else "not_started"
    if any(status in {"not_started", "in_progress", "missing"} for status in raw_stage_statuses.values()):
        return "in_progress"
    return "complete"


def build_summary(
    items: list[dict[str, Any]],
    normalized_payload: dict[str, Any],
) -> dict[str, Any]:
    """Build row-level summary counts from a normalized artifact."""
    raw_summary = normalized_payload.get("summary", {})
    source_counts = channel_counts(items)
    seed_candidate_count = source_counts.get("stackline_seed", 0)
    total_candidates = len(items)
    non_seed_candidates = total_candidates - seed_candidate_count

    return {
        "normalized_status": normalized_payload.get("artifact_status", "not_started"),
        "raw_stage_statuses": raw_summary.get("raw_stage_statuses", {}),
        "candidate_count": total_candidates,
        "seed_candidate_count": seed_candidate_count,
        "non_seed_candidate_count": non_seed_candidates,
        "source_channel_counts": source_counts,
        "top_brands": top_brands(items),
        "data_coverage": {
            "price_sample_count": sum(1 for item in items if parse_number(item.get("price")) is not None),
            "wattage_sample_count": sum(1 for item in items if representative_value(item.get("wattage")) is not None),
            "lumen_sample_count": sum(1 for item in items if representative_value(item.get("lumens")) is not None),
            "certification_sample_count": sum(1 for item in items if item.get("certifications")),
        },
    }


def build_analysis_artifact(session_dir: Path, row_number: int) -> dict[str, Any] | None:
    """Create one analysis artifact from a packet + normalized artifact."""
    normalized_path = artifact_path_for(session_dir, row_number, "normalized")
    if not normalized_path.exists():
        return None

    normalized_payload = read_json(normalized_path)
    items = [item for item in normalized_payload.get("items", []) if isinstance(item, dict)]
    if not items and normalized_payload.get("artifact_status") == "not_started":
        return None

    packet = read_json(packet_path_for(session_dir, row_number))
    summary = build_summary(items, normalized_payload)
    pricing_analysis = build_pricing_analysis(packet, items)
    spec_coverage = build_spec_coverage(packet, items)
    raw_stage_statuses = summary.get("raw_stage_statuses", {})
    blocking_issues = unique_preserve_order(
        list(normalized_payload.get("blocking_issues", [])) + list(packet.get("issues", []))
    )
    performance_estimation = build_performance_estimation(
        packet=packet,
        pricing_analysis=pricing_analysis,
        spec_coverage=spec_coverage,
        raw_stage_statuses=raw_stage_statuses,
        total_candidates=summary.get("candidate_count", 0),
        non_seed_candidates=summary.get("non_seed_candidate_count", 0),
    )
    status = derive_analysis_status(
        normalized_status=str(normalized_payload.get("artifact_status", "not_started")),
        raw_stage_statuses=raw_stage_statuses,
        total_candidates=summary.get("candidate_count", 0),
        blocking_issues=blocking_issues,
    )
    recommendations = build_recommendations(
        packet=packet,
        summary=summary,
        pricing_analysis=pricing_analysis,
        spec_coverage=spec_coverage,
        performance_estimation=performance_estimation,
    )

    notes = unique_preserve_order(
        list(normalized_payload.get("notes", []))
        + (
            ["Analysis is provisional until raw collection is complete."]
            if status == "in_progress"
            else []
        )
    )

    return compact_dict(
        {
            "schema_version": SCHEMA_VERSION,
            "artifact_type": "analysis",
            "artifact_status": status,
            "batch_id": session_dir.name,
            "row_number": row_number,
            "ideation_name": packet.get("identity", {}).get("ideation_name"),
            "expected_owner": "codex",
            "source_channel_group": "analysis",
            "packet_file": str(packet_path_for(session_dir, row_number).resolve().relative_to(session_dir.resolve())),
            "normalized_file": str(normalized_path.resolve().relative_to(session_dir.resolve())),
            "summary": summary,
            "pricing_analysis": pricing_analysis,
            "spec_coverage": spec_coverage,
            "performance_estimation": performance_estimation,
            "recommendations": recommendations,
            "notes": notes,
            "blocking_issues": blocking_issues,
            "updated_at": utc_now(),
        }
    )


def parse_rows_argument(value: str | None) -> list[int] | None:
    """Parse an optional comma-separated row filter."""
    if not value:
        return None
    rows = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        rows.append(int(part))
    return rows or None


def analyze_session(session_root: str, rows: list[int] | None = None) -> dict[str, Any]:
    """Build analysis artifacts for selected session rows."""
    session_dir = Path(session_root).resolve()
    manifest = read_json(session_dir / "manifest.json")
    target_rows = set(rows or [row["row_number"] for row in manifest.get("rows", [])])

    written_rows = []
    skipped_rows = []

    for row in manifest.get("rows", []):
        row_number = row["row_number"]
        if row_number not in target_rows:
            continue
        artifact = build_analysis_artifact(session_dir, row_number)
        if artifact is None:
            skipped_rows.append(row_number)
            continue
        write_json(artifact_path_for(session_dir, row_number, "analyzed"), artifact)
        written_rows.append(row_number)

    update_result = update_session(str(session_dir))
    return {
        "session_root": str(session_dir),
        "rows_requested": sorted(target_rows),
        "rows_written": written_rows,
        "rows_skipped": skipped_rows,
        "manifest_summary": update_result["summary"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze normalized competitor artifacts for a resumable research session."
    )
    parser.add_argument("session_root", help="Path to an initialized research session.")
    parser.add_argument(
        "--rows",
        default=None,
        help="Optional comma-separated row numbers to analyze.",
    )
    args = parser.parse_args()

    result = analyze_session(
        session_root=args.session_root,
        rows=parse_rows_argument(args.rows),
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
