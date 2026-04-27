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
from functools import lru_cache
from pathlib import Path
from typing import Any

from gate_confidence import build_gate_readiness, build_highest_impact_vendor_requests as build_base_vendor_requests
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
PROFILE_PATH = Path(__file__).resolve().parents[1] / "config" / "category_signal_profiles.json"


def normalize_text(value: Any) -> str | None:
    """Normalize optional values into stripped strings."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "Yes" if value else "No"
    text = str(value).strip()
    return text or None


def normalized_compare_text(value: Any) -> str:
    """Normalize free text for loose feature/certification matching."""
    text = (normalize_text(value) or "").lower()
    text = text.replace("0-10v", "0 10v")
    text = text.replace("back-up", "backup").replace("back up", "backup")
    text = text.replace("re-charge", "recharge")
    text = re.sub(r"[-/]", " ", text)
    text = re.sub(r"\bmins?\b", "minutes", text)
    text = re.sub(r"\bhrs?\b", "hours", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


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


@lru_cache(maxsize=1)
def load_category_signal_profiles() -> dict[str, Any]:
    """Load category-aware optimization profiles."""
    with PROFILE_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


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


def round_metric(value: float | None) -> float | int | None:
    """Round non-monetary numeric values for readable JSON output."""
    if value is None:
        return None
    if float(value).is_integer():
        return int(value)
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


def margin_floor_price(cost: float | None, margin_pct: float | None) -> float | None:
    """Return the minimum price needed to hit a gross-margin target."""
    if cost is None or margin_pct is None:
        return None
    if margin_pct < 0 or margin_pct >= 100:
        return None
    return cost / (1 - (margin_pct / 100))


def percentile_rank(values: list[float], target: float | None) -> float | None:
    """Return the share of values less than or equal to the target."""
    if target is None or not values:
        return None
    ordered = sorted(values)
    rank = sum(1 for value in ordered if value <= target)
    return round((rank / len(ordered)) * 100, 2)


def classify_market_position(percentile_value: float | None) -> str | None:
    """Map a percentile into a plain-language positioning bucket."""
    if percentile_value is None:
        return None
    if percentile_value <= 25:
        return "value"
    if percentile_value <= 60:
        return "mainstream"
    if percentile_value <= 85:
        return "premium"
    return "ultra_premium"


def build_metric_position(
    target_value: float | None,
    sample_values: list[float],
    source: str,
) -> dict[str, Any]:
    """Compare a target metric against a competitive distribution."""
    if target_value is None or not sample_values:
        return {}

    sample_size = len(sample_values)
    median_value = percentile(sample_values, 0.5)
    p75_value = percentile(sample_values, 0.75)
    percentile_value = percentile_rank(sample_values, target_value)
    beat_count = sum(1 for value in sample_values if target_value >= value)

    return compact_dict(
        {
            "evaluated_value": round_money(target_value),
            "evaluated_value_source": source,
            "sample_size": sample_size,
            "percentile": percentile_value,
            "bucket": classify_market_position(percentile_value),
            "beat_count": beat_count,
            "vs_median_pct": safe_pct_delta(target_value, median_value),
            "median": round_money(median_value),
            "p75": round_money(p75_value),
        }
    )


def coverage_signal(coverage_pct: float) -> str:
    """Classify how common a requested feature/certification is."""
    if coverage_pct >= 60:
        return "table_stakes"
    if coverage_pct >= 25:
        return "competitive"
    if coverage_pct > 0:
        return "differentiator"
    return "whitespace"


def coverage_recommendation(
    label: str,
    matched_count: int,
    total_count: int,
    signal: str,
    kind: str,
) -> str:
    """Create actionable guidance for a feature or certification ask."""
    coverage_text = f"{matched_count} of {total_count} competitors"
    if signal == "table_stakes":
        return f"{label} is table stakes: {coverage_text} include it, so Sunco should keep it to stay at parity."
    if signal == "competitive":
        return f"{label} is common but not universal: {coverage_text} include it, so include it if this concept needs mainstream channel parity."
    if signal == "differentiator":
        return f"{label} is a differentiator: only {coverage_text} include it, so keep it only if Sunco wants a premium feature claim."
    if kind == "certification":
        return f"No normalized competitors currently surface {label}; validate whether that certification is a real channel requirement before adding cost."
    return f"No normalized competitors currently surface {label}; validate that it represents a real customer need before locking in added complexity."


def build_numeric_guidance_entry(
    label: str,
    unit: str,
    target_value: float | None,
    sample_values: list[float],
) -> dict[str, Any]:
    """Compare a numeric ideation target against the competitor set."""
    if target_value is None or len(sample_values) < 3:
        return {}

    sample_size = len(sample_values)
    median_value = percentile(sample_values, 0.5)
    p75_value = percentile(sample_values, 0.75)
    percentile_value = percentile_rank(sample_values, target_value)
    beat_count = sum(1 for value in sample_values if target_value >= value)
    if percentile_value is None:
        return {}

    if percentile_value < 50 and median_value is not None:
        signal = "below_market"
        recommendation = (
            f"Increase {label.lower()} to about {round_metric(median_value)}{unit} to clear at least half of current competitors."
        )
    elif percentile_value < 75 and p75_value is not None:
        signal = "mid_pack"
        recommendation = (
            f"{label} is mid-pack today; push toward {round_metric(p75_value)}{unit} if this concept needs a stronger performance claim."
        )
    else:
        signal = "leading"
        recommendation = (
            f"{label} already clears {beat_count} of {sample_size} competitors; keep it only if margin supports a differentiated position."
        )

    return compact_dict(
        {
            "label": label,
            "unit": unit,
            "target_value": round_metric(target_value),
            "sample_size": sample_size,
            "min": round_metric(min(sample_values)),
            "p25": round_metric(percentile(sample_values, 0.25)),
            "median": round_metric(median_value),
            "p75": round_metric(p75_value),
            "max": round_metric(max(sample_values)),
            "target_percentile": percentile_value,
            "beat_count": beat_count,
            "signal": signal,
            "recommended_action": recommendation,
        }
    )


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


def sanitize_metric_value(value: float | None, metric: str) -> float | None:
    """Drop obviously malformed numeric samples before analysis."""
    if value is None:
        return None
    if metric == "wattage" and value < 10:
        return None
    if metric == "lumens" and value < 100:
        return None
    if metric == "cri" and not (60 <= value <= 100):
        return None
    return value


def item_output_wattage(item: dict[str, Any]) -> float | None:
    """Return a usable output-wattage sample, excluding bulb-equivalent labels."""
    value = sanitize_metric_value(
        representative_value(item.get("wattage"), reducer="max"),
        "wattage",
    )
    if value is None:
        return None

    title = normalized_compare_text(item.get("product_title"))
    if value in {40, 50, 60, 65, 75, 100, 120, 150, 200, 300} and (
        "equivalent" in title or re.search(r"\beq\b", title) or "replacement" in title
    ):
        return None
    return value


def should_include_wattage_guidance(packet: dict[str, Any]) -> bool:
    """Skip wattage-tier guidance for categories where wattage is mostly efficiency, not output class."""
    identity = as_dict(packet.get("identity"))
    target_profile = as_dict(packet.get("target_profile"))
    physical = as_dict(target_profile.get("physical"))

    category = normalized_compare_text(identity.get("category"))
    subcategory = normalized_compare_text(identity.get("subcategory"))
    mounting_type = normalized_compare_text(physical.get("mounting_type"))
    form_factor = normalized_compare_text(physical.get("size_form_factor"))

    if category == "bulbs":
        return False
    if mounting_type and "screw in" in mounting_type:
        return False
    if subcategory in {"a series", "a line", "bulbs"}:
        return False
    if form_factor in {"a19", "a21"}:
        return False
    return True


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

            wattage = sanitize_metric_value(
                representative_value(item.get("wattage"), reducer="max"),
                "wattage",
            )
            lumens = sanitize_metric_value(
                representative_value(item.get("lumens"), reducer="max"),
                "lumens",
            )
            if wattage not in (None, 0):
                unit_price_per_watt.append(current_unit_price / wattage)
            if lumens not in (None, 0):
                unit_price_per_lumen.append(current_unit_price / lumens)

        pack_quantity = parse_number(item.get("pack_quantity"))
        if pack_quantity and pack_quantity > 1:
            multi_pack_count += 1

    target_profile = as_dict(packet.get("target_profile"))
    electrical = as_dict(target_profile.get("electrical"))
    business_case = as_dict(target_profile.get("business_case"))
    research_plan = as_dict(packet.get("research_plan"))
    target_msrp = parse_number(business_case.get("target_msrp"))
    target_vendor_cost = parse_number(business_case.get("target_vendor_cost"))
    target_margin_pct_shopify = parse_number(business_case.get("target_margin_pct_shopify"))
    target_margin_pct_amazon = parse_number(business_case.get("target_margin_pct_amazon"))
    search_band = as_dict(research_plan.get("target_price_band"))
    search_floor = parse_number(search_band.get("search_floor"))
    search_ceiling = parse_number(search_band.get("search_ceiling"))
    unit_price_stats = summarize_numeric(unit_price_samples)
    unit_price_median = parse_number(unit_price_stats.get("median"))
    observed_floor = parse_number(unit_price_stats.get("p25")) or parse_number(unit_price_stats.get("min"))
    observed_ceiling = parse_number(unit_price_stats.get("p75")) or parse_number(unit_price_stats.get("max"))
    margin_floor_shopify = margin_floor_price(target_vendor_cost, target_margin_pct_shopify)
    margin_floor_amazon = margin_floor_price(target_vendor_cost, target_margin_pct_amazon)
    minimum_margin_safe_price = max(
        [value for value in [margin_floor_shopify, margin_floor_amazon] if value is not None],
        default=None,
    )

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

    margin_conflict = bool(
        minimum_margin_safe_price is not None
        and recommended_ceiling is not None
        and minimum_margin_safe_price > recommended_ceiling
    )
    if (
        minimum_margin_safe_price is not None
        and recommended_floor is not None
        and not margin_conflict
    ):
        recommended_floor = max(recommended_floor, minimum_margin_safe_price)

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

    evaluated_price = target_msrp
    evaluated_price_source = "target_msrp"
    if evaluated_price is None and minimum_margin_safe_price is not None:
        evaluated_price = minimum_margin_safe_price
        evaluated_price_source = "margin_floor"
    if evaluated_price is None:
        evaluated_price = anchor
        evaluated_price_source = "market_anchor"

    target_wattage = sanitize_metric_value(
        representative_value(
            electrical.get("wattage_max") or electrical.get("wattage_primary"),
            reducer="max",
        ),
        "wattage",
    )
    target_lumens = sanitize_metric_value(
        representative_value(electrical.get("lumens_target"), reducer="max"),
        "lumens",
    )
    target_unit_price_per_watt = None
    target_unit_price_per_lumen = None
    if evaluated_price is not None and target_wattage not in (None, 0):
        target_unit_price_per_watt = evaluated_price / target_wattage
    if evaluated_price is not None and target_lumens not in (None, 0):
        target_unit_price_per_lumen = evaluated_price / target_lumens

    margin_targets = compact_dict(
        {
            "shopify": compact_dict(
                {
                    "target_margin_pct": target_margin_pct_shopify,
                    "minimum_viable_msrp": round_money(margin_floor_shopify),
                    "vs_target_msrp_pct": safe_pct_delta(target_msrp, margin_floor_shopify),
                    "vs_market_median_pct": safe_pct_delta(margin_floor_shopify, unit_price_median),
                }
            ),
            "amazon": compact_dict(
                {
                    "target_margin_pct": target_margin_pct_amazon,
                    "minimum_viable_msrp": round_money(margin_floor_amazon),
                    "vs_target_msrp_pct": safe_pct_delta(target_msrp, margin_floor_amazon),
                    "vs_market_median_pct": safe_pct_delta(margin_floor_amazon, unit_price_median),
                }
            ),
        }
    )

    recommendation = compact_dict(
        {
            "observed_unit_price_floor": round_money(observed_floor),
            "observed_unit_price_ceiling": round_money(observed_ceiling),
            "recommended_floor": round_money(recommended_floor),
            "recommended_ceiling": round_money(recommended_ceiling),
            "minimum_margin_safe_price": round_money(minimum_margin_safe_price),
            "anchor": round_money(anchor),
            "positioning": positioning,
            "margin_conflict": margin_conflict,
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
            "margin_targets": margin_targets,
            "target_price_position": build_metric_position(
                evaluated_price,
                unit_price_samples,
                evaluated_price_source,
            ),
            "target_price_per_watt_position": build_metric_position(
                target_unit_price_per_watt,
                unit_price_per_watt,
                evaluated_price_source,
            ),
            "target_price_per_lumen_position": build_metric_position(
                target_unit_price_per_lumen,
                unit_price_per_lumen,
                evaluated_price_source,
            ),
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
    lowered = normalized_compare_text(label)
    title = normalized_compare_text(item.get("product_title"))
    features = normalized_compare_text(" ".join(item.get("features", [])))
    dimming_type = normalized_compare_text(item.get("dimming_type"))

    if "0-10v" in lowered:
        return "0 10v" in title or "0 10v" in features or dimming_type == "0 10v"
    if "dimm" in lowered:
        return item.get("dimmable") is True or "dimmable" in title or "dimmable" in features
    if "motion sensor" in lowered:
        return "motion sensor" in title or "motion sensor" in features or "sensor receptacle" in title
    if "auto dim" in lowered or "daylight" in lowered:
        return "auto dim" in title or "daylight" in title or "auto dim" in features
    if "emergency battery backup" in lowered:
        haystack = f"{title} {features}"
        return "battery backup" in haystack or "emergency battery" in haystack
    if "90 minutes" in lowered and "runtime" in lowered:
        haystack = f"{title} {features}"
        return bool(re.search(r"\b90\b.*\bminute", haystack))
    if "24 hours" in lowered and "charge time" in lowered:
        haystack = f"{title} {features}"
        return bool(re.search(r"\b24\b.*\bhour", haystack)) and (
            "charge" in haystack or "recharge" in haystack
        )
    if "switching time" in lowered:
        haystack = f"{title} {features}"
        return "switching time" in haystack or "instant on" in haystack
    if "selectable wattage" in lowered:
        return "/" in (normalize_text(item.get("wattage")) or "") or "selectable wattage" in features
    if "selectable cct" in lowered:
        return "/" in (normalize_text(item.get("cct")) or "") or "selectable cct" in features
    if lowered in {"dry", "damp", "wet"}:
        return lowered in title or lowered in features or f"{lowered} rated" in features
    if lowered.startswith("ip"):
        return lowered in title or lowered in features

    tokens = [token for token in re.split(r"[^a-z0-9]+", lowered) if len(token) > 2]
    if not tokens:
        return False
    haystack = f"{title} {features}"
    return all(token in haystack for token in tokens)


def certification_matches(item: dict[str, Any], label: str) -> bool:
    """Match certification labels against normalized certification fields."""
    lowered = normalized_compare_text(label)
    certifications = [normalized_compare_text(value) for value in item.get("certifications", [])]
    if any(lowered == value for value in certifications):
        return True
    if any(lowered in value for value in certifications):
        return True
    title = normalized_compare_text(item.get("product_title"))
    return lowered in title


def build_spec_coverage(packet: dict[str, Any], items: list[dict[str, Any]]) -> dict[str, Any]:
    """Measure how well the normalized set covers target features and certifications."""
    research_plan = as_dict(packet.get("research_plan"))
    target_profile = as_dict(packet.get("target_profile"))
    electrical = as_dict(target_profile.get("electrical"))
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
        coverage_pct = round((matched_count / len(items)) * 100, 2) if items else 0
        signal = coverage_signal(coverage_pct)
        feature_coverage.append(
            compact_dict(
                {
                    "label": label,
                    "matched_count": matched_count,
                    "coverage_pct": coverage_pct,
                    "signal": signal,
                    "recommended_action": coverage_recommendation(
                        label,
                        matched_count,
                        len(items),
                        signal,
                        "feature",
                    ),
                }
            )
        )

    certification_coverage = []
    for label in certifications:
        matched_count = sum(1 for item in items if certification_matches(item, label))
        coverage_pct = round((matched_count / len(items)) * 100, 2) if items else 0
        signal = coverage_signal(coverage_pct)
        certification_coverage.append(
            compact_dict(
                {
                    "label": label,
                    "matched_count": matched_count,
                    "coverage_pct": coverage_pct,
                    "signal": signal,
                    "recommended_action": coverage_recommendation(
                        label,
                        matched_count,
                        len(items),
                        signal,
                        "certification",
                    ),
                }
            )
        )

    numeric_guidance = []
    lumens_samples = [
        value
        for value in [
            sanitize_metric_value(
                representative_value(item.get("lumens"), reducer="max"),
                "lumens",
            )
            for item in items
        ]
        if value is not None
    ]
    cri_samples = [
        value
        for value in [
            sanitize_metric_value(
                representative_value(item.get("cri"), reducer="max"),
                "cri",
            )
            for item in items
        ]
        if value is not None
    ]
    wattage_samples = [value for value in [item_output_wattage(item) for item in items] if value is not None]

    for entry in [
        build_numeric_guidance_entry(
            "Lumens",
            " lm",
            sanitize_metric_value(
                representative_value(electrical.get("lumens_target"), reducer="max"),
                "lumens",
            ),
            lumens_samples,
        ),
        build_numeric_guidance_entry(
            "CRI",
            "",
            sanitize_metric_value(
                representative_value(electrical.get("cri"), reducer="max"),
                "cri",
            ),
            cri_samples,
        ),
        build_numeric_guidance_entry(
            "Output Wattage Tier",
            " W",
            sanitize_metric_value(
                representative_value(
                    electrical.get("wattage_max") or electrical.get("wattage_primary"),
                    reducer="max",
                ),
                "wattage",
            )
            if should_include_wattage_guidance(packet)
            else None,
            wattage_samples,
        ),
    ]:
        if entry:
            numeric_guidance.append(entry)

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
        "numeric_guidance": numeric_guidance,
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
    margin_conflict = bool(
        as_dict(pricing_analysis.get("suggested_msrp_range")).get("margin_conflict")
    )

    if sales_growth is not None and unit_growth is not None:
        if sales_growth > 0 and unit_growth > 0 and positioning in {"aligned", "aggressive"} and gap_count <= 2:
            return "favorable"
        if sales_growth < 0 or unit_growth < 0 or positioning == "premium":
            return "cautious"
    if margin_conflict:
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
    packet_market_context = as_dict(packet.get("market_context"))
    market_context = as_dict(packet_market_context.get("performance_estimation_context"))
    channel_comparison = as_dict(packet_market_context.get("channel_comparison"))
    channel_snapshots = as_dict(channel_comparison.get("channels"))
    channel_delta = as_dict(as_dict(channel_comparison.get("comparisons")).get("amazon_vs_home_depot"))
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
    if parse_number(channel_delta.get("avg_retail_price_gap_pct")) is not None:
        rationale.append(
            f"Amazon average retail price is {parse_number(channel_delta.get('avg_retail_price_gap_pct')):.2f}% above Home Depot for the matched Stackline segment."
        )
    if parse_number(channel_delta.get("retail_sales_gap_pct")) is not None:
        rationale.append(
            f"Amazon segment sales are {parse_number(channel_delta.get('retail_sales_gap_pct')):.2f}% above Home Depot for this segment."
        )
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
            "channel_comparison": compact_dict(
                {
                    "available_channels": list(channel_snapshots.keys()) if channel_snapshots else None,
                    "channels": channel_snapshots,
                    "comparisons": as_dict(channel_comparison.get("comparisons")),
                }
            ),
            "rationale": rationale,
        }
    )


def build_reference_anchor_context(packet: dict[str, Any]) -> dict[str, Any]:
    """Explain how the reference SKU should be used in downstream reasoning."""
    reference = as_dict(packet.get("reference_baseline"))
    shopify_revenue = parse_number(reference.get("shopify_revenue_12mo"))
    amazon_revenue = parse_number(reference.get("amazon_revenue_12mo"))
    listing_price = parse_number(reference.get("listing_price"))
    listing_price_note = normalize_text(reference.get("listing_price_note"))

    title_present = bool(normalize_text(reference.get("title")))
    listing_present = listing_price is not None
    has_any_sales = shopify_revenue is not None or amazon_revenue is not None
    both_sales_present = shopify_revenue is not None and amazon_revenue is not None

    if title_present and listing_present and both_sales_present and not listing_price_note:
        data_quality = "strong_anchor_context"
    elif title_present and (listing_present or has_any_sales):
        data_quality = "usable_anchor_context"
    elif title_present or has_any_sales:
        data_quality = "limited_anchor_context"
    else:
        data_quality = "weak_anchor_context"

    caution_parts = []
    if listing_price_note:
        caution_parts.append("Postgres listing price was rejected, so the report keeps a fallback listing price.")
    if not both_sales_present:
        caution_parts.append("Channel sales coverage is incomplete, so commercial context is directional only.")
    if not listing_present:
        caution_parts.append("Listing price is missing, so do not use the anchor for direct MSRP calibration.")
    if not caution_parts:
        caution_parts.append("Use this anchor as category and schema context first; competitor and Stackline evidence should still drive final pricing and spec decisions.")

    return compact_dict(
        {
            "anchor_role": "category_schema_anchor",
            "primary_use": "Understand the incumbent Sunco family, expected feature schema, and relevant product language for this ideation.",
            "secondary_use": "Provide a directional commercial sanity check on existing price points and channel presence.",
            "do_not_overweight": "Do not let the reference anchor override Stackline market context or the broader competitor set when setting final MSRP or feature priorities.",
            "data_quality": data_quality,
            "caution": " ".join(caution_parts),
        }
    )


def label_key(value: Any) -> str:
    """Normalize labels for loose matching across profile configs and analysis outputs."""
    return normalized_compare_text(value)


def labels_match(left: Any, right: Any) -> bool:
    """Return whether two labels are close enough to treat as the same driver."""
    left_key = label_key(left)
    right_key = label_key(right)
    if not left_key or not right_key:
        return False
    return left_key == right_key or left_key in right_key or right_key in left_key


def packet_profile_haystack(packet: dict[str, Any]) -> str:
    """Build the normalized matching text used to detect the category signal profile."""
    identity = as_dict(packet.get("identity"))
    target_profile = as_dict(packet.get("target_profile"))
    physical = as_dict(target_profile.get("physical"))
    feature_watchlist = as_list(target_profile.get("feature_watchlist"))
    reference = as_dict(packet.get("reference_baseline"))
    values = [
        identity.get("category"),
        identity.get("subcategory"),
        identity.get("ideation_name"),
        identity.get("strategy"),
        reference.get("product_type"),
        physical.get("size_form_factor"),
        physical.get("mounting_type"),
        target_profile.get("research_notes"),
        *feature_watchlist,
    ]
    return " ".join(label_key(value) for value in values if label_key(value))


def matched_keywords_for_haystack(haystack: str, keywords: list[Any]) -> list[str]:
    """Return normalized keywords that are present in the packet haystack."""
    return [keyword for keyword in (label_key(value) for value in keywords) if keyword and keyword in haystack]


def detect_profile_modifiers(haystack: str, profile_id: str | None) -> list[dict[str, Any]]:
    """Detect additive profile modifiers that sharpen the base category lens."""
    config = load_category_signal_profiles()
    modifiers = []
    for modifier in as_list(config.get("modifiers")):
        applies_to = {normalize_text(value) for value in as_list(modifier.get("profile_ids")) if normalize_text(value)}
        if applies_to and profile_id not in applies_to:
            continue
        excluded = matched_keywords_for_haystack(haystack, as_list(modifier.get("exclude_keywords")))
        if excluded:
            continue
        matched = matched_keywords_for_haystack(haystack, as_list(modifier.get("match_keywords")))
        minimum_matches = int(parse_number(modifier.get("minimum_matches")) or 1)
        if len(matched) < minimum_matches:
            continue
        payload = dict(modifier)
        payload["matched_keywords"] = matched
        payload["match_score"] = len(matched)
        modifiers.append(payload)
    modifiers.sort(
        key=lambda item: (
            int(parse_number(item.get("display_order")) or 999),
            -int(parse_number(item.get("match_score")) or 0),
            normalize_text(item.get("label")) or "",
        )
    )
    return modifiers


def detect_category_signal_profile(packet: dict[str, Any]) -> dict[str, Any]:
    """Pick the best category-aware optimization profile for the current row."""
    config = load_category_signal_profiles()
    profiles = as_list(config.get("profiles"))
    haystack = packet_profile_haystack(packet)
    generic = next((profile for profile in profiles if profile.get("id") == "generic_lighting"), {})
    best_profile = generic
    best_score = -1
    matched_keywords: list[str] = []

    for profile in profiles:
        keywords = [label_key(keyword) for keyword in as_list(profile.get("match_keywords")) if label_key(keyword)]
        score = sum(1 for keyword in keywords if keyword and keyword in haystack)
        if score > best_score:
            best_profile = profile
            best_score = score
            matched_keywords = [keyword for keyword in keywords if keyword and keyword in haystack]

    result = dict(best_profile)
    active_modifiers = detect_profile_modifiers(haystack, normalize_text(best_profile.get("id")))
    result["match_score"] = best_score
    result["matched_keywords"] = matched_keywords
    result["active_modifiers"] = active_modifiers
    return result


def find_signal_entry(entries: list[dict[str, Any]], target_label: str) -> dict[str, Any] | None:
    """Find the first coverage/guidance entry that matches a profile driver label."""
    for entry in entries:
        if labels_match(entry.get("label"), target_label):
            return entry
    return None


def feature_signal_reason(entry: dict[str, Any], driver_type: str) -> str:
    """Explain why a feature/certification driver matters for this ideation."""
    signal = normalize_text(entry.get("signal")) or "competitive"
    coverage_pct = parse_number(entry.get("coverage_pct"))
    label = normalize_text(entry.get("label")) or driver_type.title()
    if coverage_pct is None:
        return f"{label} is being tracked as a {signal.replace('_', ' ')} signal."
    return f"{label} reads as {signal.replace('_', ' ')} at {coverage_pct:.2f}% competitor coverage."


def numeric_signal_reason(entry: dict[str, Any]) -> str:
    """Explain numeric positioning guidance in plain language."""
    label = normalize_text(entry.get("label")) or "Numeric target"
    signal = normalize_text(entry.get("signal")) or "mid_pack"
    percentile_value = parse_number(entry.get("target_percentile"))
    if percentile_value is None:
        return f"{label} currently reads as {signal.replace('_', ' ')} versus the competitor set."
    return f"{label} currently lands around the {percentile_value:.0f}th percentile and reads as {signal.replace('_', ' ')}."


def static_driver_payloads(entries: list[dict[str, Any]], tier: str) -> list[dict[str, Any]]:
    """Normalize static config-defined drivers into the shared optimization shape."""
    payloads = []
    for entry in entries:
        payloads.append(
            compact_dict(
                {
                    "tier": tier,
                    "label": normalize_text(entry.get("label")),
                    "driver_type": normalize_text(entry.get("driver_type")) or "context",
                    "signal": normalize_text(entry.get("signal")) or "competitive",
                    "reason": normalize_text(entry.get("reason")) or "This variant changes what matters most for the ideation.",
                }
            )
        )
    return [entry for entry in payloads if entry]


def merged_profile_labels(profile: dict[str, Any], modifiers: list[dict[str, Any]], field_name: str) -> list[Any]:
    """Combine base profile labels with any additive modifier labels for the same field."""
    merged = list(as_list(profile.get(field_name)))
    for modifier in modifiers:
        merged.extend(as_list(modifier.get(field_name)))
    return merged


def price_driver_reason(pricing_analysis: dict[str, Any]) -> str | None:
    """Describe the pricing driver in category-agnostic language."""
    position = as_dict(pricing_analysis.get("target_price_position"))
    percentile_value = parse_number(position.get("percentile"))
    bucket = normalize_text(position.get("bucket"))
    evaluated_value = parse_number(position.get("evaluated_value"))
    if percentile_value is None or bucket is None or evaluated_value is None:
        return None
    return (
        f"Target MSRP ${evaluated_value:.2f} sits around the {percentile_value:.0f}th percentile, "
        f"which is a {bucket.replace('_', ' ')} market position."
    )


def priority_rank(priority: str | None) -> int:
    """Map textual priority to a stable ranking score."""
    return {"critical": 0, "high": 1, "medium": 2, "low": 3}.get((priority or "").lower(), 9)


def optimization_signal_score(signal: str | None) -> float:
    """Map optimization signals into a comparable 0-10 score."""
    mapping = {
        "table_stakes": 9.0,
        "competitive": 7.0,
        "leading": 9.0,
        "mid_pack": 6.0,
        "below_market": 3.0,
        "whitespace": 4.0,
        "value": 8.0,
        "mainstream": 8.0,
        "premium": 5.0,
        "ultra_premium": 3.0,
    }
    return mapping.get((signal or "").lower(), 6.0)


def optimization_label(score: float | None) -> str | None:
    """Convert an optimization score into a plain-language label."""
    if score is None:
        return None
    if score >= 8.5:
        return "strong"
    if score >= 7:
        return "solid"
    if score >= 5.5:
        return "mixed"
    return "caution"


def build_optimization_scorecard(
    pricing_analysis: dict[str, Any],
    performance_estimation: dict[str, Any],
    primary_drivers: list[dict[str, Any]],
    low_signal_attributes: list[dict[str, Any]],
) -> dict[str, Any]:
    """Score how well the ideation is configured for its category, separate from gate readiness."""
    suggested = as_dict(pricing_analysis.get("suggested_msrp_range"))
    target_position = as_dict(pricing_analysis.get("target_price_position"))
    positioning = normalize_text(suggested.get("positioning"))
    margin_conflict = bool(suggested.get("margin_conflict"))
    minimum_margin_safe_price = parse_number(suggested.get("minimum_margin_safe_price"))
    target_msrp = parse_number(pricing_analysis.get("target_msrp"))
    launch_outlook = normalize_text(performance_estimation.get("launch_outlook"))
    optimization_confidence = normalize_text(performance_estimation.get("confidence")) or "medium"

    if margin_conflict:
        price_score = 3.0
        price_reason = "Current target pricing conflicts with the minimum margin-safe price."
    elif positioning == "aligned":
        price_score = 9.0
        price_reason = "Target pricing sits inside the observed comparable band."
    elif positioning == "aggressive":
        price_score = 8.0
        price_reason = "Target pricing is a value-leaning move that can work if margins hold."
    elif positioning == "premium":
        price_score = 5.0
        price_reason = "Target pricing sits above the market band and needs stronger feature justification."
    else:
        price_score = optimization_signal_score(normalize_text(target_position.get("bucket")))
        price_reason = "Price position is being inferred from the observed competitor percentile."

    non_price_primary = [entry for entry in primary_drivers if normalize_text(entry.get("driver_type")) != "pricing"]
    if non_price_primary:
        primary_driver_score = round(
            sum(optimization_signal_score(normalize_text(entry.get("signal"))) for entry in non_price_primary)
            / len(non_price_primary),
            2,
        )
        primary_reason = f"{len(non_price_primary)} primary category drivers are currently represented in the optimization lens."
    else:
        primary_driver_score = 5.0
        primary_reason = "Primary category-specific drivers are still sparse, so this remains a placeholder fit score."

    low_signal_count = len(low_signal_attributes)
    if low_signal_count == 0:
        risk_score = 9.0
        risk_reason = "No major low-signal asks are being overweighted."
    elif low_signal_count == 1:
        risk_score = 7.0
        risk_reason = "One low-signal attribute still needs validation before it should influence vendor scope."
    elif low_signal_count == 2:
        risk_score = 5.0
        risk_reason = "Multiple low-signal asks could distract from the highest-impact changes."
    else:
        risk_score = 3.5
        risk_reason = "Too many low-signal asks are still in scope relative to the core category drivers."

    if margin_conflict:
        margin_score = 2.0
        margin_reason = "Margin conflict is still unresolved."
    elif minimum_margin_safe_price is not None and target_msrp is not None and minimum_margin_safe_price <= target_msrp:
        margin_score = 9.0
        margin_reason = "Target MSRP clears the current minimum margin-safe threshold."
    elif minimum_margin_safe_price is not None:
        margin_score = 6.0
        margin_reason = "Margin viability is partially known, but the target MSRP is still close to the safety floor."
    else:
        margin_score = 5.0
        margin_reason = "Margin viability is still partially inferred."

    if launch_outlook == "favorable":
        market_score = 8.5
        market_reason = "Current market context and spec posture support a favorable launch read."
    elif launch_outlook == "mixed":
        market_score = 6.5
        market_reason = "Market context is usable but still mixed."
    elif launch_outlook == "cautious":
        market_score = 4.0
        market_reason = "Current evidence suggests a cautious launch posture."
    else:
        market_score = 5.0
        market_reason = "Market support is still provisional."

    components = [
        {"component": "Price Fit", "score": price_score, "weight": 0.25, "reason": price_reason},
        {"component": "Primary Driver Fit", "score": primary_driver_score, "weight": 0.30, "reason": primary_reason},
        {"component": "Margin Viability", "score": margin_score, "weight": 0.20, "reason": margin_reason},
        {"component": "Market Support", "score": market_score, "weight": 0.15, "reason": market_reason},
        {"component": "Low-Signal Risk", "score": risk_score, "weight": 0.10, "reason": risk_reason},
    ]
    weighted_total = round(sum(entry["score"] * entry["weight"] for entry in components), 2)
    return {
        "score": weighted_total,
        "label": optimization_label(weighted_total),
        "confidence": optimization_confidence,
        "components": components,
    }


def build_ideation_optimization(
    packet: dict[str, Any],
    pricing_analysis: dict[str, Any],
    spec_coverage: dict[str, Any],
    performance_estimation: dict[str, Any],
    base_vendor_requests: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build a category-aware optimization lens that stays separate from gate readiness."""
    profile = detect_category_signal_profile(packet)
    active_modifiers = as_list(profile.get("active_modifiers"))
    feature_coverage = as_list(spec_coverage.get("feature_coverage"))
    certification_coverage = as_list(spec_coverage.get("certification_coverage"))
    numeric_guidance = as_list(spec_coverage.get("numeric_guidance"))

    primary_drivers: list[dict[str, Any]] = []
    secondary_drivers: list[dict[str, Any]] = []
    low_signal_attributes: list[dict[str, Any]] = []

    def append_driver(bucket: list[dict[str, Any]], label: str, driver_type: str, entry: dict[str, Any], tier: str) -> None:
        signal = normalize_text(entry.get("signal"))
        if driver_type == "numeric":
            reason = numeric_signal_reason(entry)
        else:
            reason = feature_signal_reason(entry, driver_type)
        bucket.append(
            compact_dict(
                {
                    "tier": tier,
                    "label": normalize_text(entry.get("label")) or label,
                    "driver_type": driver_type,
                    "signal": signal,
                    "reason": reason,
                }
            )
        )

    def dedupe_driver_bucket(bucket: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen = set()
        deduped = []
        for entry in bucket:
            marker = (label_key(entry.get("label")), normalize_text(entry.get("driver_type")))
            if marker in seen:
                continue
            seen.add(marker)
            deduped.append(entry)
        return deduped

    price_reason = price_driver_reason(pricing_analysis)
    if price_reason:
        primary_drivers.append(
            {
                "tier": "primary",
                "label": "Price Position",
                "driver_type": "pricing",
                "signal": normalize_text(as_dict(pricing_analysis.get("target_price_position")).get("bucket")),
                "reason": price_reason,
            }
        )

    for label in merged_profile_labels(profile, active_modifiers, "primary_features"):
        entry = find_signal_entry(feature_coverage, str(label))
        if entry:
            append_driver(primary_drivers, str(label), "feature", entry, "primary")
    for label in merged_profile_labels(profile, active_modifiers, "primary_certifications"):
        entry = find_signal_entry(certification_coverage, str(label))
        if entry:
            append_driver(primary_drivers, str(label), "certification", entry, "primary")
    for label in merged_profile_labels(profile, active_modifiers, "primary_numeric"):
        entry = find_signal_entry(numeric_guidance, str(label))
        if entry:
            append_driver(primary_drivers, str(label), "numeric", entry, "primary")

    for label in merged_profile_labels(profile, active_modifiers, "secondary_features"):
        entry = find_signal_entry(feature_coverage, str(label))
        if entry:
            append_driver(secondary_drivers, str(label), "feature", entry, "secondary")
    for label in merged_profile_labels(profile, active_modifiers, "secondary_certifications"):
        entry = find_signal_entry(certification_coverage, str(label))
        if entry:
            append_driver(secondary_drivers, str(label), "certification", entry, "secondary")

    for label in merged_profile_labels(profile, active_modifiers, "low_signal_features"):
        entry = find_signal_entry(feature_coverage, str(label))
        if not entry:
            continue
        low_signal_attributes.append(
            compact_dict(
                {
                    "label": normalize_text(entry.get("label")) or str(label),
                    "driver_type": "feature",
                    "signal": normalize_text(entry.get("signal")),
                    "reason": feature_signal_reason(entry, "feature"),
                }
            )
        )

    primary_drivers.extend(static_driver_payloads(as_list(profile.get("static_primary_drivers")), "primary"))
    secondary_drivers.extend(static_driver_payloads(as_list(profile.get("static_secondary_drivers")), "secondary"))
    low_signal_attributes.extend(static_driver_payloads(as_list(profile.get("static_low_signal_attributes")), "secondary"))
    for modifier in active_modifiers:
        primary_drivers.extend(static_driver_payloads(as_list(modifier.get("static_primary_drivers")), "primary"))
        secondary_drivers.extend(static_driver_payloads(as_list(modifier.get("static_secondary_drivers")), "secondary"))
        low_signal_attributes.extend(static_driver_payloads(as_list(modifier.get("static_low_signal_attributes")), "secondary"))

    primary_drivers = dedupe_driver_bucket(primary_drivers)
    secondary_drivers = dedupe_driver_bucket(secondary_drivers)
    low_signal_attributes = dedupe_driver_bucket(low_signal_attributes)

    primary_keys = {label_key(entry.get("label")) for entry in primary_drivers}
    secondary_keys = {label_key(entry.get("label")) for entry in secondary_drivers}
    low_signal_keys = {label_key(entry.get("label")) for entry in low_signal_attributes}

    scored_requests = []
    for request in base_vendor_requests:
        linked_metric = label_key(request.get("linked_metric"))
        request_text = label_key(request.get("request"))
        relevance_score = 0
        if linked_metric == "margin_viability":
            relevance_score += 30
        if linked_metric in primary_keys or any(key and key in request_text for key in primary_keys):
            relevance_score += 35
        elif linked_metric in secondary_keys or any(key and key in request_text for key in secondary_keys):
            relevance_score += 15
        if linked_metric in low_signal_keys or any(key and key in request_text for key in low_signal_keys):
            relevance_score -= 20
        scored_requests.append((priority_rank(normalize_text(request.get("priority"))), -relevance_score, request))

    vendor_requests = []
    seen_requests = set()
    for _, _, request in sorted(scored_requests, key=lambda item: (item[0], item[1], request_text if (request_text := label_key(item[2].get("request"))) else "")):
        request_text = normalize_text(request.get("request"))
        if not request_text or request_text in seen_requests:
            continue
        seen_requests.add(request_text)
        vendor_requests.append(request)
    vendor_requests = vendor_requests[:5]

    driver_labels = [normalize_text(entry.get("label")) for entry in primary_drivers if normalize_text(entry.get("label"))]
    low_signal_labels = [normalize_text(entry.get("label")) for entry in low_signal_attributes if normalize_text(entry.get("label"))]
    modifier_labels = [normalize_text(entry.get("label")) for entry in active_modifiers if normalize_text(entry.get("label"))]
    modifier_lenses = [normalize_text(entry.get("decision_lens_addendum")) for entry in active_modifiers if normalize_text(entry.get("decision_lens_addendum"))]
    summary_parts = [normalize_text(profile.get("decision_lens")) or "Use the category lens to separate essential spec work from lower-signal asks."]
    if modifier_labels:
        summary_parts.append(f"Active variant modifiers: {', '.join(modifier_labels[:4])}.")
    if modifier_lenses:
        summary_parts.extend(modifier_lenses[:2])
    if driver_labels:
        summary_parts.append(f"Primary decision drivers are {', '.join(driver_labels[:3])}.")
    if low_signal_labels:
        summary_parts.append(f"Treat {', '.join(low_signal_labels[:2])} as lower-signal until the market need is proven.")

    optimization_confidence = normalize_text(performance_estimation.get("confidence")) or "medium"
    scorecard = build_optimization_scorecard(
        pricing_analysis=pricing_analysis,
        performance_estimation=performance_estimation,
        primary_drivers=primary_drivers,
        low_signal_attributes=low_signal_attributes,
    )
    if parse_number(scorecard.get("score")) is not None and normalize_text(scorecard.get("label")):
        summary_parts.append(
            f"Optimization score is {parse_number(scorecard.get('score')):.2f}/10 ({normalize_text(scorecard.get('label'))})."
        )
    return compact_dict(
        {
            "profile_id": profile.get("id"),
            "profile_label": profile.get("label"),
            "decision_lens": profile.get("decision_lens"),
            "matched_keywords": profile.get("matched_keywords"),
            "active_modifiers": [
                compact_dict(
                    {
                        "id": modifier.get("id"),
                        "label": modifier.get("label"),
                        "matched_keywords": modifier.get("matched_keywords"),
                        "match_score": modifier.get("match_score"),
                    }
                )
                for modifier in active_modifiers
            ],
            "optimization_confidence": optimization_confidence,
            "optimization_scorecard": scorecard,
            "summary": " ".join(summary_parts),
            "primary_decision_drivers": primary_drivers,
            "secondary_decision_drivers": secondary_drivers,
            "low_signal_attributes": low_signal_attributes,
            "vendor_requests": vendor_requests,
        }
    )


def build_recommendations(
    packet: dict[str, Any],
    summary: dict[str, Any],
    pricing_analysis: dict[str, Any],
    spec_coverage: dict[str, Any],
    performance_estimation: dict[str, Any],
    ideation_optimization: dict[str, Any],
) -> list[str]:
    """Generate concise next-step recommendations for this ideation row."""
    recommendations = []
    target_msrp = parse_number(pricing_analysis.get("target_msrp"))
    suggested = pricing_analysis.get("suggested_msrp_range", {})
    suggested_floor = parse_number(suggested.get("observed_unit_price_floor"))
    suggested_ceiling = parse_number(suggested.get("observed_unit_price_ceiling"))
    recommended_floor = parse_number(suggested.get("recommended_floor"))
    recommended_ceiling = parse_number(suggested.get("recommended_ceiling"))
    minimum_margin_safe_price = parse_number(suggested.get("minimum_margin_safe_price"))
    positioning = normalize_text(suggested.get("positioning"))
    if (
        minimum_margin_safe_price is not None
        and suggested_ceiling is not None
        and minimum_margin_safe_price > suggested_ceiling
    ):
        recommendations.append(
            f"Minimum margin-safe MSRP of ${minimum_margin_safe_price:.2f} sits above the current market ceiling of ${suggested_ceiling:.2f}; either lower cost or justify a premium launch story."
        )
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
    elif (
        recommended_floor is not None
        and recommended_ceiling is not None
        and recommended_floor <= recommended_ceiling
    ):
        recommendations.append(
            f"Use ${recommended_floor:.2f}-${recommended_ceiling:.2f} as the working MSRP band until PMs set an explicit target MSRP."
        )

    target_price_position = as_dict(pricing_analysis.get("target_price_position"))
    percentile_value = parse_number(target_price_position.get("percentile"))
    price_bucket = normalize_text(target_price_position.get("bucket"))
    evaluated_value = parse_number(target_price_position.get("evaluated_value"))
    if evaluated_value is not None and percentile_value is not None and price_bucket:
        recommendations.append(
            f"At ${evaluated_value:.2f}, this concept prices around the {percentile_value:.0f}th percentile of comparable unit prices, which is a {price_bucket.replace('_', ' ')} market position."
        )

    feature_actions = [
        normalize_text(entry.get("recommended_action"))
        for entry in as_list(spec_coverage.get("feature_coverage"))
        if normalize_text(entry.get("recommended_action"))
        and normalize_text(entry.get("signal")) in {"table_stakes", "whitespace"}
    ]
    certification_actions = [
        normalize_text(entry.get("recommended_action"))
        for entry in as_list(spec_coverage.get("certification_coverage"))
        if normalize_text(entry.get("recommended_action"))
        and normalize_text(entry.get("signal")) in {"table_stakes", "whitespace"}
    ]
    numeric_actions = [
        normalize_text(entry.get("recommended_action"))
        for entry in as_list(spec_coverage.get("numeric_guidance"))
        if normalize_text(entry.get("recommended_action"))
    ]

    for bucket in (feature_actions, certification_actions, numeric_actions):
        if bucket:
            recommendations.append(bucket[0])

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

    optimization_summary = normalize_text(ideation_optimization.get("summary"))
    if optimization_summary:
        recommendations.insert(0, optimization_summary)
    for request in as_list(ideation_optimization.get("vendor_requests"))[:2]:
        request_text = normalize_text(as_dict(request).get("request"))
        if request_text:
            recommendations.append(request_text)

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
    base_vendor_requests = build_base_vendor_requests(
        pricing_analysis=pricing_analysis,
        spec_coverage=spec_coverage,
    )
    ideation_optimization = build_ideation_optimization(
        packet=packet,
        pricing_analysis=pricing_analysis,
        spec_coverage=spec_coverage,
        performance_estimation=performance_estimation,
        base_vendor_requests=base_vendor_requests,
    )
    highest_impact_vendor_requests = as_list(ideation_optimization.get("vendor_requests")) or base_vendor_requests
    recommendations = build_recommendations(
        packet=packet,
        summary=summary,
        pricing_analysis=pricing_analysis,
        spec_coverage=spec_coverage,
        performance_estimation=performance_estimation,
        ideation_optimization=ideation_optimization,
    )
    reference_anchor_context = build_reference_anchor_context(packet)
    gate_readiness = build_gate_readiness(
        packet=packet,
        pricing_analysis=pricing_analysis,
        spec_coverage=spec_coverage,
        performance_estimation=performance_estimation,
    )

    notes = unique_preserve_order(
        list(normalized_payload.get("notes", []))
        + [
            normalize_text(reference_anchor_context.get("do_not_overweight")) or "",
            normalize_text(gate_readiness.get("summary")) or "",
        ]
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
            "reference_anchor_context": reference_anchor_context,
            "ideation_optimization": ideation_optimization,
            "gate_readiness": gate_readiness,
            "highest_impact_vendor_requests": highest_impact_vendor_requests,
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
