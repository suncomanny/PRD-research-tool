"""
Step 4: Competitive research packet builder.

This script turns enriched ideation rows into structured research packets that
are ready for channel-by-channel competitor collection and pricing validation.

Usage:
  python tools/competitive_research_engine.py templates/PRD_Research_Template.xlsx
  python tools/competitive_research_engine.py "C:\\path\\to\\filled_workbook.xlsx" --output research_packets.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from template_parser import DEFAULT_WORKBOOK, load_postgres_payloads, parse_template


DEFAULT_CAPTURE_FIELDS = [
    "brand",
    "product_title",
    "price",
    "wattage",
    "lumens",
    "cct",
    "cri",
    "dimmable",
    "features",
    "certifications",
    "rating",
    "review_count",
    "url",
    "source_channel",
]

DEFAULT_DIRECT_COMPETITORS = [
    "Duralec",
    "Amico",
    "NuWatt",
    "Maxxima",
    "1000Bulbs",
    "NSL USA",
]

CHANNEL_DOMAINS = {
    "amazon": "amazon.com",
    "home_depot": "homedepot.com",
    "walmart": "walmart.com",
    "lowes": "lowes.com",
}

CHANNEL_ORDER = [
    "amazon",
    "home_depot",
    "walmart",
    "lowes",
    "brand_sites",
]

CHANNEL_LABELS = {
    "amazon": "Amazon",
    "home_depot": "Home Depot",
    "walmart": "Walmart",
    "lowes": "Lowe's",
    "brand_sites": "Brand Sites",
}


def normalize_text(value: Any) -> str | None:
    """Normalize optional values into trimmed strings."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "Yes" if value else "No"
    text = str(value).strip()
    return text or None


def compact_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Drop null / empty values from a dict."""
    return {
        key: value
        for key, value in data.items()
        if value not in (None, "", [], {})
    }


def unique_preserve_order(values: list[str]) -> list[str]:
    """Return unique values while preserving input order."""
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
    """Parse a numeric-ish value from text, currency, or percent."""
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


def parse_currency(value: Any) -> float | None:
    """Parse a currency-like value."""
    return parse_number(value)


def parse_percent(value: Any) -> float | None:
    """Parse a percent-like value into a 0-100 scale."""
    number = parse_number(value)
    if number is None:
        return None
    if number <= 1:
        return number * 100
    return number


def format_currency(value: Any) -> str | None:
    """Format a currency value for display."""
    number = parse_currency(value)
    if number is None:
        return None
    return f"${number:,.2f}"


def format_percent(value: Any, decimals: int = 1) -> str | None:
    """Format a percent value for display."""
    number = parse_percent(value)
    if number is None:
        return None
    return f"{number:.{decimals}f}%"


def pct_delta(current: float | None, baseline: float | None) -> float | None:
    """Return percent delta vs a baseline."""
    if current is None or baseline in (None, 0):
        return None
    return ((current - baseline) / baseline) * 100


def normalize_channel_name(value: str) -> str | None:
    """Map workbook channel labels into packet channel keys."""
    lowered = value.strip().lower()
    mapping = {
        "amazon": "amazon",
        "home depot": "home_depot",
        "homedepot": "home_depot",
        "hd": "home_depot",
        "walmart": "walmart",
        "lowe's": "lowes",
        "lowes": "lowes",
        "direct": "brand_sites",
        "brand sites": "brand_sites",
    }
    return mapping.get(lowered)


def build_priority_channels(ideation: dict[str, Any]) -> list[str]:
    """Build the ordered channel sequence for research execution."""
    requested = ideation["research_guidance"].get("priority_channels_list") or []
    ordered = []
    for item in requested:
        normalized = normalize_channel_name(item)
        if normalized:
            ordered.append(normalized)
    ordered = unique_preserve_order(ordered)
    return ordered + [channel for channel in CHANNEL_ORDER if channel not in ordered]


def bool_feature(label: str, value: Any) -> str | None:
    """Convert truthy feature flags into descriptive labels."""
    if value is True:
        return label
    return None


def format_wattage(value: Any) -> str | None:
    """Format wattage terms for search."""
    text = normalize_text(value)
    if not text:
        return None
    if text.lower().endswith("w"):
        return text
    return f"{text}W"


def format_lumens(value: Any) -> str | None:
    """Format lumen terms for search."""
    text = normalize_text(value)
    if not text:
        return None
    lowered = text.lower()
    if "lumen" in lowered or "lm" in lowered:
        return text
    return f"{text} lumens"


def format_cct(value: Any) -> str | None:
    """Format CCT terms for search."""
    text = normalize_text(value)
    if not text:
        return None
    if text.lower().endswith("k"):
        return text
    return f"{text}K"


def format_cri(value: Any) -> str | None:
    """Format CRI terms for search."""
    text = normalize_text(value)
    if not text:
        return None
    if "cri" in text.lower():
        return text
    return f"{text} CRI"


def truncate_words(text: str | None, max_words: int = 8) -> str | None:
    """Trim long ideation labels into compact search phrases."""
    if not text:
        return None
    words = text.split()
    return " ".join(words[:max_words])


def build_feature_watchlist(ideation: dict[str, Any]) -> list[str]:
    """Build a feature list that must be validated in competitive research."""
    electrical = ideation["electrical_specs"]
    physical = ideation["physical_mechanical"]
    features = ideation["features_requirements"]
    business = ideation["business_targets"]

    return unique_preserve_order(
        [
            bool_feature("0-10V dimmable", electrical.get("dimmable"))
            if normalize_text(electrical.get("dimming_type")) == "0-10V"
            else bool_feature("dimmable", electrical.get("dimmable")),
            bool_feature("emergency battery backup", features.get("emergency_battery")),
            bool_feature("motion sensor", features.get("motion_sensor")),
            bool_feature(
                "daylight sensor / auto-dimming",
                features.get("daylight_sensor_auto_dimming"),
            ),
            bool_feature("smart connected", features.get("smart_connected")),
            bool_feature("linkable", features.get("linkable")),
            normalize_text(physical.get("moisture_rating")),
            normalize_text(physical.get("ip_rating")),
            normalize_text(features.get("additional_features")),
            ", ".join(business.get("certifications_list") or []),
        ]
    )


def build_query_terms(ideation: dict[str, Any]) -> dict[str, list[str]]:
    """Build strict, relaxed, and named term groups for search planning."""
    identity = ideation["identity"]
    electrical = ideation["electrical_specs"]
    physical = ideation["physical_mechanical"]

    feature_terms = build_feature_watchlist(ideation)
    strict_terms = unique_preserve_order(
        [
            normalize_text(identity.get("subcategory")),
            normalize_text(physical.get("size_form_factor")),
            normalize_text(physical.get("mounting_type")),
            format_wattage(electrical.get("wattage_primary")),
            format_lumens(electrical.get("lumens_target")),
            format_cct(electrical.get("cct_primary")),
            format_cri(electrical.get("cri")),
            *feature_terms[:3],
        ]
    )
    relaxed_terms = unique_preserve_order(
        [
            normalize_text(identity.get("subcategory")),
            normalize_text(physical.get("size_form_factor")),
            format_wattage(electrical.get("wattage_primary")),
            format_cct(electrical.get("cct_primary")),
            *feature_terms[:2],
        ]
    )
    named_terms = unique_preserve_order(
        [
            truncate_words(normalize_text(identity.get("ideation_name"))),
            normalize_text(identity.get("subcategory")),
            normalize_text(physical.get("size_form_factor")),
            format_wattage(electrical.get("wattage_primary")),
        ]
    )
    return {
        "strict": strict_terms,
        "relaxed": relaxed_terms,
        "named": named_terms,
    }


def build_query_variants(ideation: dict[str, Any]) -> dict[str, list[str]]:
    """Build channel-specific search query variants."""
    term_groups = build_query_terms(ideation)
    queries = {}
    for channel, domain in CHANNEL_DOMAINS.items():
        entries = [
            f"site:{domain} " + " ".join(term_groups["strict"]),
            f"site:{domain} " + " ".join(term_groups["relaxed"]),
        ]
        named_query = " ".join(term_groups["named"])
        if named_query:
            entries.append(f"site:{domain} {named_query}")
        queries[channel] = unique_preserve_order(
            [entry.strip() for entry in entries if entry.strip()]
        )
    return queries


def build_brand_watchlist(ideation: dict[str, Any]) -> list[dict[str, Any]]:
    """Merge workbook competitor hints with Stackline brand leaders."""
    research = ideation["research_guidance"]
    stackline = ideation.get("stackline_context") or {}
    performance = stackline.get("performance_estimation_context") or {}

    watchlist: list[dict[str, Any]] = []
    seen = set()

    def add_brand(brand: str | None, source: str, priority: str) -> None:
        if not brand:
            return
        marker = brand.strip().lower()
        if not marker or marker == "sunco lighting" or marker in seen:
            return
        seen.add(marker)
        watchlist.append(
            {
                "brand": brand.strip(),
                "source": source,
                "priority": priority,
            }
        )

    for brand in research.get("known_competitors_list") or []:
        add_brand(brand, "workbook_known_competitor", "high")

    for brand in performance.get("estimation_inputs", {}).get("top_brands", []):
        add_brand(brand.get("brand"), "stackline_top_brand", "high")

    for brand in DEFAULT_DIRECT_COMPETITORS:
        add_brand(brand, "default_watchlist", "medium")

    return watchlist[:10]


def build_brand_queries(
    ideation: dict[str, Any],
    brand_watchlist: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build direct-brand research prompts."""
    term_groups = build_query_terms(ideation)
    strict = " ".join(term_groups["strict"][:5])
    relaxed = " ".join(term_groups["relaxed"][:4])

    entries = []
    for target in brand_watchlist:
        brand = target["brand"]
        entries.append(
            {
                **target,
                "queries": unique_preserve_order(
                    [
                        f"{brand} {strict}".strip(),
                        f"{brand} {relaxed}".strip(),
                    ]
                ),
            }
        )
    return entries


def build_stackline_amazon_seeds(ideation: dict[str, Any]) -> list[dict[str, Any]]:
    """Return Stackline-backed Amazon competitor seeds, if available."""
    stackline = ideation.get("stackline_context") or {}
    performance = stackline.get("performance_estimation_context") or {}
    seeds = []
    for product in performance.get("estimation_inputs", {}).get(
        "top_competitor_products", []
    ):
        seeds.append(
            compact_dict(
                {
                    "brand": product.get("brand"),
                    "model_number": product.get("model_number"),
                    "title": product.get("title"),
                    "avg_retail_price": product.get("avg_retail_price"),
                    "units_sold": product.get("units_sold"),
                    "sales_share_pct": product.get("sales_share_pct"),
                    "source": "stackline_summary_amazon",
                }
            )
        )
    return seeds


def build_reference_baseline(ideation: dict[str, Any]) -> dict[str, Any] | None:
    """Trim the reference context down to the fields Step 4 actually needs."""
    reference = ideation.get("reference_context")
    if not reference:
        return None

    return compact_dict(
        {
            "sku": reference.get("sku"),
            "title": reference.get("title"),
            "product_type": reference.get("product_type"),
            "image_url": reference.get("image_url"),
            "listing_price": reference.get("listing_price"),
            "shopify_revenue_12mo": reference.get("shopify_revenue_12mo"),
            "shopify_units_12mo": reference.get("shopify_units_12mo"),
            "amazon_revenue_12mo": reference.get("amazon_revenue_12mo"),
            "amazon_units_12mo": reference.get("amazon_units_12mo"),
        }
    )


def build_target_profile(ideation: dict[str, Any]) -> dict[str, Any]:
    """Preserve the ideation target profile needed for Step 4 research."""
    electrical = ideation["electrical_specs"]
    physical = ideation["physical_mechanical"]
    features = ideation["features_requirements"]
    business = ideation["business_targets"]
    research = ideation["research_guidance"]

    return compact_dict(
        {
            "electrical": compact_dict(
                {
                    "voltage": normalize_text(electrical.get("voltage")),
                    "wattage_primary": normalize_text(
                        electrical.get("wattage_primary")
                    ),
                    "wattage_max": normalize_text(electrical.get("wattage_max")),
                    "selectable_wattage": electrical.get("selectable_wattage"),
                    "cct_primary": normalize_text(electrical.get("cct_primary")),
                    "cct_max": normalize_text(electrical.get("cct_max")),
                    "selectable_cct": electrical.get("selectable_cct"),
                    "cri": normalize_text(electrical.get("cri")),
                    "lumens_target": normalize_text(electrical.get("lumens_target")),
                    "dimmable": electrical.get("dimmable"),
                    "dimming_type": normalize_text(electrical.get("dimming_type")),
                }
            ),
            "physical": compact_dict(
                {
                    "size_form_factor": normalize_text(
                        physical.get("size_form_factor")
                    ),
                    "mounting_type": normalize_text(physical.get("mounting_type")),
                    "material": normalize_text(physical.get("material")),
                    "finish_color": normalize_text(physical.get("finish_color")),
                    "ip_rating": normalize_text(physical.get("ip_rating")),
                    "moisture_rating": normalize_text(physical.get("moisture_rating")),
                    "weight_lbs": normalize_text(physical.get("weight_lbs")),
                }
            ),
            "feature_watchlist": build_feature_watchlist(ideation),
            "business_case": compact_dict(
                {
                    "target_msrp": parse_currency(business.get("target_msrp")),
                    "target_vendor_cost": parse_currency(
                        business.get("target_vendor_cost")
                    ),
                    "target_margin_pct_shopify": parse_percent(
                        business.get("target_margin_pct_shopify")
                    ),
                    "target_margin_pct_amazon": parse_percent(
                        business.get("target_margin_pct_amazon")
                    ),
                    "cost_type": normalize_text(business.get("cost_type")),
                    "certifications": business.get("certifications_list"),
                    "lifetime_hours": parse_number(business.get("lifetime_hours")),
                    "warranty": normalize_text(business.get("warranty")),
                }
            ),
            "research_notes": normalize_text(research.get("research_notes")),
            "known_competitors": research.get("known_competitors_list"),
        }
    )


def build_target_price_band(
    target_msrp: float | None,
    reference_price: float | None,
    segment_avg_price: float | None,
) -> dict[str, Any] | None:
    """Create the price band to use during competitor collection."""
    anchors = [
        ("target_msrp", target_msrp),
        ("reference_listing_price", reference_price),
        ("stackline_segment_avg_price", segment_avg_price),
    ]
    anchor_source = None
    anchor_value = None
    for source, value in anchors:
        if value is not None:
            anchor_source = source
            anchor_value = value
            break
    if anchor_value is None:
        return None

    return {
        "anchor_source": anchor_source,
        "anchor_price": round(anchor_value, 2),
        "search_floor": round(anchor_value * 0.75, 2),
        "search_ceiling": round(anchor_value * 1.25, 2),
    }


def build_pricing_hypothesis(ideation: dict[str, Any]) -> dict[str, Any]:
    """Summarize the pricing posture that Step 4 needs to validate."""
    business = ideation["business_targets"]
    reference = ideation.get("reference_context") or {}
    performance = (
        (ideation.get("stackline_context") or {})
        .get("performance_estimation_context", {})
    )
    market_snapshot = performance.get("segment", {}).get("market_snapshot", {})
    estimation_inputs = performance.get("estimation_inputs", {})
    target_msrp = parse_currency(business.get("target_msrp"))
    target_vendor_cost = parse_currency(business.get("target_vendor_cost"))
    reference_price = parse_currency(reference.get("listing_price"))
    segment_avg_price = parse_currency(market_snapshot.get("avg_retail_price"))
    current_sunco_anchor = parse_currency(
        (estimation_inputs.get("price_anchor") or {}).get("avg_retail_price")
    )

    target_vs_segment = pct_delta(target_msrp, segment_avg_price)
    target_vs_reference = pct_delta(target_msrp, reference_price)

    posture = None
    comparison = (
        target_vs_segment if target_vs_segment is not None else target_vs_reference
    )
    if comparison is not None:
        if comparison <= -10:
            posture = "value"
        elif comparison >= 10:
            posture = "premium"
        else:
            posture = "parity"

    gross_margin = None
    if target_msrp not in (None, 0) and target_vendor_cost is not None:
        gross_margin = ((target_msrp - target_vendor_cost) / target_msrp) * 100

    return compact_dict(
        {
            "posture": posture,
            "target_msrp": target_msrp,
            "target_vendor_cost": target_vendor_cost,
            "reference_listing_price": reference_price,
            "segment_avg_price": segment_avg_price,
            "current_sunco_price_anchor": current_sunco_anchor,
            "target_vs_segment_avg_pct": target_vs_segment,
            "target_vs_reference_pct": target_vs_reference,
            "implied_gross_margin_pct": gross_margin,
            "price_band_for_collection": build_target_price_band(
                target_msrp=target_msrp,
                reference_price=reference_price,
                segment_avg_price=segment_avg_price,
            ),
        }
    )


def build_demand_hypothesis(ideation: dict[str, Any]) -> dict[str, Any]:
    """Summarize the demand picture that Step 4 should validate."""
    performance = (
        (ideation.get("stackline_context") or {})
        .get("performance_estimation_context", {})
    )
    market_snapshot = performance.get("segment", {}).get("market_snapshot", {})
    market_momentum = performance.get("segment", {}).get("market_momentum_pct", {})
    sunco_position = performance.get("sunco_position", {})
    reference_family = performance.get("reference_family", {})
    segment_leader = (
        performance.get("estimation_inputs", {}).get("segment_leader", {})
    )
    signals = performance.get("opportunity_signals", [])

    if not market_snapshot:
        return {
            "posture": "no_stackline_context",
            "signals": ["collect_market_proxies_from_competitor_set"],
        }

    sunco_share = parse_percent(sunco_position.get("sales_share_pct"))
    sales_growth = parse_percent(market_momentum.get("retail_sales"))

    posture = "steady_segment"
    if sunco_share is not None and sunco_share < 5:
        posture = "share_expansion_opportunity"
    elif sunco_share is not None and sunco_share >= 20:
        posture = "defend_existing_share"
    elif sales_growth is not None and sales_growth >= 10:
        posture = "growth_segment"

    return compact_dict(
        {
            "posture": posture,
            "segment_retail_sales": market_snapshot.get("retail_sales"),
            "segment_units_sold": market_snapshot.get("units_sold"),
            "segment_conversion_rate_pct": market_snapshot.get(
                "conversion_rate_pct"
            ),
            "segment_sales_growth_pct": market_momentum.get("retail_sales"),
            "segment_units_growth_pct": market_momentum.get("units_sold"),
            "sunco_sales_share_pct": sunco_position.get("sales_share_pct"),
            "sunco_units_share_pct": sunco_position.get("units_share_pct"),
            "segment_leader_brand": segment_leader.get("brand"),
            "share_gap_to_leader_pct_points": segment_leader.get(
                "share_gap_vs_sunco_pct_points"
            ),
            "reference_family_present_in_stackline": reference_family.get("found"),
            "signals": signals,
        }
    )


def build_evidence_to_collect(ideation: dict[str, Any]) -> list[str]:
    """List the concrete evidence Step 4 should collect for this ideation."""
    profile = build_target_profile(ideation)
    pricing = build_pricing_hypothesis(ideation)
    demand = build_demand_hypothesis(ideation)
    feature_watchlist = profile.get("feature_watchlist") or []
    certifications = (profile.get("business_case") or {}).get("certifications") or []
    evidence = [
        "Find 5-10 close Amazon comparables that match the same size/form factor and wattage class.",
        "Find 3-6 Home Depot, Walmart, and Lowe's comparables to anchor non-Amazon price positioning.",
    ]

    price_band = pricing.get("price_band_for_collection") or {}
    if price_band:
        evidence.append(
            "Bias collection toward products priced between "
            f"{format_currency(price_band.get('search_floor'))} and "
            f"{format_currency(price_band.get('search_ceiling'))}."
        )

    if feature_watchlist:
        evidence.append(
            "Validate whether these features are standard, premium, or rare: "
            + ", ".join(feature_watchlist[:5])
            + "."
        )

    if certifications:
        evidence.append(
            "Capture certification coverage for close competitors: "
            + ", ".join(certifications)
            + "."
        )

    if pricing.get("target_vendor_cost") is not None:
        evidence.append(
            "Collect enough pricing and spec detail to backsolve whether the target cost and MSRP can coexist."
        )

    if "reference_family_absent_from_stackline_segment" in (
        demand.get("signals") or []
    ):
        evidence.append(
            "Because the exact reference family is absent from Stackline, identify the closest substitute families instead of forcing a direct match."
        )

    if "sunco_share_below_5pct" in (demand.get("signals") or []):
        evidence.append(
            "Identify which brands currently own the segment and what spec or price advantages they use."
        )

    return evidence


def build_research_priority(ideation: dict[str, Any]) -> str:
    """Assign a research priority for execution ordering."""
    performance = (
        (ideation.get("stackline_context") or {})
        .get("performance_estimation_context", {})
    )
    signals = performance.get("opportunity_signals", [])

    if any(
        signal in signals
        for signal in [
            "segment_sales_growth_above_10pct",
            "sunco_share_below_5pct",
            "meaningful_share_gap_to_segment_leader",
        ]
    ):
        return "high"
    if ideation.get("stackline_context"):
        return "medium"
    return "normal"


def build_collection_targets(ideation: dict[str, Any]) -> dict[str, Any]:
    """Define the minimum collection counts for the research pass."""
    priority_channels = build_priority_channels(ideation)
    amazon_primary = priority_channels[0] == "amazon"

    return {
        "amazon_min_results": 8 if amazon_primary else 6,
        "brick_and_mortar_min_results": 6,
        "brand_site_min_results": 4,
    }


def build_research_prompt(
    ideation: dict[str, Any],
    brand_watchlist: list[dict[str, Any]],
) -> str:
    """Create a concise prompt for a future Claude/web research pass."""
    identity = ideation["identity"]
    notes = normalize_text(ideation["research_guidance"].get("research_notes"))
    pricing = build_pricing_hypothesis(ideation)
    demand = build_demand_hypothesis(ideation)
    evidence = build_evidence_to_collect(ideation)
    terms = ", ".join(build_query_terms(ideation)["strict"][:7])
    brands = ", ".join([entry["brand"] for entry in brand_watchlist[:6]])

    lines = [
        f"Research competitors for ideation '{identity.get('ideation_name')}' in subcategory '{identity.get('subcategory')}'.",
        f"Primary search attributes: {terms}." if terms else None,
        (
            "Pricing posture to validate: "
            f"target MSRP {format_currency(pricing.get('target_msrp'))}, "
            f"segment average {format_currency(pricing.get('segment_avg_price'))}, "
            f"reference listing {format_currency(pricing.get('reference_listing_price'))}."
        )
        if pricing
        else None,
        (
            "Demand context: "
            f"{format_currency(demand.get('segment_retail_sales'))} segment sales, "
            f"{format_percent(demand.get('segment_sales_growth_pct'))} sales growth, "
            f"Sunco share {format_percent(demand.get('sunco_sales_share_pct'))}."
        )
        if demand.get("segment_retail_sales") is not None
        else None,
        f"Brand watchlist: {brands}." if brands else None,
        f"PM notes: {notes}." if notes else None,
        "Evidence to collect: " + " ".join(evidence[:4]) if evidence else None,
    ]
    return " ".join([line for line in lines if line])


def build_channel_plan(ideation: dict[str, Any]) -> dict[str, Any]:
    """Build the Step 4 collection plan for one ideation."""
    priority_channels = build_priority_channels(ideation)
    brand_watchlist = build_brand_watchlist(ideation)
    brand_queries = build_brand_queries(ideation, brand_watchlist)
    queries = build_query_variants(ideation)
    pricing = build_pricing_hypothesis(ideation)
    performance = (
        (ideation.get("stackline_context") or {})
        .get("performance_estimation_context", {})
    )
    feature_watchlist = build_feature_watchlist(ideation)

    return {
        "priority": build_research_priority(ideation),
        "channel_sequence": priority_channels,
        "collection_targets": build_collection_targets(ideation),
        "target_price_band": pricing.get("price_band_for_collection"),
        "must_validate": compact_dict(
            {
                "features": feature_watchlist,
                "certifications": (
                    build_target_profile(ideation)
                    .get("business_case", {})
                    .get("certifications")
                ),
            }
        ),
        "capture_fields": DEFAULT_CAPTURE_FIELDS,
        "amazon": {
            "primary_channel": "amazon" in priority_channels[:1],
            "queries": queries["amazon"],
            "competitor_seeds": build_stackline_amazon_seeds(ideation),
            "segment_context": performance.get("segment", {}),
        },
        "brick_and_mortar": {
            "home_depot": {
                "primary_channel": "home_depot" in priority_channels[:1],
                "queries": queries["home_depot"],
            },
            "walmart": {
                "primary_channel": "walmart" in priority_channels[:1],
                "queries": queries["walmart"],
            },
            "lowes": {
                "primary_channel": "lowes" in priority_channels[:1],
                "queries": queries["lowes"],
            },
        },
        "brand_watchlist": brand_watchlist,
        "known_competitor_brands": brand_queries,
        "research_prompt": build_research_prompt(ideation, brand_watchlist),
    }


def build_packet_status(ideation: dict[str, Any]) -> str:
    """Return a coarse readiness state for research execution."""
    issues = ideation.get("issues", [])
    blocking = [issue for issue in issues if issue.startswith("Missing required fields")]
    if blocking:
        return "blocked"
    if any("Reference SKU" in issue and "not found" in issue for issue in issues):
        return "ready_with_reference_warning"
    return "ready"


def build_packet(ideation: dict[str, Any]) -> dict[str, Any]:
    """Build a single research packet for one ideation row."""
    stackline = ideation.get("stackline_context") or {}
    performance = stackline.get("performance_estimation_context") or {}

    return {
        "row_number": ideation["row_number"],
        "status": build_packet_status(ideation),
        "identity": compact_dict(ideation["identity"]),
        "target_profile": build_target_profile(ideation),
        "reference_baseline": build_reference_baseline(ideation),
        "market_context": compact_dict(
            {
                "segment_name": stackline.get("segment_name"),
                "matched_bundle": stackline.get("matched_bundle"),
                "performance_estimation_context": performance,
            }
        ),
        "estimation_focus": {
            "pricing_hypothesis": build_pricing_hypothesis(ideation),
            "demand_hypothesis": build_demand_hypothesis(ideation),
            "evidence_to_collect": build_evidence_to_collect(ideation),
        },
        "research_plan": build_channel_plan(ideation),
        "issues": ideation.get("issues", []),
    }


def build_research_packets(
    workbook_path: str,
    postgres_payloads: dict[str, dict[str, Any]] | None = None,
    include_queries: bool = False,
    include_stackline_raw: bool = False,
    start_date: str | None = None,
    end_date: str | None = None,
    stackline_folder: str | None = None,
    stackline_brand: str = "Sunco Lighting",
    sheet_name: str = "Ideations",
) -> dict[str, Any]:
    """Build Step 4 research packets from a filled workbook."""
    parsed = parse_template(
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

    packets = [build_packet(ideation) for ideation in parsed["ideations"]]
    return {
        "workbook_path": parsed["workbook_path"],
        "sheet_name": parsed["sheet_name"],
        "packet_count": len(packets),
        "packets": packets,
        "warnings": parsed.get("warnings", []),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build Step 4 competitive research packets from the filled workbook."
    )
    parser.add_argument(
        "workbook",
        nargs="?",
        default=str(DEFAULT_WORKBOOK),
        help="Path to the filled PRD research workbook.",
    )
    parser.add_argument(
        "--sheet",
        default="Ideations",
        help="Worksheet name to parse.",
    )
    parser.add_argument(
        "--postgres-json",
        default=None,
        help="Optional JSON file with per-SKU Postgres enrichment payloads.",
    )
    parser.add_argument(
        "--include-queries",
        action="store_true",
        help="Include MCP query templates for each parsed Reference SKU.",
    )
    parser.add_argument(
        "--include-stackline-raw",
        action="store_true",
        help="Include the full raw Stackline analysis under each ideation packet.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Override MCP sales query start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Override MCP sales query end date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--stackline-folder",
        default=None,
        help="Override the Stackline export folder.",
    )
    parser.add_argument(
        "--stackline-brand",
        default="Sunco Lighting",
        help="Brand name to treat as the internal brand focus in Stackline analysis.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional path to write the packet JSON output.",
    )
    args = parser.parse_args()

    postgres_payloads = load_postgres_payloads(args.postgres_json)
    result = build_research_packets(
        workbook_path=args.workbook,
        postgres_payloads=postgres_payloads,
        include_queries=args.include_queries,
        include_stackline_raw=args.include_stackline_raw,
        start_date=args.start_date,
        end_date=args.end_date,
        stackline_folder=args.stackline_folder,
        stackline_brand=args.stackline_brand,
        sheet_name=args.sheet,
    )

    output = json.dumps(result, indent=2)
    if args.output:
        Path(args.output).write_text(output + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
