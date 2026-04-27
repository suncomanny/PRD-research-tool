"""Gate-readiness rubric and first-pass scoring helpers."""

from __future__ import annotations

import json
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any


RUBRIC_PATH = Path(__file__).resolve().parents[1] / "config" / "gate_readiness_rubric.json"
GATE_ORDER = {"G1": 1, "G2": 2, "G3": 3, "G4": 4, "G5": 5}
CHANNELS = ["amazon", "sunco_com"]
CHANNEL_TO_SOURCE_ID = {"amazon": 11929, "sunco_com": 12585}


def load_rubric() -> dict[str, Any]:
    with RUBRIC_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


@lru_cache(maxsize=1)
def cached_rubric() -> dict[str, Any]:
    return load_rubric()


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_month_value(value: Any) -> datetime | None:
    """Parse a month-like value into a comparable datetime."""
    text = normalize_text(value)
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def parse_number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("$", "").replace("%", "").replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def round_metric(value: float | None) -> float | int | None:
    if value is None:
        return None
    if float(value).is_integer():
        return int(value)
    return round(value + 1e-9, 2)


def clamp_score(value: float) -> float:
    return max(0.0, min(10.0, value))


def family_state(packet: dict[str, Any]) -> str:
    identity = as_dict(packet.get("identity"))
    reference = as_dict(packet.get("reference_baseline"))
    reference_sku = normalize_text(reference.get("sku") or identity.get("sunco_reference_sku"))
    reference_source = (normalize_text(identity.get("reference_sku_source")) or "").lower()

    if "new" in reference_source and "existing" not in reference_source:
        return "new"
    if "existing catalog" in reference_source or reference_sku:
        return "known"

    title_present = bool(normalize_text(reference.get("title")))
    listing_present = parse_number(reference.get("listing_price")) is not None
    shopify_present = parse_number(reference.get("shopify_revenue_12mo")) is not None
    amazon_present = parse_number(reference.get("amazon_revenue_12mo")) is not None
    if title_present or listing_present or shopify_present or amazon_present:
        return "known"
    return "new"


def gate_is_active(rule: dict[str, Any], gate_id: str) -> bool:
    earliest = rule.get("earliest_gate")
    latest = rule.get("latest_gate", "G5")
    if earliest is None:
        return False
    return GATE_ORDER[gate_id] >= GATE_ORDER[earliest] and GATE_ORDER[gate_id] <= GATE_ORDER[latest]


def resolve_channel_rule(
    question: dict[str, Any],
    channel: str,
    family_state_value: str,
) -> dict[str, Any]:
    channel_rule = as_dict(question.get("channel_rules", {}).get(channel))
    if not channel_rule:
        return {}

    family_gate_rules = as_dict(channel_rule.get("family_gate_rules"))
    if family_gate_rules:
        family_rule = as_dict(family_gate_rules.get(family_state_value))
        if not family_rule:
            return {}
        merged = dict(channel_rule)
        merged.update(family_rule)
        merged.pop("family_gate_rules", None)
        return merged
    return channel_rule


def lookup_channel_context(packet: dict[str, Any], channel: str) -> dict[str, Any]:
    market_context = as_dict(packet.get("market_context"))
    channel_contexts = as_dict(market_context.get("channel_performance_estimation_contexts"))
    return as_dict(channel_contexts.get(channel))


def reference_channel_metrics(packet: dict[str, Any], channel: str) -> dict[str, Any]:
    """Return channel-specific reference metrics block."""
    reference = as_dict(packet.get("reference_baseline"))
    concentration = as_dict(reference.get("customer_concentration"))
    if channel == "amazon":
        return as_dict(concentration.get("amazon"))
    return as_dict(concentration.get("shopify"))


def reference_monthly_sales_rows(packet: dict[str, Any], channel: str) -> list[dict[str, Any]]:
    """Return sorted monthly sales rows for a channel from the reference baseline."""
    reference = as_dict(packet.get("reference_baseline"))
    wanted_id = CHANNEL_TO_SOURCE_ID[channel]
    rows = []
    for item in as_list(reference.get("monthly_sales")):
        if not isinstance(item, dict):
            continue
        channel_id = parse_number(item.get("sales_channel_id"))
        if channel_id != wanted_id:
            continue
        month_value = parse_month_value(item.get("month"))
        if month_value is None:
            continue
        rows.append(
            {
                "month": month_value,
                "revenue": parse_number(item.get("revenue")) or 0.0,
                "units": parse_number(item.get("units")) or 0.0,
                "distinct_customers": parse_number(item.get("distinct_customers")),
            }
        )
    rows.sort(key=lambda entry: entry["month"])
    return rows


def score_by_thresholds(value: float | None, thresholds: list[tuple[float, float]]) -> float | None:
    if value is None:
        return None
    for minimum, score in thresholds:
        if value >= minimum:
            return float(score)
    return 1.0


def score_reference_channel_revenue(packet: dict[str, Any], channel: str) -> dict[str, Any]:
    reference = as_dict(packet.get("reference_baseline"))
    source_key = "amazon_data_source" if channel == "amazon" else "shopify_data_source"
    revenue_key = "amazon_revenue_12mo" if channel == "amazon" else "shopify_revenue_12mo"
    revenue = parse_number(reference.get(revenue_key))
    if revenue is None:
        return {"status": "inactive_missing_source", "reason": "No channel revenue baseline is available."}

    score = score_by_thresholds(
        revenue,
        [
            (500000, 10),
            (300000, 9),
            (100000, 8),
            (50000, 6),
            (10000, 4),
            (5000, 3),
        ],
    )
    source = normalize_text(reference.get(source_key))
    evidence_type = "direct" if source == "postgres_mcp" else "proxy"
    return {
        "status": "scored",
        "score": score,
        "evidence_type": evidence_type,
        "evidence": f"Reference {channel} revenue baseline = ${revenue:,.2f}.",
    }


def score_demand_consistency(packet: dict[str, Any], channel: str) -> dict[str, Any]:
    rows = reference_monthly_sales_rows(packet, channel)
    if not rows:
        return {"status": "inactive_missing_source", "reason": "No monthly sales history is available for demand consistency."}

    trailing = rows[-12:]
    active_months = sum(1 for row in trailing if (row["revenue"] > 0 or row["units"] > 0))
    if active_months >= 11:
        score = 10
    elif active_months >= 9:
        score = 8
    elif active_months >= 7:
        score = 6
    elif active_months >= 5:
        score = 4
    else:
        score = 2

    return {
        "status": "scored",
        "score": float(score),
        "evidence_type": "direct",
        "evidence": f"{active_months} active months with orders in the trailing {len(trailing)} months.",
    }


def score_bulk_buyer_analysis(packet: dict[str, Any], channel: str) -> dict[str, Any]:
    metrics = reference_channel_metrics(packet, channel)
    total_customers = parse_number(metrics.get("total_customers"))
    top_share = parse_number(metrics.get("top_20pct_revenue_share_pct"))
    repeat_rate = parse_number(metrics.get("repeat_rate_pct"))
    if total_customers is None or top_share is None:
        return {"status": "inactive_missing_source", "reason": "No customer concentration metrics are available for bulk buyer analysis."}

    if channel == "amazon":
        if top_share < 40:
            score = 10
        elif top_share < 55:
            score = 8
        elif top_share < 70:
            score = 6
        elif top_share < 85:
            score = 4
        else:
            score = 2
    else:
        if total_customers < 10:
            score = 5
        elif top_share > 70:
            score = 10
        elif top_share >= 55:
            score = 8
        elif top_share >= 40:
            score = 6
        else:
            score = 4

    if repeat_rate is not None and repeat_rate > 15:
        score += 1

    return {
        "status": "scored",
        "score": float(clamp_score(score)),
        "evidence_type": "direct",
        "evidence": f"Top-20pct revenue share is {round_metric(top_share)}% across {round_metric(total_customers)} customers; repeat rate is {round_metric(repeat_rate)}%.",
    }


def score_sales_trend(packet: dict[str, Any], channel: str) -> dict[str, Any]:
    rows = reference_monthly_sales_rows(packet, channel)
    if not rows:
        return {"status": "inactive_missing_source", "reason": "No monthly sales history is available for sales trend."}

    by_month = {row["month"].strftime("%Y-%m"): row["revenue"] for row in rows}
    yoy_deltas: list[float] = []
    recent_months = rows[-12:]
    for row in recent_months:
        current_month = row["month"]
        prior_key = current_month.replace(year=current_month.year - 1).strftime("%Y-%m")
        prior_revenue = by_month.get(prior_key)
        if prior_revenue in (None, 0):
            continue
        yoy_deltas.append(((row["revenue"] - prior_revenue) / prior_revenue) * 100)

    if not yoy_deltas:
        return {"status": "inactive_missing_source", "reason": "Monthly history does not contain enough prior-year pairs for sales trend."}

    yoy_avg = sum(yoy_deltas) / len(yoy_deltas)
    if yoy_avg > 20:
        score = 9
    elif yoy_avg > 10:
        score = 7
    elif yoy_avg >= 0:
        score = 5
    elif yoy_avg >= -10:
        score = 3
    else:
        score = 1

    if len(yoy_deltas) < 12:
        score = min(score, 8)

    return {
        "status": "scored",
        "score": float(score),
        "evidence_type": "direct",
        "evidence": f"Average YoY monthly revenue trend is {round_metric(yoy_avg)}% across {len(yoy_deltas)} month-pairs.",
    }


def score_stackline_brand_count(packet: dict[str, Any], channel: str) -> dict[str, Any]:
    context = lookup_channel_context(packet, channel)
    brand_count = parse_number(as_dict(as_dict(context.get("segment")).get("market_snapshot")).get("brand_count"))
    if brand_count is None:
        return {"status": "inactive_missing_source", "reason": "No segment brand-count snapshot is available for this channel."}

    if brand_count <= 2:
        score = 2
    elif brand_count <= 8:
        score = 9
    elif brand_count <= 15:
        score = 7
    elif brand_count <= 20:
        score = 5
    else:
        score = 4

    return {
        "status": "scored",
        "score": score,
        "evidence_type": "direct",
        "evidence": f"Stackline brand count = {int(brand_count)} for the {channel} segment snapshot.",
    }


def score_target_price_position(pricing_analysis: dict[str, Any]) -> dict[str, Any]:
    position = as_dict(pricing_analysis.get("target_price_position"))
    vs_median_pct = parse_number(position.get("vs_median_pct"))
    percentile = parse_number(position.get("percentile"))
    if vs_median_pct is None and percentile is None:
        return {"status": "inactive_missing_source", "reason": "No target price position is available."}

    if vs_median_pct is None:
        if percentile is None:
            return {"status": "inactive_missing_source", "reason": "No target price position is available."}
        if percentile <= 25:
            score = 9
        elif percentile <= 60:
            score = 8
        elif percentile <= 85:
            score = 5
        else:
            score = 3
    else:
        if vs_median_pct <= -10:
            score = 9
        elif vs_median_pct <= 5:
            score = 8
        elif vs_median_pct <= 15:
            score = 6
        elif vs_median_pct <= 30:
            score = 4
        else:
            score = 2

    evaluated = position.get("evaluated_value")
    return {
        "status": "scored",
        "score": float(score),
        "evidence_type": "proxy",
        "evidence": f"Evaluated unit price {evaluated} sits at {position.get('percentile')}th percentile vs competitors.",
    }


def score_stackline_total_traffic(packet: dict[str, Any], channel: str) -> dict[str, Any]:
    context = lookup_channel_context(packet, channel)
    traffic = parse_number(as_dict(as_dict(context.get("segment")).get("market_snapshot")).get("total_traffic"))
    if traffic is None:
        return {"status": "inactive_missing_source", "reason": "No traffic snapshot is available for this channel."}

    score = score_by_thresholds(
        traffic,
        [
            (750000, 10),
            (500000, 9),
            (250000, 8),
            (100000, 7),
            (50000, 5),
        ],
    )
    return {
        "status": "scored",
        "score": score,
        "evidence_type": "direct",
        "evidence": f"Segment traffic snapshot = {traffic:,.0f}.",
    }


def score_gmc_visibility_proxy(packet: dict[str, Any], channel: str) -> dict[str, Any]:
    context = lookup_channel_context(packet, channel)
    traffic = parse_number(as_dict(as_dict(context.get("segment")).get("market_snapshot")).get("total_traffic"))
    if traffic is None:
        return {"status": "inactive_missing_source", "reason": "No Stackline traffic snapshot is available for the GMC visibility proxy."}

    if traffic >= 750000:
        score = 9
    elif traffic >= 500000:
        score = 8
    elif traffic >= 250000:
        score = 7
    elif traffic >= 100000:
        score = 5
    elif traffic >= 50000:
        score = 3
    else:
        score = 2

    return {
        "status": "scored",
        "score": float(score),
        "evidence_type": "proxy",
        "evidence": f"Stackline segment traffic proxy = {traffic:,.0f}; proxy score capped below 10 by methodology.",
    }


def score_competitor_growth_proxy(packet: dict[str, Any], channel: str) -> dict[str, Any]:
    context = lookup_channel_context(packet, channel)
    segment = as_dict(context.get("segment"))
    snapshot = as_dict(segment.get("market_snapshot"))
    momentum = as_dict(segment.get("market_momentum_pct"))

    brand_count = parse_number(snapshot.get("brand_count"))
    product_count = parse_number(snapshot.get("catalog_product_count"))
    sales_growth = parse_number(momentum.get("retail_sales"))
    units_growth = parse_number(momentum.get("units_sold"))
    traffic_growth = parse_number(momentum.get("traffic"))

    if brand_count is None and product_count is None and sales_growth is None and units_growth is None and traffic_growth is None:
        return {"status": "inactive_missing_source", "reason": "No assortment or momentum snapshot is available to proxy competitor growth pressure."}

    pressure = 0
    if brand_count is not None:
        if brand_count >= 200:
            pressure += 3
        elif brand_count >= 100:
            pressure += 2
        elif brand_count >= 40:
            pressure += 1

    if product_count is not None:
        if product_count >= 1000:
            pressure += 3
        elif product_count >= 500:
            pressure += 2
        elif product_count >= 200:
            pressure += 1

    strongest_growth = max(
        value for value in (sales_growth, units_growth, traffic_growth)
        if value is not None
    ) if any(value is not None for value in (sales_growth, units_growth, traffic_growth)) else None

    if strongest_growth is not None:
        if strongest_growth >= 25:
            pressure += 2
        elif strongest_growth >= 10:
            pressure += 1

    if pressure <= 1:
        score = 8
    elif pressure <= 3:
        score = 6
    elif pressure <= 5:
        score = 4
    else:
        score = 2

    evidence_type = "direct" if channel == "amazon" else "proxy"
    return {
        "status": "scored",
        "score": float(score),
        "evidence_type": evidence_type,
        "evidence": (
            f"Growth-pressure proxy uses brand count {round_metric(brand_count)}, "
            f"catalog products {round_metric(product_count)}, and strongest momentum "
            f"{round_metric(strongest_growth)}%."
        ),
    }


def score_segment_persistence(packet: dict[str, Any]) -> dict[str, Any]:
    market_context = as_dict(as_dict(packet.get("market_context")).get("performance_estimation_context"))
    segment = as_dict(market_context.get("segment"))
    momentum = as_dict(segment.get("market_momentum_pct"))
    sales_growth = parse_number(momentum.get("retail_sales"))
    units_growth = parse_number(momentum.get("units_sold"))
    traffic_growth = parse_number(momentum.get("traffic"))
    if sales_growth is None and units_growth is None and traffic_growth is None:
        return {"status": "inactive_missing_source", "reason": "No momentum metrics are available to proxy persistence."}

    positive_count = sum(
        1
        for value in (sales_growth, units_growth, traffic_growth)
        if value is not None and value > 0
    )
    if positive_count == 3:
        score = 9
    elif positive_count == 2:
        score = 7
    elif positive_count == 1:
        score = 5
    else:
        score = 2

    return {
        "status": "scored",
        "score": float(min(score, 9)),
        "evidence_type": "proxy",
        "evidence": f"Momentum proxy uses sales growth {sales_growth}, units growth {units_growth}, traffic growth {traffic_growth}.",
    }


def score_target_market_fit(spec_coverage: dict[str, Any]) -> dict[str, Any]:
    feature_entries = as_list(spec_coverage.get("feature_coverage"))
    cert_entries = as_list(spec_coverage.get("certification_coverage"))
    numeric_entries = as_list(spec_coverage.get("numeric_guidance"))

    table_stakes = sum(1 for entry in feature_entries if entry.get("signal") == "table_stakes")
    competitive = sum(1 for entry in feature_entries if entry.get("signal") == "competitive")
    certification_support = sum(1 for entry in cert_entries if entry.get("coverage_pct", 0) >= 25)
    below_market = sum(1 for entry in numeric_entries if entry.get("signal") == "below_market")
    mid_pack = sum(1 for entry in numeric_entries if entry.get("signal") == "mid_pack")

    score = 6.0
    score += min(table_stakes, 3) * 0.8
    score += min(competitive, 2) * 0.4
    score += min(certification_support, 2) * 0.3
    score -= below_market * 1.2
    score -= mid_pack * 0.5
    score = clamp_score(score)

    return {
        "status": "scored",
        "score": score,
        "evidence_type": "proxy",
        "evidence": (
            f"Market-fit proxy uses {table_stakes} table-stakes features, "
            f"{competitive} competitive features, {certification_support} supported certifications, "
            f"{below_market} below-market numeric gaps, and {mid_pack} mid-pack numeric gaps."
        ),
    }


def score_target_vendor_cost_proxy(packet: dict[str, Any], pricing_analysis: dict[str, Any]) -> dict[str, Any]:
    business_case = as_dict(as_dict(packet.get("target_profile")).get("business_case"))
    target_vendor_cost = parse_number(
        business_case.get("target_vendor_cost")
        or pricing_analysis.get("target_vendor_cost")
    )
    if target_vendor_cost is None:
        return {"status": "inactive_missing_source", "reason": "No target vendor cost is available for an early investment proxy."}

    score = score_by_thresholds(
        -target_vendor_cost,
        [
            (-10, 10),
            (-20, 8),
            (-30, 6),
            (-50, 4),
        ],
    )
    return {
        "status": "scored",
        "score": score,
        "evidence_type": "proxy",
        "evidence": f"Early investment proxy uses target vendor cost ${target_vendor_cost:,.2f} without MOQ or DOI sizing.",
    }


def score_margin_viability(pricing_analysis: dict[str, Any], channel: str) -> dict[str, Any]:
    suggested = as_dict(pricing_analysis.get("suggested_msrp_range"))
    margins = as_dict(pricing_analysis.get("margin_targets"))
    margin_entry = as_dict(margins.get("amazon" if channel == "amazon" else "shopify"))
    margin_floor = parse_number(margin_entry.get("minimum_viable_msrp"))
    conflict = bool(suggested.get("margin_conflict"))
    if margin_floor is None and not suggested:
        return {"status": "inactive_missing_source", "reason": "No margin viability data is available."}

    if conflict:
        score = 2
    elif margin_floor is not None and parse_number(suggested.get("recommended_ceiling")) is not None and margin_floor <= parse_number(suggested.get("recommended_ceiling")):
        score = 8
    else:
        score = 6

    return {
        "status": "scored",
        "score": float(score),
        "evidence_type": "proxy",
        "evidence": f"Margin floor {margin_floor} with conflict={conflict}.",
    }


STRATEGIES = {
    "reference_channel_revenue": score_reference_channel_revenue,
    "demand_consistency": score_demand_consistency,
    "bulk_buyer_analysis": score_bulk_buyer_analysis,
    "sales_trend": score_sales_trend,
    "stackline_brand_count": score_stackline_brand_count,
    "target_price_position": score_target_price_position,
    "stackline_total_traffic": score_stackline_total_traffic,
    "competitor_growth_proxy": score_competitor_growth_proxy,
    "segment_persistence_proxy": score_segment_persistence,
    "gmc_visibility_proxy": score_gmc_visibility_proxy,
    "target_market_fit_proxy": score_target_market_fit,
    "target_vendor_cost_proxy": score_target_vendor_cost_proxy,
    "margin_viability": score_margin_viability,
}


def evaluate_question(
    question: dict[str, Any],
    gate_id: str,
    channel: str,
    family_state_value: str,
    packet: dict[str, Any],
    pricing_analysis: dict[str, Any],
    spec_coverage: dict[str, Any],
) -> dict[str, Any] | None:
    if family_state_value not in question.get("family_states", []):
        return None

    channel_rule = resolve_channel_rule(question, channel, family_state_value)
    if not channel_rule:
        return None
    if not gate_is_active(channel_rule, gate_id):
        return None

    strategy_name = channel_rule.get("strategy")
    if not strategy_name:
        return {
            "id": question["id"],
            "label": question["label"],
            "pillar": question["pillar"],
            "status": "inactive_missing_source",
            "evidence_type": channel_rule.get("source_type"),
            "reason": f"No implemented scoring strategy yet for {question['label']} on {channel}.",
            "controllability": question.get("controllability"),
            "methodology_source_type": channel_rule.get("source_type"),
        }

    strategy = STRATEGIES[strategy_name]
    if strategy_name in {
        "reference_channel_revenue",
        "demand_consistency",
        "bulk_buyer_analysis",
        "sales_trend",
        "stackline_brand_count",
        "stackline_total_traffic",
        "competitor_growth_proxy",
        "gmc_visibility_proxy",
    }:
        result = strategy(packet, channel)
    elif strategy_name in {"target_price_position"}:
        result = strategy(pricing_analysis)
    elif strategy_name in {"segment_persistence_proxy"}:
        result = strategy(packet)
    elif strategy_name in {"target_market_fit_proxy"}:
        result = strategy(spec_coverage)
    elif strategy_name in {"target_vendor_cost_proxy"}:
        result = strategy(packet, pricing_analysis)
    elif strategy_name in {"margin_viability"}:
        result = strategy(pricing_analysis, channel)
    else:
        result = {"status": "inactive_missing_source", "reason": f"Unknown strategy {strategy_name}."}

    result.update(
        {
            "id": question["id"],
            "label": question["label"],
            "pillar": question["pillar"],
            "controllability": question.get("controllability"),
            "source_type": channel_rule.get("source_type"),
            "methodology_source_type": channel_rule.get("source_type"),
        }
    )
    return result


def rollup_pillars(rubric: dict[str, Any], question_results: list[dict[str, Any]]) -> dict[str, Any]:
    by_pillar: dict[str, list[dict[str, Any]]] = {}
    for result in question_results:
        by_pillar.setdefault(result["pillar"], []).append(result)

    pillar_entries = []
    scored_weights = 0
    for pillar in rubric["pillars"]:
        pillar_id = pillar["id"]
        results = by_pillar.get(pillar_id, [])
        scored = [entry for entry in results if entry.get("status") == "scored" and entry.get("score") is not None]
        if scored:
            avg_score = sum(float(entry["score"]) for entry in scored) / len(scored)
            scored_weights += pillar["weight"]
            pillar_entries.append(
                {
                    "pillar": pillar_id,
                    "label": pillar["label"],
                    "base_weight": pillar["weight"],
                    "status": "scored",
                    "question_count": len(results),
                    "scored_question_count": len(scored),
                    "average_score": round_metric(avg_score),
                }
            )
        else:
            pillar_entries.append(
                {
                    "pillar": pillar_id,
                    "label": pillar["label"],
                    "base_weight": pillar["weight"],
                    "status": "inactive",
                    "question_count": len(results),
                    "scored_question_count": 0,
                    "average_score": None,
                }
            )

    weighted_score = 0.0
    for pillar_entry in pillar_entries:
        if pillar_entry["status"] != "scored" or scored_weights == 0:
            pillar_entry["effective_weight"] = None
            pillar_entry["weighted_contribution"] = None
            continue
        effective_weight = (pillar_entry["base_weight"] / scored_weights) * 100
        weighted_contribution = (pillar_entry["average_score"] * effective_weight) / 100
        pillar_entry["effective_weight"] = round_metric(effective_weight)
        pillar_entry["weighted_contribution"] = round_metric(weighted_contribution)
        weighted_score += weighted_contribution

    return {
        "pillars": pillar_entries,
        "weighted_score": round_metric(weighted_score) if scored_weights else None,
        "pillar_weight_coverage_pct": round_metric((scored_weights / 100) * 100),
    }


def evidence_confidence(question_results: list[dict[str, Any]], pillar_rollup: dict[str, Any]) -> dict[str, Any]:
    methodology_active = len(question_results)
    scored = [entry for entry in question_results if entry.get("status") == "scored"]
    direct = [entry for entry in scored if entry.get("evidence_type") == "direct"]
    proxy = [entry for entry in scored if entry.get("evidence_type") != "direct"]

    implemented_pct = (len(scored) / methodology_active) if methodology_active else 0.0
    direct_pct = (len(direct) / len(scored)) if scored else 0.0
    pillar_coverage_pct = (parse_number(pillar_rollup.get("pillar_weight_coverage_pct")) or 0.0) / 100
    confidence_score = round_metric((implemented_pct * 0.45 + direct_pct * 0.3 + pillar_coverage_pct * 0.25) * 10)

    if confidence_score is None:
        label = "none"
    elif confidence_score >= 7.5:
        label = "high"
    elif confidence_score >= 5:
        label = "medium"
    else:
        label = "low"

    return {
        "score": confidence_score,
        "label": label,
        "methodology_active_questions": methodology_active,
        "implemented_questions": len(scored),
        "direct_questions": len(direct),
        "proxy_questions": len(proxy),
        "implemented_pct": round_metric(implemented_pct * 100),
        "direct_pct": round_metric(direct_pct * 100),
    }


def top_caveats(question_results: list[dict[str, Any]]) -> list[str]:
    caveats = []
    for result in question_results:
        if result.get("status") == "inactive_missing_source" and result.get("reason"):
            caveats.append(result["reason"])
        elif result.get("status") == "scored" and result.get("evidence_type") == "proxy" and result.get("evidence"):
            caveats.append(f"{result['label']} uses proxy evidence. {result['evidence']}")
    deduped = []
    seen = set()
    for caveat in caveats:
        if caveat in seen:
            continue
        seen.add(caveat)
        deduped.append(caveat)
    return deduped[:5]


def build_channel_gate_snapshot(
    rubric: dict[str, Any],
    gate_id: str,
    channel: str,
    family_state_value: str,
    packet: dict[str, Any],
    pricing_analysis: dict[str, Any],
    spec_coverage: dict[str, Any],
) -> dict[str, Any]:
    active_questions = []
    for question in rubric["questions"]:
        result = evaluate_question(
            question=question,
            gate_id=gate_id,
            channel=channel,
            family_state_value=family_state_value,
            packet=packet,
            pricing_analysis=pricing_analysis,
            spec_coverage=spec_coverage,
        )
        if result is not None:
            active_questions.append(result)

    pillar_rollup = rollup_pillars(rubric, active_questions)
    evidence = evidence_confidence(active_questions, pillar_rollup)
    return {
        "gate": gate_id,
        "channel": channel,
        "family_state": family_state_value,
        "weighted_score": pillar_rollup.get("weighted_score"),
        "methodology_active_questions": len(active_questions),
        "evidence_confidence": evidence,
        "pillar_scores": pillar_rollup.get("pillars"),
        "question_scores": active_questions,
        "top_caveats": top_caveats(active_questions),
    }


def pick_primary_channel(packet: dict[str, Any], snapshots: list[dict[str, Any]]) -> str | None:
    preferred = normalize_text(as_dict(packet.get("market_context")).get("primary_channel"))
    available = {snapshot["channel"] for snapshot in snapshots}
    if preferred in available:
        return preferred
    if "amazon" in available:
        return "amazon"
    if available:
        return sorted(available)[0]
    return None


def build_gate_readiness(
    packet: dict[str, Any],
    pricing_analysis: dict[str, Any],
    spec_coverage: dict[str, Any],
    performance_estimation: dict[str, Any],
) -> dict[str, Any]:
    rubric = cached_rubric()
    family_state_value = family_state(packet)

    snapshots = []
    for gate_id in ["G1", "G2"]:
        for channel in CHANNELS:
            snapshots.append(
                build_channel_gate_snapshot(
                    rubric=rubric,
                    gate_id=gate_id,
                    channel=channel,
                    family_state_value=family_state_value,
                    packet=packet,
                    pricing_analysis=pricing_analysis,
                    spec_coverage=spec_coverage,
                )
            )

    primary_channel = pick_primary_channel(packet, snapshots)
    primary_g2 = next(
        (
            snapshot
            for snapshot in snapshots
            if snapshot["channel"] == primary_channel and snapshot["gate"] == "G2"
        ),
        None,
    )

    if primary_g2 and primary_g2.get("weighted_score") is not None:
        summary = (
            f"{primary_channel} G2 readiness is {primary_g2['weighted_score']}/10 "
            f"with {primary_g2['evidence_confidence']['label']} evidence confidence."
        )
    else:
        summary = "Gate readiness is provisional until more methodology questions are implemented."

    return {
        "scoring_model_version": rubric["version"],
        "score_scale": rubric["score_scale"],
        "family_state": family_state_value,
        "primary_channel": primary_channel,
        "methodology_scope": rubric.get("implementation_scope"),
        "summary": summary,
        "snapshots": snapshots,
    }


def build_highest_impact_vendor_requests(
    pricing_analysis: dict[str, Any],
    spec_coverage: dict[str, Any],
) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    suggested = as_dict(pricing_analysis.get("suggested_msrp_range"))
    margin_conflict = bool(suggested.get("margin_conflict"))
    min_margin_safe = parse_number(suggested.get("minimum_margin_safe_price"))
    ceiling = parse_number(suggested.get("recommended_ceiling"))
    if margin_conflict:
        requests.append(
            {
                "priority": "critical",
                "request": "Reduce vendor cost or add enough differentiated value to justify a premium MSRP.",
                "reason": f"Minimum margin-safe MSRP ({min_margin_safe}) sits above the recommended competitive ceiling ({ceiling}).",
                "linked_metric": "margin_viability",
            }
        )

    for entry in as_list(spec_coverage.get("numeric_guidance")):
        signal = normalize_text(entry.get("signal"))
        if signal == "below_market":
            requests.append(
                {
                    "priority": "high",
                    "request": normalize_text(entry.get("recommended_action")),
                    "reason": f"{entry.get('label')} trails the current competitive median.",
                    "linked_metric": entry.get("label"),
                }
            )
        elif signal == "mid_pack":
            requests.append(
                {
                    "priority": "medium",
                    "request": normalize_text(entry.get("recommended_action")),
                    "reason": f"{entry.get('label')} is mid-pack and could be strengthened for a better claim.",
                    "linked_metric": entry.get("label"),
                }
            )

    for entry in as_list(spec_coverage.get("feature_coverage")):
        signal = normalize_text(entry.get("signal"))
        if signal == "table_stakes":
            requests.append(
                {
                    "priority": "high",
                    "request": f"Require the vendor to confirm true support for {entry.get('label')}.",
                    "reason": f"{entry.get('label')} appears in {entry.get('matched_count')} competitor listings and reads as table stakes.",
                    "linked_metric": entry.get("label"),
                }
            )
        elif signal == "competitive":
            requests.append(
                {
                    "priority": "medium",
                    "request": f"Keep {entry.get('label')} in scope if Sunco needs mainstream parity in this category.",
                    "reason": f"{entry.get('label')} shows up often enough to influence channel expectations.",
                    "linked_metric": entry.get("label"),
                }
            )

    priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    deduped = []
    seen = set()
    for entry in sorted(
        requests,
        key=lambda item: (priority_order.get(item["priority"], 9), item["request"] or ""),
    ):
        marker = entry["request"]
        if not marker or marker in seen:
            continue
        seen.add(marker)
        deduped.append(entry)
    return deduped[:5]
