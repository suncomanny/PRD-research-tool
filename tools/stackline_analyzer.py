"""
Analyze Stackline exports for a subcategory or segment.

Primary inputs:
  - *_summary.csv: product-level weekly sales, units, price, brand share
  - *_traffic.csv: segment traffic trend used to derive conversion

Usage:
  python tools/stackline_analyzer.py --subcategory Panels --reference-sku PN24_HO-4060K-1PK
  python tools/stackline_analyzer.py --summary path\\to\\summary.csv --traffic path\\to\\traffic.csv
"""

from __future__ import annotations

import argparse
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from sku_lookup import strip_pack_suffix


STACKLINE_DIR = Path(
    r"C:\Users\Sunco\Sunco Lighting\Product - Manny Tools\PRD Research\Stackline Data"
)
STACKLINE_FILE_RE = re.compile(
    r"^Stackline_(?P<segment>.+)_(?P<period>\d{4}-\d{2})_(?P<kind>summary|traffic|sales)\.csv$",
    re.IGNORECASE,
)
SUMMARY_REQUIRED_COLUMNS = {
    "Retail Sales",
    "Units Sold",
    "Retail Price",
    "Segment Name",
    "TimePeriod",
}
TWO_PERIOD_FIRST_COLUMNS = {"weekending", "week ending", "date"}
RETAILER_ALIASES = {
    "amazon.com": "amazon",
    "amazon": "amazon",
    "homedepot": "home_depot",
    "home depot": "home_depot",
    "walmart": "walmart",
    "lowes": "lowes",
    "lowe s": "lowes",
    "lowe's": "lowes",
}
NOISE_TOKENS = {
    "stackline",
    "summary",
    "sales",
    "traffic",
    "count",
    "all",
    "retailer",
    "retailers",
    "home",
    "depot",
    "homedepot",
    "amazon",
    "walmart",
    "lowes",
    "lowe",
}
DATE_TOKEN_RE = re.compile(r"^\d{4}(?:-\d{2})?$|^\d{6,8}$")
RETAILER_SCOPE_PRIORITY = {
    "all_retailers": 4,
    "amazon": 3,
    "home_depot": 2,
    "walmart": 1,
    "lowes": 1,
}


@dataclass
class StacklineBundle:
    segment_slug: str
    period: str
    summary_path: Path | None = None
    traffic_path: Path | None = None
    sales_path: Path | None = None
    segment_name: str | None = None
    retailer_scope: str | None = None
    discovery_method: str = "filename"
    match_score: float = 0.0


def normalize_text(value: str | None) -> str:
    """Normalize text for loose segment matching."""
    if not value:
        return ""

    text = value.lower()
    text = re.sub(r"[_\-]+", " ", text)
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    parts = []
    for part in text.split():
        if len(part) > 4 and part.endswith("ies"):
            part = part[:-3] + "y"
        elif len(part) > 4 and part.endswith("s"):
            part = part[:-1]
        parts.append(part)
    return " ".join(parts)


def text_tokens(value: str | None) -> set[str]:
    return {token for token in normalize_text(value).split() if token}


def infer_retailer_scope(value: str | None) -> str | None:
    normalized = normalize_text(value)
    if not normalized:
        return None
    for alias, scope in RETAILER_ALIASES.items():
        if alias in normalized:
            return scope
    return None


def retailer_scope_key(value: str | None) -> str:
    """Normalize retailer scope into a stable channel key."""
    return value or "all_retailers"


def infer_period_from_dates(values: pd.Series | list[Any]) -> str | None:
    try:
        parsed = pd.to_datetime(pd.Series(list(values)), errors="coerce")
    except Exception:
        return None
    parsed = parsed.dropna()
    if parsed.empty:
        return None
    latest = parsed.max()
    return f"{latest.year:04d}-{latest.month:02d}"


def infer_period_from_header_labels(labels: list[str]) -> str | None:
    date_values: list[str] = []
    for label in labels:
        if not isinstance(label, str):
            continue
        label = label.strip()
        if " - " in label:
            tail = label.split(" - ")[-1].strip()
            date_values.append(tail)
        else:
            date_values.append(label)
    return infer_period_from_dates(pd.Series(date_values))


def infer_period_from_filename(path: Path) -> str | None:
    tokens = re.split(r"[_\-\s]+", path.stem)
    candidates = [token for token in tokens if DATE_TOKEN_RE.match(token)]
    if not candidates:
        return None

    for candidate in reversed(candidates):
        if re.fullmatch(r"\d{4}-\d{2}", candidate):
            return candidate
        if re.fullmatch(r"\d{6}", candidate):
            return f"{candidate[:4]}-{candidate[4:6]}"
        if re.fullmatch(r"\d{8}", candidate):
            return f"{candidate[:4]}-{candidate[4:6]}"
    return None


def infer_kind_from_filename(path: Path) -> str | None:
    normalized = normalize_text(path.stem)
    if not normalized:
        return None
    if "traffic" in normalized:
        return "traffic"
    if "sales" in normalized or "count" in normalized:
        return "sales"
    if "summary" in normalized:
        return "summary"
    return None


def infer_segment_slug_from_filename(path: Path) -> str | None:
    raw_tokens = re.split(r"[_\-\s]+", path.stem)
    tokens = []
    for token in raw_tokens:
        token = token.strip()
        if not token:
            continue
        normalized = normalize_text(token)
        if not normalized:
            continue
        if normalized in NOISE_TOKENS:
            continue
        if DATE_TOKEN_RE.match(token):
            continue
        tokens.append(token)
    if not tokens:
        return None
    return " ".join(tokens)


def sniff_stackline_file(path: Path) -> dict[str, str] | None:
    try:
        header_frame = pd.read_csv(path, nrows=3)
    except Exception:
        return None

    columns = [str(column).strip() for column in header_frame.columns]
    column_set = set(columns)
    inferred_kind = infer_kind_from_filename(path)
    inferred_segment = infer_segment_slug_from_filename(path)
    inferred_retailer = infer_retailer_scope(path.stem)
    inferred_period = infer_period_from_filename(path)

    if SUMMARY_REQUIRED_COLUMNS.issubset(column_set):
        segment_name = None
        if "Segment Name" in header_frame.columns:
            segment_series = header_frame["Segment Name"].dropna()
            if not segment_series.empty:
                segment_name = str(segment_series.iloc[0]).strip()

        retailer_scope = inferred_retailer
        if "Retailer Name" in header_frame.columns:
            retailer_series = header_frame["Retailer Name"].dropna()
            if not retailer_series.empty:
                retailer_scope = infer_retailer_scope(str(retailer_series.iloc[0]))

        period = inferred_period
        if "Week Ending" in header_frame.columns:
            try:
                week_series = pd.read_csv(path, usecols=["Week Ending"])["Week Ending"]
            except Exception:
                week_series = header_frame["Week Ending"]
            period = infer_period_from_dates(week_series) or period

        return {
            "segment": segment_name or inferred_segment or path.stem,
            "period": period or "unknown",
            "kind": "summary",
            "retailer_scope": retailer_scope or "",
            "discovery_method": "content_sniff",
        }

    first_column = normalize_text(columns[0]) if columns else None
    if len(columns) >= 3 and first_column in TWO_PERIOD_FIRST_COLUMNS:
        return {
            "segment": inferred_segment or path.stem,
            "period": infer_period_from_header_labels(columns[1:3]) or inferred_period or "unknown",
            "kind": inferred_kind or "sales",
            "retailer_scope": inferred_retailer or "",
            "discovery_method": "content_sniff",
        }

    return None


def parse_stackline_file(path: Path) -> dict[str, str] | None:
    match = STACKLINE_FILE_RE.match(path.name)
    if match:
        parsed = match.groupdict()
        parsed["retailer_scope"] = infer_retailer_scope(path.stem) or ""
        parsed["discovery_method"] = "filename"
        return parsed
    return sniff_stackline_file(path)


def extract_segment_name(summary_path: Path) -> str | None:
    """Read just enough of the summary CSV to get the segment name."""
    try:
        series = pd.read_csv(summary_path, usecols=["Segment Name"])["Segment Name"].dropna()
    except Exception:
        return None
    if series.empty:
        return None
    return str(series.iloc[0]).strip()


def extract_retailer_scope(summary_path: Path) -> str | None:
    try:
        series = pd.read_csv(summary_path, usecols=["Retailer Name"])["Retailer Name"].dropna()
    except Exception:
        return None
    if series.empty:
        return None
    return infer_retailer_scope(str(series.iloc[0]))


def segment_match_score(subcategory: str, segment_slug: str, segment_name: str | None) -> float:
    """Score how well a Stackline file bundle matches the template subcategory."""
    query_tokens = text_tokens(subcategory)
    if not query_tokens:
        return 0.0

    candidates = [segment_slug]
    if segment_name:
        candidates.append(segment_name)

    best = 0.0
    for candidate in candidates:
        normalized = normalize_text(candidate)
        if normalized == normalize_text(subcategory):
            best = max(best, 1.0)
            continue

        candidate_tokens = text_tokens(candidate)
        if not candidate_tokens:
            continue

        overlap = len(query_tokens & candidate_tokens)
        if overlap:
            coverage = overlap / len(query_tokens)
            precision = overlap / len(candidate_tokens)
            score = max(coverage, (coverage + precision) / 2)
            if query_tokens.issubset(candidate_tokens):
                score += 0.25
            best = max(best, min(score, 0.99))

    return best


def discover_bundle(folder: Path, subcategory: str) -> StacklineBundle:
    """Find the newest matching Stackline bundle for a subcategory."""
    bundles = discover_bundles(folder, subcategory)
    return bundles[0]


def discover_bundles(folder: Path, subcategory: str) -> list[StacklineBundle]:
    """Find the best matching Stackline bundles for each retailer scope."""
    bundles: dict[tuple[str, str, str], StacklineBundle] = {}

    for path in folder.iterdir():
        if not path.is_file():
            continue

        parsed = parse_stackline_file(path)
        if not parsed:
            continue

        key = (parsed["segment"], parsed["period"], parsed.get("retailer_scope") or "")
        bundle = bundles.setdefault(
            key,
            StacklineBundle(
                segment_slug=parsed["segment"],
                period=parsed["period"],
                retailer_scope=parsed.get("retailer_scope") or None,
                discovery_method=parsed.get("discovery_method") or "filename",
            ),
        )
        kind = parsed["kind"].lower()
        if kind == "summary":
            bundle.summary_path = path
        elif kind == "traffic":
            bundle.traffic_path = path
        elif kind == "sales":
            bundle.sales_path = path

    if not bundles:
        raise FileNotFoundError(f"No Stackline exports found in {folder}")

    for bundle in bundles.values():
        if bundle.summary_path:
            bundle.segment_name = extract_segment_name(bundle.summary_path)
            bundle.retailer_scope = bundle.retailer_scope or extract_retailer_scope(
                bundle.summary_path
            )
        bundle.match_score = segment_match_score(
            subcategory,
            bundle.segment_slug,
            bundle.segment_name,
        )

    ranked = sorted(
        bundles.values(),
        key=lambda bundle: (
            bool(bundle.summary_path),
            bundle.match_score,
            bool(bundle.traffic_path),
            bool(bundle.sales_path),
            RETAILER_SCOPE_PRIORITY.get(retailer_scope_key(bundle.retailer_scope), 0),
            bundle.period,
        ),
        reverse=True,
    )
    if not ranked or ranked[0].match_score <= 0:
        raise FileNotFoundError(
            f"No Stackline bundle could be matched to subcategory '{subcategory}'."
        )

    selected_by_scope: dict[str, StacklineBundle] = {}
    for bundle in ranked:
        if bundle.match_score <= 0 or not bundle.summary_path:
            continue
        scope = retailer_scope_key(bundle.retailer_scope)
        if scope not in selected_by_scope:
            selected_by_scope[scope] = bundle

    if not selected_by_scope:
        raise FileNotFoundError(
            f"Matched Stackline exports for '{subcategory}' did not include a usable summary CSV."
        )

    selected = list(selected_by_scope.values())
    for selected_bundle in selected:
        compatible = [
            bundle
            for bundle in bundles.values()
            if bundle is not selected_bundle
            and bundle.period == selected_bundle.period
            and retailer_scope_key(bundle.retailer_scope)
            == retailer_scope_key(selected_bundle.retailer_scope)
            and bundle.match_score > 0
        ]
        if not selected_bundle.traffic_path:
            traffic_donor = next(
                (
                    bundle
                    for bundle in sorted(compatible, key=lambda bundle: bundle.match_score, reverse=True)
                    if bundle.traffic_path
                ),
                None,
            )
            if traffic_donor:
                selected_bundle.traffic_path = traffic_donor.traffic_path
        if not selected_bundle.sales_path:
            sales_donor = next(
                (
                    bundle
                    for bundle in sorted(compatible, key=lambda bundle: bundle.match_score, reverse=True)
                    if bundle.sales_path
                ),
                None,
            )
            if sales_donor:
                selected_bundle.sales_path = sales_donor.sales_path

    return sorted(
        selected,
        key=lambda bundle: (
            RETAILER_SCOPE_PRIORITY.get(retailer_scope_key(bundle.retailer_scope), 0),
            bundle.match_score,
            bool(bundle.traffic_path),
            bool(bundle.sales_path),
            bundle.period,
        ),
        reverse=True,
    )


def load_summary(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def align_two_period_series(path: Path) -> dict[str, Any]:
    """Return aligned current/prior values from a Stackline 2-column trend export."""
    frame = pd.read_csv(path)
    prior_label = frame.columns[1]
    current_label = frame.columns[2]
    aligned = frame[frame[current_label].notna()].copy()

    return {
        "path": str(path),
        "prior_label": prior_label,
        "current_label": current_label,
        "aligned_rows": int(len(aligned)),
        "prior_total": float(aligned[prior_label].sum()),
        "current_total": float(aligned[current_label].sum()),
        "prior_last_value": float(aligned[prior_label].iloc[-1]) if len(aligned) else None,
        "current_last_value": float(aligned[current_label].iloc[-1]) if len(aligned) else None,
        "delta_pct": pct_delta(
            float(aligned[current_label].sum()),
            float(aligned[prior_label].sum()),
        ),
    }


def pct_delta(current: float | None, prior: float | None) -> float | None:
    if current is None or prior in (None, 0):
        return None
    return (current / prior - 1) * 100


def clean_number(value: float | int | None, digits: int = 2) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return round(float(value), digits)


def pct_point_gap(current: float | None, baseline: float | None) -> float | None:
    """Return the simple percentage-point gap between two percent values."""
    if current is None or baseline is None:
        return None
    return current - baseline


def build_period_metrics(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for period in ["Main", "Comparison"]:
        period_df = df[df["TimePeriod"] == period]
        total_sales = float(period_df["Retail Sales"].sum())
        total_units = float(period_df["Units Sold"].sum())
        result[period] = {
            "retail_sales": clean_number(total_sales),
            "units_sold": clean_number(total_units),
            "avg_retail_price": clean_number(total_sales / total_units if total_units else None),
            "catalog_product_count": int(period_df["Retailer SKU"].nunique()),
            "brand_count": int(period_df["Brand"].nunique()),
            "week_count": int(period_df["Week Ending"].nunique()),
            "week_start": str(period_df["Week Ending"].min()),
            "week_end": str(period_df["Week Ending"].max()),
        }
    return result


def build_brand_table(df: pd.DataFrame, top_n: int = 10) -> list[dict[str, Any]]:
    grouped = (
        df.groupby("Brand", dropna=False, as_index=False)
        .agg(
            retail_sales=("Retail Sales", "sum"),
            units_sold=("Units Sold", "sum"),
            avg_retail_price=("Retail Price", "mean"),
            product_count=("Retailer SKU", "nunique"),
        )
        .sort_values("retail_sales", ascending=False)
        .head(top_n)
    )

    total_sales = float(df["Retail Sales"].sum())
    total_units = float(df["Units Sold"].sum())
    rows = []
    for record in grouped.to_dict(orient="records"):
        rows.append(
            {
                "brand": record["Brand"],
                "retail_sales": clean_number(record["retail_sales"]),
                "units_sold": int(record["units_sold"]),
                "avg_retail_price": clean_number(record["avg_retail_price"]),
                "product_count": int(record["product_count"]),
                "sales_share_pct": clean_number(record["retail_sales"] / total_sales * 100),
                "units_share_pct": clean_number(record["units_sold"] / total_units * 100),
            }
        )
    return rows


def build_product_table(df: pd.DataFrame, brand_to_exclude: str | None = None, top_n: int = 10) -> list[dict[str, Any]]:
    filtered = df
    if brand_to_exclude:
        filtered = df[df["Brand"].astype(str).str.upper() != brand_to_exclude.upper()]

    grouped = (
        filtered.groupby(
            ["Retailer SKU", "Model Number", "Brand", "Title"],
            dropna=False,
            as_index=False,
        )
        .agg(
            retail_sales=("Retail Sales", "sum"),
            units_sold=("Units Sold", "sum"),
            avg_retail_price=("Retail Price", "mean"),
            week_count=("Week ID", "nunique"),
        )
        .sort_values("retail_sales", ascending=False)
        .head(top_n)
    )

    total_sales = float(df["Retail Sales"].sum())
    rows = []
    for record in grouped.to_dict(orient="records"):
        rows.append(
            {
                "retailer_sku": record["Retailer SKU"],
                "model_number": record["Model Number"],
                "brand": record["Brand"],
                "title": record["Title"],
                "retail_sales": clean_number(record["retail_sales"]),
                "units_sold": int(record["units_sold"]),
                "avg_retail_price": clean_number(record["avg_retail_price"]),
                "week_count": int(record["week_count"]),
                "sales_share_pct": clean_number(record["retail_sales"] / total_sales * 100),
            }
        )
    return rows


def build_brand_focus_metrics(
    summary_df: pd.DataFrame,
    brand_name: str,
) -> dict[str, Any]:
    result: dict[str, Any] = {"brand": brand_name}
    for period in ["Main", "Comparison"]:
        period_df = summary_df[summary_df["TimePeriod"] == period]
        brand_df = period_df[period_df["Brand"].astype(str).str.upper() == brand_name.upper()]

        total_sales = float(period_df["Retail Sales"].sum())
        total_units = float(period_df["Units Sold"].sum())
        brand_sales = float(brand_df["Retail Sales"].sum())
        brand_units = float(brand_df["Units Sold"].sum())

        result[period.lower()] = {
            "retail_sales": clean_number(brand_sales),
            "units_sold": clean_number(brand_units),
            "product_count": int(brand_df["Retailer SKU"].nunique()),
            "sales_share_pct": clean_number(brand_sales / total_sales * 100 if total_sales else None),
            "units_share_pct": clean_number(brand_units / total_units * 100 if total_units else None),
            "avg_retail_price": clean_number(brand_sales / brand_units if brand_units else None),
        }

    result["deltas_pct"] = {
        "retail_sales": pct_delta(
            result["main"]["retail_sales"],
            result["comparison"]["retail_sales"],
        ),
        "units_sold": pct_delta(
            result["main"]["units_sold"],
            result["comparison"]["units_sold"],
        ),
        "sales_share_pct": pct_delta(
            result["main"]["sales_share_pct"],
            result["comparison"]["sales_share_pct"],
        ),
        "units_share_pct": pct_delta(
            result["main"]["units_share_pct"],
            result["comparison"]["units_share_pct"],
        ),
    }
    return result


def build_reference_metrics(summary_df: pd.DataFrame, reference_sku: str | None) -> dict[str, Any] | None:
    if not reference_sku:
        return None

    family = strip_pack_suffix(reference_sku.upper())
    model_mask = summary_df["Model Number"].astype(str).str.upper().str.contains(re.escape(family), na=False)
    if not model_mask.any():
        return {
            "reference_sku": reference_sku,
            "reference_family": family,
            "found": False,
        }

    result: dict[str, Any] = {
        "reference_sku": reference_sku,
        "reference_family": family,
        "found": True,
    }
    for period in ["Main", "Comparison"]:
        period_df = summary_df[(summary_df["TimePeriod"] == period) & model_mask]
        total_df = summary_df[summary_df["TimePeriod"] == period]

        sales = float(period_df["Retail Sales"].sum())
        units = float(period_df["Units Sold"].sum())
        total_sales = float(total_df["Retail Sales"].sum())
        total_units = float(total_df["Units Sold"].sum())

        grouped = (
            period_df.groupby(["Model Number", "Title"], dropna=False, as_index=False)
            .agg(
                retail_sales=("Retail Sales", "sum"),
                units_sold=("Units Sold", "sum"),
                avg_retail_price=("Retail Price", "mean"),
                retailer_skus=("Retailer SKU", "nunique"),
            )
            .sort_values("retail_sales", ascending=False)
        )

        result[period.lower()] = {
            "retail_sales": clean_number(sales),
            "units_sold": clean_number(units),
            "avg_retail_price": clean_number(sales / units if units else None),
            "sales_share_pct": clean_number(sales / total_sales * 100 if total_sales else None),
            "units_share_pct": clean_number(units / total_units * 100 if total_units else None),
            "variant_count": int(period_df["Retailer SKU"].nunique()),
            "variants": [
                {
                    "model_number": row["Model Number"],
                    "title": row["Title"],
                    "retail_sales": clean_number(row["retail_sales"]),
                    "units_sold": int(row["units_sold"]),
                    "avg_retail_price": clean_number(row["avg_retail_price"]),
                    "retailer_skus": int(row["retailer_skus"]),
                }
                for row in grouped.head(10).to_dict(orient="records")
            ],
        }

    result["deltas_pct"] = {
        "retail_sales": pct_delta(
            result["main"]["retail_sales"],
            result["comparison"]["retail_sales"],
        ),
        "units_sold": pct_delta(
            result["main"]["units_sold"],
            result["comparison"]["units_sold"],
        ),
        "sales_share_pct": pct_delta(
            result["main"]["sales_share_pct"],
            result["comparison"]["sales_share_pct"],
        ),
    }
    return result


def analyze_stackline(
    summary_path: Path,
    traffic_path: Path | None = None,
    sales_path: Path | None = None,
    brand_name: str = "Sunco Lighting",
    reference_sku: str | None = None,
    subcategory: str | None = None,
    match_bundle: StacklineBundle | None = None,
) -> dict[str, Any]:
    summary_df = load_summary(summary_path)
    period_metrics = build_period_metrics(summary_df)
    main_df = summary_df[summary_df["TimePeriod"] == "Main"]
    retailer_scope = None
    if "Retailer Name" in summary_df.columns:
        retailer_series = summary_df["Retailer Name"].dropna()
        if not retailer_series.empty:
            retailer_scope = infer_retailer_scope(str(retailer_series.iloc[0]))

    result: dict[str, Any] = {
        "subcategory": subcategory,
        "segment_name": str(summary_df["Segment Name"].dropna().iloc[0]),
        "retailer_scope": retailer_scope,
        "matched_bundle": None,
        "files": {
            "summary": str(summary_path),
            "traffic": str(traffic_path) if traffic_path else None,
            "sales": str(sales_path) if sales_path else None,
        },
        "segment_metrics": {
            "main": period_metrics["Main"],
            "comparison": period_metrics["Comparison"],
            "deltas_pct": {
                "retail_sales": pct_delta(
                    period_metrics["Main"]["retail_sales"],
                    period_metrics["Comparison"]["retail_sales"],
                ),
                "units_sold": pct_delta(
                    period_metrics["Main"]["units_sold"],
                    period_metrics["Comparison"]["units_sold"],
                ),
                "avg_retail_price": pct_delta(
                    period_metrics["Main"]["avg_retail_price"],
                    period_metrics["Comparison"]["avg_retail_price"],
                ),
            },
        },
        "brand_focus": build_brand_focus_metrics(summary_df, brand_name),
        "reference_model": build_reference_metrics(summary_df, reference_sku),
        "top_brands": build_brand_table(main_df),
        "top_competitor_products": build_product_table(main_df, brand_to_exclude=brand_name),
        "warnings": [],
    }

    if match_bundle:
        result["matched_bundle"] = {
            "segment_slug": match_bundle.segment_slug,
            "period": match_bundle.period,
            "retailer_scope": match_bundle.retailer_scope,
            "discovery_method": match_bundle.discovery_method,
            "match_score": clean_number(match_bundle.match_score, 4),
        }

    if traffic_path:
        traffic_metrics = align_two_period_series(traffic_path)
        result["traffic_metrics"] = traffic_metrics
        result["segment_metrics"]["main"]["total_traffic"] = clean_number(
            traffic_metrics["current_total"]
        )
        result["segment_metrics"]["comparison"]["total_traffic"] = clean_number(
            traffic_metrics["prior_total"]
        )
        result["segment_metrics"]["main"]["conversion_rate_pct"] = clean_number(
            period_metrics["Main"]["units_sold"] / traffic_metrics["current_total"] * 100
        )
        result["segment_metrics"]["comparison"]["conversion_rate_pct"] = clean_number(
            period_metrics["Comparison"]["units_sold"] / traffic_metrics["prior_total"] * 100
        )
        result["segment_metrics"]["deltas_pct"]["traffic"] = traffic_metrics["delta_pct"]
        result["segment_metrics"]["deltas_pct"]["conversion_rate_pct"] = pct_delta(
            result["segment_metrics"]["main"]["conversion_rate_pct"],
            result["segment_metrics"]["comparison"]["conversion_rate_pct"],
        )
    else:
        result["warnings"].append(
            "No traffic CSV provided. Traffic and conversion metrics are omitted."
        )
    if retailer_scope:
        result["warnings"].append(
            f"Stackline context is scoped to {retailer_scope.replace('_', ' ')} rather than an all-retailer market view."
        )

    if sales_path:
        result["sales_csv_observation"] = align_two_period_series(sales_path)
        result["warnings"].append(
            "The sales CSV schema does not currently map cleanly to retail sales dollars or units in this sample. Treat it as auxiliary only until it is validated across more segments."
        )

    result["performance_estimation_context"] = build_performance_estimation_context(result)
    return result


def build_performance_estimation_context(stackline_result: dict[str, Any]) -> dict[str, Any]:
    """Condense Stackline output into ideation performance-estimation inputs."""
    main = stackline_result["segment_metrics"]["main"]
    deltas = stackline_result["segment_metrics"]["deltas_pct"]
    sunco_brand_name = stackline_result["brand_focus"]["brand"]
    sunco_position = stackline_result["brand_focus"]["main"]
    reference_model = stackline_result.get("reference_model")
    top_brands = stackline_result.get("top_brands", [])
    top_competitors = stackline_result.get("top_competitor_products", [])
    leader_brand = top_brands[0] if top_brands else None
    reference_main = (
        reference_model.get("main")
        if reference_model and reference_model.get("found")
        else None
    )
    blended_reference_variants = (
        int(reference_main.get("variant_count", 0)) > 1
        if reference_main
        else False
    )

    if reference_main:
        price_anchor_source = "reference_family"
        price_anchor_value = reference_main.get("avg_retail_price")
    elif sunco_position.get("product_count"):
        price_anchor_source = "sunco_brand"
        price_anchor_value = sunco_position.get("avg_retail_price")
    else:
        price_anchor_source = "segment_average"
        price_anchor_value = main.get("avg_retail_price")

    segment_avg_price = main.get("avg_retail_price")
    price_gap_pct = None
    if price_anchor_value is not None and segment_avg_price not in (None, 0):
        price_gap_pct = (price_anchor_value / segment_avg_price - 1) * 100

    share_gap_to_leader = None
    if leader_brand and leader_brand["brand"] != sunco_brand_name:
        share_gap_to_leader = pct_point_gap(
            leader_brand.get("sales_share_pct"),
            sunco_position.get("sales_share_pct"),
        )

    opportunity_signals = []
    retail_sales_growth = deltas.get("retail_sales")
    traffic_growth = deltas.get("traffic")
    conversion_rate_change = deltas.get("conversion_rate_pct")
    sunco_sales_share = sunco_position.get("sales_share_pct")

    if retail_sales_growth is not None and retail_sales_growth >= 10:
        opportunity_signals.append("segment_sales_growth_above_10pct")
    if sunco_sales_share is not None and sunco_sales_share < 5:
        opportunity_signals.append("sunco_share_below_5pct")
    if share_gap_to_leader is not None and share_gap_to_leader >= 5:
        opportunity_signals.append("meaningful_share_gap_to_segment_leader")
    if (
        traffic_growth is not None
        and traffic_growth >= 10
        and conversion_rate_change is not None
        and conversion_rate_change <= -10
    ):
        opportunity_signals.append("traffic_up_conversion_down")
    if reference_model and not reference_model.get("found"):
        opportunity_signals.append("reference_family_absent_from_stackline_segment")
    if price_gap_pct is not None and price_gap_pct >= 20 and not blended_reference_variants:
        opportunity_signals.append("current_price_anchor_above_segment_average")
    if price_gap_pct is not None and price_gap_pct <= -20 and not blended_reference_variants:
        opportunity_signals.append("current_price_anchor_below_segment_average")
    if blended_reference_variants:
        opportunity_signals.append("reference_family_spans_multiple_listings")

    warnings = list(stackline_result.get("warnings", []))
    if blended_reference_variants:
        warnings.append(
            "Reference family pricing is blended across multiple retailer listings / pack sizes. Use the price anchor as directional context, not a normalized unit-price benchmark."
        )

    return {
        "segment": {
            "name": stackline_result.get("segment_name"),
            "retailer_scope": stackline_result.get("retailer_scope"),
            "matched_bundle": stackline_result.get("matched_bundle"),
            "market_snapshot": {
                "retail_sales": main.get("retail_sales"),
                "units_sold": main.get("units_sold"),
                "avg_retail_price": main.get("avg_retail_price"),
                "total_traffic": main.get("total_traffic"),
                "conversion_rate_pct": main.get("conversion_rate_pct"),
                "catalog_product_count": main.get("catalog_product_count"),
                "brand_count": main.get("brand_count"),
            },
            "market_momentum_pct": {
                "retail_sales": retail_sales_growth,
                "units_sold": deltas.get("units_sold"),
                "avg_retail_price": deltas.get("avg_retail_price"),
                "traffic": traffic_growth,
                "conversion_rate_pct": conversion_rate_change,
            },
        },
        "sunco_position": {
            "brand": sunco_brand_name,
            "sales_share_pct": sunco_position.get("sales_share_pct"),
            "units_share_pct": sunco_position.get("units_share_pct"),
            "product_count": sunco_position.get("product_count"),
            "avg_retail_price": sunco_position.get("avg_retail_price"),
        },
        "reference_family": (
            {
                "reference_sku": reference_model.get("reference_sku"),
                "reference_family": reference_model.get("reference_family"),
                "found": reference_model.get("found"),
                "main": reference_model.get("main"),
                "deltas_pct": reference_model.get("deltas_pct"),
            }
            if reference_model
            else None
        ),
        "estimation_inputs": {
            "price_anchor": {
                "source": price_anchor_source,
                "avg_retail_price": clean_number(price_anchor_value),
                "gap_vs_segment_avg_pct": clean_number(price_gap_pct),
            },
            "segment_leader": {
                "brand": leader_brand.get("brand") if leader_brand else None,
                "sales_share_pct": leader_brand.get("sales_share_pct") if leader_brand else None,
                "share_gap_vs_sunco_pct_points": clean_number(share_gap_to_leader),
            },
            "top_competitor_products": top_competitors[:5],
            "top_brands": top_brands[:5],
        },
        "opportunity_signals": opportunity_signals,
        "warnings": warnings,
    }


def analyze_stackline_for_subcategory(
    subcategory: str,
    reference_sku: str | None = None,
    folder: Path = STACKLINE_DIR,
    brand_name: str = "Sunco Lighting",
) -> dict[str, Any]:
    """Discover and analyze the newest Stackline bundle for a template subcategory."""
    bundle = discover_bundle(folder, subcategory)
    if not bundle.summary_path:
        raise FileNotFoundError(
            f"Matched Stackline bundle for '{subcategory}' is missing a summary CSV."
        )

    return analyze_stackline(
        summary_path=bundle.summary_path,
        traffic_path=bundle.traffic_path,
        sales_path=bundle.sales_path,
        brand_name=brand_name,
        reference_sku=reference_sku,
        subcategory=subcategory,
        match_bundle=bundle,
    )


def build_channel_comparison(
    channel_results: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Summarize channel-level Stackline comparisons for packet/report use."""
    channel_summary: dict[str, Any] = {}
    for channel, result in channel_results.items():
        perf = result.get("performance_estimation_context") or {}
        segment = perf.get("segment") or {}
        snapshot = segment.get("market_snapshot") or {}
        momentum = segment.get("market_momentum_pct") or {}
        sunco_position = perf.get("sunco_position") or {}
        channel_summary[channel] = {
            "segment_name": segment.get("name"),
            "retailer_scope": segment.get("retailer_scope") or channel,
            "matched_bundle": segment.get("matched_bundle"),
            "retail_sales": snapshot.get("retail_sales"),
            "units_sold": snapshot.get("units_sold"),
            "avg_retail_price": snapshot.get("avg_retail_price"),
            "total_traffic": snapshot.get("total_traffic"),
            "conversion_rate_pct": snapshot.get("conversion_rate_pct"),
            "retail_sales_growth_pct": momentum.get("retail_sales"),
            "units_sold_growth_pct": momentum.get("units_sold"),
            "avg_retail_price_growth_pct": momentum.get("avg_retail_price"),
            "traffic_growth_pct": momentum.get("traffic"),
            "sunco_sales_share_pct": sunco_position.get("sales_share_pct"),
            "sunco_units_share_pct": sunco_position.get("units_share_pct"),
            "warnings": result.get("warnings", []),
        }

    comparisons: dict[str, Any] = {}
    amazon = channel_summary.get("amazon")
    home_depot = channel_summary.get("home_depot")
    if amazon and home_depot:
        comparisons["amazon_vs_home_depot"] = {
            "avg_retail_price_gap_pct": clean_number(
                pct_delta(amazon.get("avg_retail_price"), home_depot.get("avg_retail_price"))
            ),
            "retail_sales_gap_pct": clean_number(
                pct_delta(amazon.get("retail_sales"), home_depot.get("retail_sales"))
            ),
            "units_sold_gap_pct": clean_number(
                pct_delta(amazon.get("units_sold"), home_depot.get("units_sold"))
            ),
            "sunco_sales_share_gap_pct_points": clean_number(
                pct_point_gap(
                    amazon.get("sunco_sales_share_pct"),
                    home_depot.get("sunco_sales_share_pct"),
                )
            ),
        }

    return {
        "available_channels": list(channel_summary.keys()),
        "channels": channel_summary,
        "comparisons": comparisons,
    }


def analyze_stackline_channels_for_subcategory(
    subcategory: str,
    reference_sku: str | None = None,
    folder: Path = STACKLINE_DIR,
    brand_name: str = "Sunco Lighting",
) -> dict[str, Any]:
    """Discover and analyze all matching retailer-scoped Stackline bundles for a subcategory."""
    bundles = discover_bundles(folder, subcategory)
    primary_bundle = bundles[0]
    channel_results: dict[str, dict[str, Any]] = {}
    for bundle in bundles:
        if not bundle.summary_path:
            continue
        result = analyze_stackline(
            summary_path=bundle.summary_path,
            traffic_path=bundle.traffic_path,
            sales_path=bundle.sales_path,
            brand_name=brand_name,
            reference_sku=reference_sku,
            subcategory=subcategory,
            match_bundle=bundle,
        )
        channel_results[retailer_scope_key(bundle.retailer_scope)] = result

    if not channel_results:
        raise FileNotFoundError(
            f"No Stackline bundle could be matched to subcategory '{subcategory}'."
        )

    primary_key = retailer_scope_key(primary_bundle.retailer_scope)
    primary_result = channel_results[primary_key]
    comparison = build_channel_comparison(channel_results)
    warnings = []
    for result in channel_results.values():
        warnings.extend(result.get("warnings", []))

    return {
        "subcategory": subcategory,
        "segment_name": primary_result.get("segment_name"),
        "primary_channel": primary_key,
        "primary_analysis": primary_result,
        "channels": {
            channel: {
                "analysis": result,
                "performance_estimation_context": result.get("performance_estimation_context"),
            }
            for channel, result in channel_results.items()
        },
        "channel_comparison": comparison,
        "warnings": list(dict.fromkeys(warnings)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Stackline exports.")
    parser.add_argument("--summary", default=None, help="Path to a Stackline summary CSV.")
    parser.add_argument("--traffic", default=None, help="Path to a Stackline traffic CSV.")
    parser.add_argument("--sales", default=None, help="Path to a Stackline sales CSV.")
    parser.add_argument(
        "--folder",
        default=str(STACKLINE_DIR),
        help="Folder containing Stackline exports.",
    )
    parser.add_argument(
        "--subcategory",
        default=None,
        help="Template subcategory to match against the newest Stackline bundle.",
    )
    parser.add_argument(
        "--brand",
        default="Sunco Lighting",
        help="Brand name to treat as the internal brand focus.",
    )
    parser.add_argument(
        "--reference-sku",
        default=None,
        help="Reference SKU used to match the Stackline model family.",
    )
    parser.add_argument("--output", default=None, help="Optional path to write JSON output.")
    args = parser.parse_args()

    bundle = None
    summary_path = Path(args.summary) if args.summary else None
    traffic_path = Path(args.traffic) if args.traffic else None
    sales_path = Path(args.sales) if args.sales else None

    if not summary_path:
        if not args.subcategory:
            raise SystemExit("Provide --summary or --subcategory.")
        result = analyze_stackline_for_subcategory(
            subcategory=args.subcategory,
            reference_sku=args.reference_sku,
            folder=Path(args.folder),
            brand_name=args.brand,
        )
    else:
        result = analyze_stackline(
            summary_path=summary_path,
            traffic_path=traffic_path,
            sales_path=sales_path,
            brand_name=args.brand,
            reference_sku=args.reference_sku,
            subcategory=args.subcategory,
            match_bundle=bundle,
        )

    output = json.dumps(result, indent=2)
    if args.output:
        Path(args.output).write_text(output + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
