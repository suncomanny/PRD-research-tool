"""
Reference SKU Lightweight Lookup — Step 2
Looks up baseline context for a Reference SKU:
  - Product image URL (from metadata CSV)
  - Product title (from metadata CSV, Postgres backup)
  - Current listing price (from Postgres via MCP)
  - 12-month sales split by channel (Shopify vs Amazon, from Postgres via MCP)

Usage:
  python sku_lookup.py <sku>                     # Image + title from CSV only
  python sku_lookup.py <sku> --with-sales <json>  # Merge in Postgres sales data

The Postgres queries are executed by Claude via MCP and passed in as --with-sales JSON.
This script handles the CSV-based lookups and merges everything into a final JSON output.
"""

import argparse
import json
import os
import re
import sys
from datetime import date
from functools import lru_cache

import pandas as pd


# ── Paths ────────────────────────────────────────────────────────────────
RESOURCES_DIR = os.path.join(
    os.path.expanduser("~"),
    "OneDrive - Sunco Lighting",
    "Documents",
    "Claude Workbook",
    "Manny Sunco",
    "Resources",
)
METADATA_FILE = "SUNCO ALL METADATA.csv"
SHOPIFY_SALES_FILE = "SUNCO 2025 ALL SALES Shopify - Categorized.csv"
AMAZON_SALES_FILE = "FULL SUNCO 2025 SALES Amazon.csv"
LOCAL_SALES_PERIOD_LABEL = "FY2025 local export fallback"


def strip_pack_suffix(sku: str) -> str:
    """Strip -XPK / -XPC / -XPK-FBM / -XPK-M suffixes to get the SKU family."""
    return re.sub(r'-\d+(PK|PC)(-FBM|-M)?$', '', sku, flags=re.IGNORECASE)


def find_smallest_pack_sku(metadata_df: pd.DataFrame, family: str) -> pd.Series | None:
    """Find the smallest pack variant row for a SKU family in metadata."""
    family_rows = metadata_df[metadata_df['family'] == family]
    if family_rows.empty:
        return None
    return family_rows.loc[family_rows['pack'].idxmin()]


def extract_pack_count(sku: str) -> int:
    """Extract numeric pack count from -XPK suffix. Returns 1 if no suffix."""
    if pd.isna(sku):
        return 1
    m = re.search(r'-(\d+)(PK|PC)', str(sku), re.IGNORECASE)
    return int(m.group(1)) if m else 1


def parse_currency_value(value) -> float | None:
    """Parse a currency-like value into a float."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text or text in {"$-", "$- ", "-", "nan", "None"}:
        return None

    text = text.replace("$", "").replace(",", "").strip()
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1]

    try:
        parsed = float(text)
    except ValueError:
        return None
    return -parsed if negative else parsed


def handle_to_title(handle: str | None) -> str | None:
    """Convert a Shopify handle slug into a readable title."""
    if not handle:
        return None
    text = str(handle).strip().replace("-", " ")
    text = re.sub(r"\s+", " ", text)
    return text.title() if text else None


def title_is_usable(title: str | None) -> bool:
    """Reject obviously bad fallback titles like '2' or other placeholder slugs."""
    if not title or not isinstance(title, str):
        return False
    cleaned = title.strip()
    if len(cleaned) < 4:
        return False
    return bool(re.search(r"[A-Za-z]", cleaned))


@lru_cache(maxsize=1)
def load_metadata() -> pd.DataFrame:
    """Load metadata CSV and add family + pack columns."""
    path = os.path.join(RESOURCES_DIR, METADATA_FILE)
    if not os.path.exists(path):
        print(f"ERROR: Metadata file not found: {path}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(path, engine='python', on_bad_lines='skip')
    df['family'] = df['Variant SKU'].apply(
        lambda x: strip_pack_suffix(str(x)) if pd.notna(x) else ''
    )
    df['pack'] = df['Variant SKU'].apply(extract_pack_count)
    return df


@lru_cache(maxsize=1)
def load_shopify_sales() -> pd.DataFrame:
    """Load Shopify sales export and normalize SKU family columns."""
    path = os.path.join(RESOURCES_DIR, SHOPIFY_SALES_FILE)
    if not os.path.exists(path):
        return pd.DataFrame()

    df = pd.read_csv(path, engine='python', on_bad_lines='skip')
    if 'Product variant SKU' not in df.columns:
        return pd.DataFrame()

    df['Product variant SKU'] = df['Product variant SKU'].astype(str).str.strip()
    df['family'] = df['Product variant SKU'].apply(
        lambda x: strip_pack_suffix(str(x)).upper() if pd.notna(x) else ''
    )
    if 'Net sales' in df.columns:
        df['Net sales parsed'] = df['Net sales'].apply(parse_currency_value)
    else:
        df['Net sales parsed'] = None
    if 'Net items sold' in df.columns:
        df['Net items sold parsed'] = pd.to_numeric(df['Net items sold'], errors='coerce')
    else:
        df['Net items sold parsed'] = None
    return df


@lru_cache(maxsize=1)
def load_amazon_sales() -> pd.DataFrame:
    """Load Amazon sales export and normalize SKU family columns."""
    path = os.path.join(RESOURCES_DIR, AMAZON_SALES_FILE)
    if not os.path.exists(path):
        return pd.DataFrame()

    df = pd.read_csv(path, engine='python', on_bad_lines='skip')
    if 'SKU' not in df.columns:
        return pd.DataFrame()

    df['SKU'] = df['SKU'].astype(str).str.strip()
    df['family'] = df['SKU'].apply(
        lambda x: strip_pack_suffix(str(x)).upper() if pd.notna(x) else ''
    )
    sales_column = 'Sales ' if 'Sales ' in df.columns else 'Sales'
    if sales_column in df.columns:
        df['Sales parsed'] = df[sales_column].apply(parse_currency_value)
    else:
        df['Sales parsed'] = None
    if 'Units' in df.columns:
        df['Units parsed'] = pd.to_numeric(df['Units'], errors='coerce')
    else:
        df['Units parsed'] = None
    return df


def summarize_local_sales(
    sales_df: pd.DataFrame,
    family: str,
    revenue_column: str,
    units_column: str,
    title_column: str,
) -> dict[str, float | str | None]:
    """Aggregate one local sales export by SKU family."""
    if sales_df.empty:
        return {
            "revenue": None,
            "units": None,
            "title": None,
        }

    family_rows = sales_df[sales_df['family'] == family]
    if family_rows.empty:
        return {
            "revenue": None,
            "units": None,
            "title": None,
        }

    revenue = family_rows[revenue_column].dropna().sum()
    units = family_rows[units_column].dropna().sum()

    title = None
    for candidate in family_rows.get(title_column, pd.Series(dtype=object)).tolist():
        if isinstance(candidate, str) and candidate.strip():
            title = candidate.strip()
            break

    return {
        "revenue": float(revenue) if pd.notna(revenue) else None,
        "units": float(units) if pd.notna(units) else None,
        "title": title,
    }


def apply_local_fallbacks(result: dict, row: pd.Series | None) -> dict:
    """Fill reference baseline fields from local metadata + sales exports."""
    family = str(result.get("family") or "").upper()
    fallback_used = False

    if row is not None:
        variant_price = parse_currency_value(row.get('Variant Price'))
        if result.get('listing_price') is None and variant_price is not None:
            result['listing_price'] = variant_price
            result['listing_price_source'] = 'metadata_variant_price'
            fallback_used = True

        if not result.get('title'):
            title_from_handle = handle_to_title(result.get('handle'))
            if title_is_usable(title_from_handle):
                result['title'] = title_from_handle
                result['title_source'] = 'metadata_handle'
                fallback_used = True

    shopify_summary = summarize_local_sales(
        load_shopify_sales(),
        family=family,
        revenue_column='Net sales parsed',
        units_column='Net items sold parsed',
        title_column='Product title',
    )
    amazon_summary = summarize_local_sales(
        load_amazon_sales(),
        family=family,
        revenue_column='Sales parsed',
        units_column='Units parsed',
        title_column='Name',
    )

    local_title = shopify_summary.get('title') or amazon_summary.get('title')
    if local_title and (
        not title_is_usable(result.get('title'))
        or result.get('title_source') == 'metadata_handle'
    ):
        result['title'] = local_title
        result['title_source'] = 'local_sales_export'
        fallback_used = True

    if result.get('shopify_revenue_12mo') is None and shopify_summary.get('revenue') is not None:
        result['shopify_revenue_12mo'] = shopify_summary['revenue']
        result['shopify_units_12mo'] = shopify_summary.get('units')
        result['shopify_data_source'] = 'shopify_sales_csv'
        fallback_used = True

    if result.get('amazon_revenue_12mo') is None and amazon_summary.get('revenue') is not None:
        result['amazon_revenue_12mo'] = amazon_summary['revenue']
        result['amazon_units_12mo'] = amazon_summary.get('units')
        result['amazon_data_source'] = 'amazon_sales_csv'
        fallback_used = True

    if fallback_used:
        result.setdefault('sales_period_label', LOCAL_SALES_PERIOD_LABEL)
        result.setdefault('reference_data_source', 'local_metadata_and_sales_exports')

    return result


def lookup_from_csv(sku: str) -> dict:
    """Look up image URL and title from metadata CSV."""
    metadata_df = load_metadata()
    family = strip_pack_suffix(sku)

    # Try exact SKU match first, then family match (smallest pack)
    exact = metadata_df[metadata_df['Variant SKU'] == sku]
    if not exact.empty:
        row = exact.iloc[0]
    else:
        row = find_smallest_pack_sku(metadata_df, family)

    if row is None:
        result = {
            "sku": sku,
            "family": family,
            "found": False,
            "image_url": None,
            "title": None,
            "product_type": None,
            "handle": None,
            "listing_price": None,
            "shopify_revenue_12mo": None,
            "shopify_units_12mo": None,
            "amazon_revenue_12mo": None,
            "amazon_units_12mo": None,
        }
        return apply_local_fallbacks(result, None)

    # Extract image URL — use Image Src column
    image_url = row.get('Image Src')
    if pd.isna(image_url):
        image_url = None

    # Extract title from Handle (Shopify slug) — convert to readable title
    handle = row.get('Handle')
    if pd.isna(handle):
        handle = None

    # Product type from CSV (e.g., "Panel Lights", "Vapor Tight")
    product_type = row.get('Type')
    if pd.isna(product_type) if isinstance(product_type, float) else not product_type:
        product_type = None

    result = {
        "sku": sku,
        "family": family,
        "found": True,
        "image_url": str(image_url) if image_url else None,
        "title": None,  # Prefer Postgres title when available; local fallbacks fill this otherwise.
        "product_type": str(product_type) if product_type else None,
        "handle": str(handle) if handle else None,
    }
    return apply_local_fallbacks(result, row)


def merge_postgres_data(csv_result: dict, postgres_data: dict) -> dict:
    """Merge Postgres MCP query results into the CSV lookup result."""
    result = {**csv_result}

    # Postgres title overrides CSV if available (more readable)
    if postgres_data.get('title'):
        result['title'] = postgres_data['title']

    # Listing price
    if postgres_data.get('listing_price') is not None:
        result['listing_price'] = postgres_data.get('listing_price')
        result['listing_price_source'] = 'postgres_mcp'

    # Sales split by channel — ALWAYS separate
    if postgres_data.get('shopify_revenue') is not None:
        result['shopify_revenue_12mo'] = postgres_data.get('shopify_revenue')
        result['shopify_data_source'] = 'postgres_mcp'
    if postgres_data.get('shopify_units') is not None:
        result['shopify_units_12mo'] = postgres_data.get('shopify_units')
    if postgres_data.get('amazon_revenue') is not None:
        result['amazon_revenue_12mo'] = postgres_data.get('amazon_revenue')
        result['amazon_data_source'] = 'postgres_mcp'
    if postgres_data.get('amazon_units') is not None:
        result['amazon_units_12mo'] = postgres_data.get('amazon_units')

    if any(
        postgres_data.get(key) is not None
        for key in ('listing_price', 'shopify_revenue', 'amazon_revenue')
    ):
        result['reference_data_source'] = 'postgres_mcp_plus_metadata'
        result['sales_period_label'] = 'Last 12 complete months via Postgres MCP'

    return result


# ── MCP Query Templates (for Claude to execute via Postgres MCP) ─────────
MCP_QUERIES = {
    "title": """
        SELECT sp.title
        FROM shopify_productvariantatshopify sv
        JOIN shopify_shopifyproduct sp ON sp.id = sv.shopify_product_id
        WHERE sv.sku = '{sku}'
        LIMIT 1
    """,
    "listing_price": """
        SELECT pl.list_price, sl.listing_sku, sl.sales_channel_id
        FROM pricing_listingprice pl
        JOIN skubana_listing sl ON sl.id = pl.listing_sku_id
        WHERE sl.listing_sku = '{sku}'
        AND sl.sales_channel_id IN (12585, 11929)
    """,
    "sales_12mo": """
        SELECT
            o.sales_channel_id,
            SUM(oi.sales_price * oi.quantity_ordered) as revenue,
            SUM(oi.quantity_ordered) as units
        FROM skubana_order o
        JOIN skubana_orderitem oi ON o.order_id = oi.order_id
        WHERE oi.listing_sku LIKE '{family}%'
          AND o.order_date >= '{start_date}'
          AND o.order_date < '{end_date}'
          AND o.order_status NOT IN ('CANCELLED')
          AND o.sales_channel_id IN (12585, 11929)
        GROUP BY o.sales_channel_id
    """,
}


def default_sales_window(anchor_date: date | None = None) -> tuple[str, str]:
    """Return the last 12 complete calendar months as ISO date strings."""
    anchor = anchor_date or date.today()
    end_date = date(anchor.year, anchor.month, 1)
    start_year = end_date.year - 1
    start_date = date(start_year, end_date.month, 1)
    return start_date.isoformat(), end_date.isoformat()


def build_mcp_queries(
    sku: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, str]:
    """Build the Postgres MCP query strings for a SKU."""
    family = strip_pack_suffix(sku)
    resolved_start, resolved_end = (
        (start_date, end_date)
        if start_date and end_date
        else default_sales_window()
    )

    queries = {}
    for name, query in MCP_QUERIES.items():
        queries[name] = query.format(
            sku=sku,
            family=family,
            start_date=resolved_start,
            end_date=resolved_end,
        ).strip()
    return queries


def print_mcp_queries(
    sku: str,
    start_date: str | None = None,
    end_date: str | None = None,
):
    """Print the MCP queries Claude should execute for this SKU."""
    print("\n--- MCP Queries for Claude to execute ---")
    for name, filled in build_mcp_queries(
        sku=sku,
        start_date=start_date,
        end_date=end_date,
    ).items():
        print(f"\n[{name}]")
        print(filled)


def main():
    parser = argparse.ArgumentParser(description='Reference SKU Lightweight Lookup')
    parser.add_argument('sku', help='Reference SKU to look up (e.g., PN24_HO-4060K-1PK)')
    parser.add_argument('--with-sales', type=str, default=None,
                        help='JSON string with Postgres sales data to merge')
    parser.add_argument('--show-queries', action='store_true',
                        help='Print the MCP queries Claude should execute')
    parser.add_argument('--start-date', type=str, default=None,
                        help='Override MCP sales window start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default=None,
                        help='Override MCP sales window end date (YYYY-MM-DD)')
    args = parser.parse_args()

    # CSV lookup
    result = lookup_from_csv(args.sku)

    # Merge Postgres data if provided
    if args.with_sales:
        postgres_data = json.loads(args.with_sales)
        result = merge_postgres_data(result, postgres_data)

    # Output
    print(json.dumps(result, indent=2))

    if args.show_queries:
        print_mcp_queries(
            args.sku,
            start_date=args.start_date,
            end_date=args.end_date,
        )


if __name__ == '__main__':
    main()
