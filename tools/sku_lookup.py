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
        return {
            "sku": sku,
            "family": family,
            "found": False,
            "image_url": None,
            "title": None,
            "product_type": None,
            "handle": None,
        }

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

    return {
        "sku": sku,
        "family": family,
        "found": True,
        "image_url": str(image_url) if image_url else None,
        "title": None,  # Title comes from Postgres (shopify_shopifyproduct.title)
        "product_type": str(product_type) if product_type else None,
        "handle": str(handle) if handle else None,
    }


def merge_postgres_data(csv_result: dict, postgres_data: dict) -> dict:
    """Merge Postgres MCP query results into the CSV lookup result."""
    result = {**csv_result}

    # Postgres title overrides CSV if available (more readable)
    if postgres_data.get('title'):
        result['title'] = postgres_data['title']

    # Listing price
    result['listing_price'] = postgres_data.get('listing_price')

    # Sales split by channel — ALWAYS separate
    result['shopify_revenue_12mo'] = postgres_data.get('shopify_revenue')
    result['shopify_units_12mo'] = postgres_data.get('shopify_units')
    result['amazon_revenue_12mo'] = postgres_data.get('amazon_revenue')
    result['amazon_units_12mo'] = postgres_data.get('amazon_units')

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
