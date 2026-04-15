# Step 2: Reference SKU Lightweight Lookup — Progress & Next Steps

## Status: IN PROGRESS

## What's Done
1. **Postgres MCP connected to Nimbalyst** — both `postgres` and `redshift` MCPs are configured and working
2. **Database schema explored** — all key tables and joins mapped out
3. **CSV lookup script written** — `tools/sku_lookup.py` handles metadata CSV image lookup + merge logic
4. **Test queries validated** — confirmed sales, pricing, and title queries all work

## Database Schema (Confirmed Working)

### Tables & Key Columns
| Table | Key Columns | Purpose |
|-------|------------|---------|
| `shopify_shopifyproduct` | id, shopify_id, title, vendor, product_type | Product title |
| `shopify_productvariantatshopify` | sku, shopify_product_id, price | SKU → Shopify product mapping |
| `shopify_shopifymedia` | product_id, original_source, response_json, position | Product images (newer products only, ID 1333+) |
| `products_product` | master_sku, name, pack_size, category_id | Internal product data |
| `skubana_order` | order_id, order_date, sales_channel_id, order_status | Orders |
| `skubana_orderitem` | order_id, listing_sku, sales_price, quantity_ordered | Order line items |
| `skubana_listing` | id, listing_sku, sales_channel_id, product_id | SKU listings |
| `pricing_listingprice` | listing_sku_id (FK → skubana_listing.id), list_price | Current prices |

### Sales Channels
- **Shopify:** 12585
- **Amazon US:** 11929

### Working Queries

**Title lookup:**
```sql
SELECT sp.title
FROM shopify_productvariantatshopify sv
JOIN shopify_shopifyproduct sp ON sp.id = sv.shopify_product_id
WHERE sv.sku = '{sku}'
LIMIT 1
```

**Listing price:**
```sql
SELECT pl.list_price, sl.listing_sku, sl.sales_channel_id
FROM pricing_listingprice pl
JOIN skubana_listing sl ON sl.id = pl.listing_sku_id
WHERE sl.listing_sku = '{sku}'
AND sl.sales_channel_id IN (12585, 11929)
```

**12-month sales (ALWAYS split by channel):**
```sql
SELECT
    o.sales_channel_id,
    SUM(oi.sales_price * oi.quantity_ordered) as revenue,
    SUM(oi.quantity_ordered) as units
FROM skubana_order o
JOIN skubana_orderitem oi ON o.order_id = oi.order_id
WHERE oi.listing_sku LIKE '{family}%'
  AND o.order_date >= '2025-04-01' AND o.order_date < '2026-04-01'
  AND o.order_status NOT IN ('CANCELLED')
  AND o.sales_channel_id IN (12585, 11929)
GROUP BY o.sales_channel_id
```

**Image (Shopify media — only works for newer products):**
```sql
SELECT sm.original_source,
       sm.response_json::json->'preview'->'image'->>'url' as cdn_url
FROM shopify_shopifymedia sm
JOIN shopify_shopifyproduct sp ON sp.id = sm.product_id
JOIN shopify_productvariantatshopify sv ON sv.shopify_product_id = sp.id
WHERE sv.sku = '{sku}'
AND sm.position = 1
LIMIT 1
```

### Key Finding: Image Fallback Needed
- `shopify_shopifymedia` only has images for products with ID >= 1333
- Many established SKUs (panels, etc.) have no image in Postgres
- **Solution:** Use metadata CSV (`SUNCO ALL METADATA.csv` → `Image Src` column) as fallback
- Script `tools/sku_lookup.py` already handles this

## What's Left to Do

### Immediate (finish Step 2):
1. **Test the sku_lookup.py script** — run it against a real SKU to verify CSV lookup works
2. **Create an orchestration script or document** that shows how Claude will:
   - Run MCP queries for title + price + sales
   - Run sku_lookup.py for image
   - Merge into final JSON output
3. **Test end-to-end** with 3-5 SKUs across different categories (panels, emergency, strips, etc.)
4. **Git checkpoint** — commit as "Step 2 - Reference SKU lookup working"

### Then Step 3: Template Parser + Enrichment
- Read filled Excel template (openpyxl)
- Parse each row into a structured ideation object
- For each row, run Step 2 lookup → attach image, price, sales context
- User-entered specs are the TARGET (never overwritten)
- Output: list of enriched ideation objects ready for competitive research

### Then Step 4: Competitive Research Engine
- WebSearch for Amazon competitors matching specs
- WebSearch for Home Depot / Walmart / Lowe's competitors
- Stackline integration (if Stackline Data? = Yes, read from local sync folder)
- Known competitors from Competitors.md

### Then Steps 5-6: Analysis + Report Generation
- Pricing analysis, spec recommendations
- Excel report (one sheet per ideation)
- SharePoint upload + SKILL.md definition

## Files Created This Session
- `tools/sku_lookup.py` — CSV image lookup + merge logic + MCP query templates
- `docs/STEP2_PROGRESS.md` — this file
