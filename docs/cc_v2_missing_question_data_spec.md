# CC V2 Missing Question Data Spec

## Overview of Data Availability

| source_system | exists_in_postgres | exists_in_stackline_csv | exists_elsewhere |
|---|---|---|---|
| `skubana_order` + `skubana_orderitem` | yes - `order_date`, `sales_channel_id`, `customer_id`, `listing_sku`, `sales_price`, `quantity_ordered` | n/a | n/a |
| `brand_analytics_keywords` | no | n/a | Amazon Brand Analytics exports |
| `gold_sales` / `gold_traffic` | no | yes - local Stackline CSV bundles | n/a |

## Q2 Demand Consistency

| field | value |
|---|---|
| question_id | Q2 |
| ideal_source_system | Postgres `skubana_order` + `skubana_orderitem` |
| fallback_source_system | none |
| exact_data_fields_needed | `order_date` by month, `sales_channel_id`, `listing_sku LIKE family%`, `quantity_ordered` |
| grain_of_data_needed | family x channel x month (24 months) |
| current_packets_contain_enough_data | no |
| fetch_stage | packet-generation time |
| recommended_packet_shape | `reference_baseline.monthly_sales = [{month, sales_channel_id, revenue, units, distinct_customers}]` |
| recommended_scoring_formula | count active months with >=1 order in the trailing 12 months |
| recommended_thresholds | 11-12 months=10, 9-10=8, 7-8=6, 5-6=4, under 5=2 |
| risks | rewards being listed/in stock more than strong demand; CoV may replace this later |

## Q3 Bulk Buyer Analysis

| field | value |
|---|---|
| question_id | Q3 |
| ideal_source_system | Postgres `skubana_order` + `skubana_orderitem` |
| fallback_source_system | none |
| exact_data_fields_needed | `customer_id`, `sales_channel_id`, `listing_sku LIKE family%`, `sales_price`, `quantity_ordered`, `order_date` |
| grain_of_data_needed | family x channel x customer (12 months) |
| current_packets_contain_enough_data | no |
| fetch_stage | packet-generation time |
| recommended_packet_shape | `reference_baseline.customer_concentration.{amazon|shopify} = {total_customers, total_revenue, top_20pct_customer_count, top_20pct_revenue, top_20pct_revenue_share_pct, repeat_customer_count, repeat_rate_pct}` |
| recommended_scoring_formula | Amazon favors distributed demand; Sunco.com favors concentration with a volume guard; repeat rate can add a bonus |
| recommended_thresholds | Amazon: top-20pct share under 40%=10, 40-55%=8, 55-70%=6, 70-85%=4, above 85%=2. Sunco.com: if total_customers under 10 cap at 5; otherwise top-20pct share above 70%=10, 55-70%=8, 40-55%=6, under 40%=4 |
| risks | channel logic is inverted; Amazon customer identity may be marketplace-limited; Sunco.com needs a volume guard |

## Q4 Sales Trend

| field | value |
|---|---|
| question_id | Q4 |
| ideal_source_system | Postgres `skubana_order` + `skubana_orderitem` |
| fallback_source_system | none |
| exact_data_fields_needed | `order_date` by month, `sales_channel_id`, `listing_sku LIKE family%`, `sales_price`, `quantity_ordered` |
| grain_of_data_needed | family x channel x month (24 months) |
| current_packets_contain_enough_data | partial - packets have segment growth in some rows, not family-level monthly history |
| fetch_stage | packet-generation time |
| recommended_packet_shape | reuse `reference_baseline.monthly_sales`; derive `analysis.sales_trend.{amazon|shopify}` during scoring |
| recommended_scoring_formula | compute 12 month-over-month YoY revenue deltas, average them, then percentile-rank across all families in the same channel |
| recommended_thresholds | bottom 10%=1, P10-25=3, P25-50=5, P50-75=7, P75-90=9, top 10%=10 |
| risks | needs 24 months of data and ideally batch-wide ranking; if only a subset is available, use absolute thresholds as fallback |

## Q13 GMC Visibility

| field | value |
|---|---|
| question_id | Q13 |
| ideal_source_system | Amazon Brand Analytics keyword data |
| fallback_source_system | Stackline `gold_traffic` segment traffic |
| exact_data_fields_needed | ideal: keyword search volume, keyword rank, segment revenue share. fallback: `total_traffic` and traffic trend from Stackline |
| grain_of_data_needed | ideal: keyword x ASIN x segment. fallback: segment level |
| current_packets_contain_enough_data | partial - Stackline rows have segment traffic, rows without Stackline have nothing |
| fetch_stage | Stackline proxy at packet-generation time; Brand Analytics is a separate ingestion path |
| recommended_packet_shape | `reference_baseline.gmc_visibility` or analysis-level `gmc_visibility = {source, segment_traffic_52w, traffic_percentile_vs_all_segments, score, capped}` |
| recommended_scoring_formula | percentile-rank segment 52-week traffic against all available Stackline segments, cap at 9 |
| recommended_thresholds | bottom 10%=2, P10-25=3, P25-50=5, P50-75=7, top 10%=9 |
| risks | this is a segment popularity proxy, not true Sunco keyword visibility; rows without Stackline should remain N/A and redistribute weight |

## Shared Implementation Notes

- One monthly query can feed Q2 and Q4.
- One customer-grain query can feed Q3.
- Q4 percentile scoring is a batch operation across families in the same channel.
- Channel IDs are currently Amazon `11929` and Shopify/Sunco.com `12585`.
- When Postgres is unavailable, the FY2025 local Amazon sales export can be used as a proxy fallback for monthly Amazon rows only. That supports partial `Q2` coverage and limited `Q4` prep, but it does not provide customer concentration for `Q3` and should remain a lower-confidence source.
