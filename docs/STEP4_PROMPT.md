# STEP4_PROMPT.md — Competitive Collection (1 Row x 1 Channel)

## Assignment

You are collecting raw competitor data for exactly **one row** on exactly **one channel**.

- **Session root:** `<SESSION_ROOT>`
- **Row:** `<ROW_NUMBER>`
- **Channel:** `<CHANNEL>` (`amazon`, `brick_and_mortar`, or `brand_sites`)
- **Output file:** `<OUTPUT_FILE>`

## Steps

1. **Read the packet.**
   - `<SESSION_ROOT>/packets/row_<ROW_NUMBER>_packet.json`
   - Extract:
     - `identity.ideation_name`
     - `target_profile.electrical`
     - `target_profile.physical`
     - `research_plan`
   - From `research_plan`, use:
     - `target_price_band`
     - `brand_watchlist`
     - `must_validate`
     - `collection_targets`
     - channel-specific queries:
       - `research_plan.amazon.queries`
       - `research_plan.brick_and_mortar.home_depot.queries`
       - `research_plan.brick_and_mortar.walmart.queries`
       - `research_plan.brick_and_mortar.lowes.queries`
       - `research_plan.known_competitor_brands[*].queries`

2. **Read the existing output scaffold first.**
   - If `artifact_status` is `complete` or `blocked`, stop and do nothing.
   - If `artifact_status` is `in_progress`, continue from the existing `items` and `queries_used`.
   - If `artifact_status` is `not_started`, proceed normally.

3. **Run channel-specific collection.**
   - **amazon**
     - Use `research_plan.amazon.queries`.
     - Rely on WebSearch snippets only.
     - Target: `research_plan.collection_targets.amazon_min_results` items when possible.
   - **brick_and_mortar**
     - Use the queries under `research_plan.brick_and_mortar`.
     - Search Home Depot, Walmart, and Lowe's separately.
     - Use WebSearch snippets only.
     - Each item must use `source_channel` = `home_depot`, `walmart`, or `lowes`.
     - Target: `research_plan.collection_targets.brick_and_mortar_min_results` items total.
   - **brand_sites**
     - Use `research_plan.known_competitor_brands[*].queries` and `research_plan.brand_watchlist`.
     - WebFetch product pages when reachable.
     - Each item must use `source_channel` = `brand_site`.
     - Set `source_domain` to the real domain.
     - Target: `research_plan.collection_targets.brand_site_min_results` items when possible.

4. **Extract raw competitor-result objects only.**
   - Follow `schemas/competitor-result.schema.json`.
    - Required fields:
      - `source_channel`
      - `collection_method`
      - `brand`
      - `product_title`
      - `url`
    - `brand` must never be `null` or empty. If the source does not disclose a manufacturer, use `Generic / Unbranded`.
   - Best-effort fields may be `null` if unavailable:
     - `price`, `wattage`, `lumens`, `cct`, `cri`, `voltage`
     - `dimmable`, `dimming_type`
     - `certifications`, `features`
     - `rating`, `review_count`
     - `match_confidence`, `match_notes`, `extraction_notes`
   - Use `research_plan.must_validate` to describe confirmed, missing, or contradictory specs in `match_notes`.
   - Do not normalize or infer hidden values. If the source does not show it, set it to `null`.

5. **Write exactly one raw artifact file.**
   - Follow `schemas/collection-artifact.schema.json`.
   - Overwrite `<OUTPUT_FILE>` completely.
   - Set:
     - `artifact_status`
     - `queries_used`
     - `items`
     - `notes`
     - `blocking_issues`
     - `updated_at`

## Failure Rules

| Situation | Action |
|-----------|--------|
| Retailer blocks WebFetch with 403 / CAPTCHA | Use WebSearch snippets only. Add `extraction_notes` on affected items. Still mark the artifact `complete`. |
| Zero useful results | Set `artifact_status` to `complete`, leave `items` empty, and explain what was searched in `notes`. |
| Weak match | Include it only if `match_confidence < 0.6` is justified in `match_notes`. Prefer omission over pollution. |
| Token budget is running out | Set `artifact_status` to `in_progress` and write the partial artifact before stopping. |
| Entire channel is unreachable and no results exist | Set `artifact_status` to `blocked` and populate `blocking_issues`. |

## Scope Rules — Do NOT

- Do not edit `manifest.json`.
- Do not touch any row other than `<ROW_NUMBER>`.
- Do not touch any channel other than `<CHANNEL>`.
- Do not read or edit `normalized/`, `analysis/`, or `reports/`.
- Do not normalize, deduplicate, rank, or analyze the results.
- Do not generate recommendations or pricing targets.
- Do not fabricate specs.

## Finish

After writing `<OUTPUT_FILE>`, stop. Do not update any other files.
