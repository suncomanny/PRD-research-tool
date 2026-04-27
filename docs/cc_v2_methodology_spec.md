# CC V2 Scoring Methodology - Implementation Spec

---

## 1. Pillars and Weights

| pillar_id | pillar_name | default_weight | question_count_amazon | question_count_sunco |
|-----------|-------------|---------------:|----------------------:|---------------------:|
| SALES | Sales | 35 | 4 | 4 |
| COMP | Competition | 30 | 8 | 8 |
| MARKET | Market | 15 | 2 | 2 |
| LOG | Logistics | 20 | 3 | 3 |
| **Total** | | **100** | **17** | **17** |

Amazon active max: 17 questions. Sunco.com active max: 14 (Q7, Q8, Q9 are permanently N/A).

---

## 2. Scoring Questions

| question_id | pillar | question_name | applies_to_channel | applies_to_family_state | earliest_gate_active | latest_gate_active | source_type | controllability | scoring_logic_summary | scoring_tiers_summary | important_caveats |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Q1 | SALES | Volume | both | known | G1 | G5 | direct | market_context | 12mo revenue per family per channel from `tb_sales`. SUM(order_value_item). | >=$500K->10, >=$300K->9, >=$100K->8, >=$50K->6, >=$10K->4, >=$5K->3, else->1 | New families: N/A (greyed). No historical sales to score. |
| Q2 | SALES | Demand Consistency | both | known | G1 | G5 | direct | market_context | Count of distinct months with orders / 12. CoV alternative under evaluation. | 11-12mo->10, 9-10->8, 7-8->6, 5-6->4, <5->2 | CoV approach may replace this. Current method rewards simply being in-stock and listed. |
| Q3 | SALES | Bulk Buyer Analysis | both | known | G1 | G5 | direct | market_context | Pareto: top 20% customers' share of revenue + repeat rate. Channel-inverted scoring. | Thresholds TBD from data. Amazon: distributed=high score. Sunco.com: concentrated=high score (with volume guard). | Scoring is inverted between channels. Amazon high-concentration = risk. Sunco.com high-concentration = positive (contractor accounts). Volume + customer count guard prevents single-buyer families from scoring high. |
| Q4 | SALES | Sales Trend | both | known | G1 | G5 | direct | market_context | Month-by-month YoY averaged across 12 months. Uses 24mo of data. Seasonality cancels out. | Percentile-based per channel distribution: Bot 10%->1, P25-50->5, P75-90->9, Top 10%->10 | Percentile rank is relative to the channel's own family distribution, not absolute growth. |
| Q5 | COMP | Brand Count | both | both | G1 (amz) / G2 (sunco) | G5 | direct (amz) / proxy (sunco) | market_context | Count of brands in segment (Pareto 80%, 52w). Non-linear sweet spot scoring. | 0-2->2, 3-8->8-10, 9-15->6, 20+->4-6. Thresholds TBD. | Amazon: from `gold_sales` (Stackline). Sunco.com: proxy from 1000Bulbs + SBL competitor SKU count. Non-linear because monopoly (0-2) and gold rush (20+) are both bad. |
| Q6 | COMP | Price Position | both | both | G1 (amz) / G2 (sunco) | G5 | direct (amz) / proxy (sunco) | reference_anchor | Sunco price vs segment/portal median, revenue-weighted. | Below median->high score, above median->low score. Thresholds TBD. | Amazon: `sunco_pilot` price vs `gold_sales` segment median. Sunco.com: Sunco price vs trade portal competitor pricing. |
| Q7 | COMP | Review Strength | amazon | both | G1 (known) / G5 (new) | G5 | direct | post_launch_only (new) | Family rating + review count vs segment benchmark, revenue-weighted. | Revenue-weighted vs benchmark. Thresholds TBD. | Amazon-only. Sunco.com: permanently N/A (trade portals have no review data). New families: only scores at G5 post-launch. |
| Q8 | COMP | BSR Rank | amazon | both | G1 (known) / G5 (new) | G5 | direct | post_launch_only (new) | Family BSR percentile within segment, revenue-weighted. | Top 10%->10, Top 25%->8, Top 50%->6, Bot 50%->2-4 | Amazon-only. Sunco.com: permanently N/A. New families: only scores at G5. Source: `redshift_bsr` + `gold_sales`. |
| Q9 | COMP | TACoS | amazon | both | G1 (known) / G5 (new) | G5 | direct | controllable_ideation (known) / post_launch_only (new) | Ad cost intensity - how expensive to compete. | Lower TACoS->higher score. Thresholds TBD. | Amazon-only. Sunco.com: permanently N/A. Source: `sunco_pilot.tacos_4w`. Known families can action on this (adjust ad spend). New families: post-launch only. |
| Q10 | COMP | Competitor Growth Rate | both | both | G1 (amz) / G2 (sunco) | G5 | direct (amz) / proxy (sunco) | market_context | New brands/SKUs entering segment over 13 weeks. | Stable->8, 2-3 new->6, 4-6->4, 7+->2 (gold rush). Thresholds TBD. | Amazon: `gold_sales` historical brand entry rate. Sunco.com: 1000Bulbs + SBL new SKU listings. |
| Q11 | COMP | Segment Traffic | both | both | G1 (amz) / G4 (sunco) | G5 | direct (amz) / proxy (sunco) | market_context | 52w segment traffic (organic + paid), percentile rank. | Bot 10%->2, P25-50->6, P50-75->8, Top 10%->10 | Amazon: `gold_traffic` (Stackline). Sunco.com: SEMrush / analytics, available Gate 4+. |
| Q12 | COMP | Demand Persistence | both | both | G1 (amz) / G2 (sunco) | G5 | proxy | market_context | Traffic trend + BSR/velocity persistence. Proxy signal, capped. | Proxy: Traffic trend + persistence. Thresholds from data. Capped score. | This is explicitly a PROXY, not a direct measure. Score is capped (cannot reach 10). Amazon: `gold_traffic` trend + `redshift_bsr` 3mo stability. Sunco.com: trade portal velocity persistence. |
| Q13 | MARKET | GMC Visibility | both | both | G1 | G5 | proxy | market_context | Keyword volume percentile within segment, revenue-weighted. Proxy for actual GMC. Capped at 9. | Bot 10%->2, P25-50->5, P50-75->7, Top 10%->9 (cap) | PROXY - capped at 9 until real GMC data arrives. Source: `brand_analytics_keywords`. Same source for both channels currently. |
| Q14 | MARKET | Target Market Fit | both | both | G1 (amz) / G2 (sunco) | G5 | direct (amz) / proxy (sunco) | market_context | Does our product cover this segment's demand for this channel's buyer base? Channel-aware at the source. | Data-informed fixed thresholds. Separate calibration per channel. Annual refresh. | Amazon: keyword overlap (Sunco ASIN keywords intersect segment top-N revenue keywords) from `brand_analytics_keywords`. Sunco.com: velocity-weighted product overlap (Sunco catalog intersect trade portal top SKUs) from 1000Bulbs + SBL. |
| Q15 | LOG | Initial Investment | both | both | G3 | G5 | direct | controllable_ideation | MAX(MOQ cost, 150-DOI x velocity x unit cost). | <$1K->10, <$5K->8, <$10K->6, <$20K->4, <$50K->2, >=$50K->0 | Lower capital = higher score. Requires vendor quotes / target pricing (Gate 3+). Source: `sunco_pilot` (MOQ, cost) + `tb_sales` (velocity). |
| Q16 | LOG | Fulfillment Viability | both | both | G3 | G5 | direct | controllable_ideation | Can we scale profitably on this channel? Channel-specific margin tiers. | Amazon: FBA >15%->10, 5-15%->7, <5%->4; FBM >15%->6, <15%->3. Sunco.com: >20%->10, 10-20%->7, <10%->4. | Different tier structures per channel. Amazon splits FBA vs FBM. Sunco.com uses single channel margin. Sub-signal split deferred. Source: `sunco_pilot` margins. |
| Q17 | LOG | Inventory Health | both | known | G5 | G5 | direct | post_launch_only | Actual DOI vs 150-day target. Post-launch only. | 120-180d->10 (on target), 90-120/180-240->7, 60-90/240+->4, <60d->2 (critical) | Post-launch only - Gates 1-4: greyed out. Source: `vw_ibl_doi`. New families: N/A until inventory exists. |

**Not scored (metadata only):** Tariff rate - not a scored question. Stored as metadata in rubric and feeds into ECV margin calculation via `sunco_pilot.tariff_rate`.

---

## 3. Gate Activation Table

| gate | purpose | amazon_known | amazon_new | sunco_known | sunco_new | what_unlocks |
|------|---------|-------------:|-----------:|------------:|----------:|-------------|
| G1 | Discovery | 15 | 7 | 5 | 1 | PM assigns Stackline segment (Amazon) + maps trade portal category (Sunco.com). Sales + Competition + Market light up. |
| G2 | Market Analysis | 15 | 7 | 11 | 6 | Trade portal scrape data mapped to families. Sunco.com Competition + Market light up. Amazon unchanged. |
| G3 | Investment Validation | 16 | 9 | 12 | 8 | Vendor quotes / target pricing available. Fulfillment Viability + refined 150-DOI Investment activate. |
| G4 | Monitoring Setup | 16 | 9 | 13 | 9 | SEMrush / Sunco.com analytics wired. Segment Traffic activates for Sunco.com. Costs finalized. |
| G5 | Launch / Post-Launch | 17 | 17 | 14 | 14 | All questions active. Inventory Health + post-launch signals (Reviews, BSR, TACoS) for new families. |

---

## 4. Weight Redistribution Rules

1. **Inactive questions within a pillar** -> their weight redistributes equally to the remaining active questions in the same pillar.
2. **Entirely inactive pillar** (all questions N/A) -> that pillar's weight redistributes proportionally to the other active pillars.
3. **Pillar score** = average of active question scores within that pillar.
4. **Weighted score** = SUM(pillar_avg x pillar_weight) across all active pillars, normalized to 0-10.
5. **Permanently N/A questions** (Q7 Review Strength, Q8 BSR Rank, Q9 TACoS on Sunco.com) are never counted - their pillar weight is always redistributed for Sunco.com scoring.

---

## 5. Questions NOT Usable at Early Ideation Gates (G1-G2)

| question_id | question_name | reason | earliest_usable |
|-------------|---------------|--------|----------------|
| Q15 | Initial Investment | Requires vendor quotes / target pricing | G3 |
| Q16 | Fulfillment Viability | Requires vendor cost + margin data | G3 |
| Q17 | Inventory Health | Post-launch only - no inventory exists | G5 |
| Q7 (new families) | Review Strength | New families have no reviews yet | G5 |
| Q8 (new families) | BSR Rank | New families have no BSR yet | G5 |
| Q9 (new families) | TACoS | New families have no ad history | G5 |
| Q11 (sunco) | Segment Traffic | Sunco.com analytics not wired until G4 | G4 |

**For the PRD Research Tool at Gate 0/1 ideation:** Only Q1-Q4 (Sales, known only), Q5-Q6 (Competition partial), Q10-Q14 (Competition/Market partial) are available on Amazon. Sunco.com has almost nothing at G1 for new families (1 question active).

---

## 6. Questions Most Useful for Vendor Optimization Decisions

| question_id | question_name | why_useful_for_vendor_decisions |
|-------------|---------------|-------------------------------|
| Q6 | Price Position | Tells you where your price sits vs competitors - directly drives target vendor cost negotiation. |
| Q15 | Initial Investment | MOQ x cost sizing - compares vendors on capital commitment. |
| Q16 | Fulfillment Viability | Margin by channel - determines whether vendor cost supports profitable fulfillment. |
| Q9 | TACoS | Ad cost intensity - if TACoS is high, vendor cost must be lower to preserve margin. |
| Q1 | Volume | Revenue baseline - validates whether volume justifies vendor MOQ commitment. |

---

## 7. What the Score Means

The CC V2 weighted score (0-10) is a **prioritization signal** for the product family within a specific channel. It answers: "Given what we know at this gate, how strong is this family's competitive position and business viability on this channel?"

A high score means the family has strong sales history (or market opportunity for new), reasonable competitive dynamics, market demand alignment, and feasible logistics/margins. It should be prioritized for investment, listing optimization, or new product development.

A low score means one or more pillars are weak - saturated competition, declining sales, poor margins, or insufficient market signal. It needs investigation before further investment.

The score is **gate-aware**: it only uses data available at the current gate. As families progress through gates and more data unlocks, the score becomes more complete and reliable.

---

## 8. What the Score Does NOT Mean

The score is **not a launch/kill decision**. It is a ranking input, not a binary threshold. A score of 5.0 does not mean "don't launch" - it means "other families ranked higher given current evidence."

The score is **not comparable across channels**. An 8.0 on Amazon and a 6.0 on Sunco.com do not mean Amazon is better - different question counts, different data sources, different scoring inversions (for example, Q3 Bulk Buyers).

The score is **not stable across gates**. A family scoring 7.5 at G1 may score 5.0 at G3 when vendor costs reveal poor margins. This is by design - the score improves in accuracy as data arrives.

The score **does not account for strategic intent**. A family with a low score might still be worth pursuing for portfolio coverage, brand positioning, or retailer relationship reasons. Those decisions belong to the PM, not the rubric.

The score **does not replace sampling/validation**. The weighted score powers backend ranking but is hidden from evaluators until sampling validates that the weights produce sensible orderings. Evaluators see unweighted per-question scores.
