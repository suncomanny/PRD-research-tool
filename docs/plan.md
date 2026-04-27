---
planStatus:
  planId: plan-prd-research-tool
  title: "PRD Research Tool - Ideation Template + Competitive Research Engine"
  status: in-development
  planType: feature
  priority: high
  owner: Manny Hernandez
  stakeholders: [Jesse Harper, Stephanie Barrera, Nelson Chu]
  tags: [tool-building, prd, competitive-research, category-agnostic]
  created: "2026-04-10"
  updated: "2026-04-27T14:45:00.000Z"
  progress: 10
---

# PRD Research Tool - Ideation Template + Competitive Research Engine

## Finalized Decisions

| Decision | Answer |
|----------|--------|
| **Input format** | Excel (.xlsx) with dropdowns/validation — lives on SharePoint |
| **Multiple ideations** | Yes, multiple rows per template, same category per batch |
| **Output format** | Excel (.xlsx) — **one sheet per ideation** with custom-tailored layout |
| **Output delivery** | SharePoint: `Manny Tools/Research Reports/` |
| **Vendor identification** | Redshift/Postgres MCP query (not CSV lookup) |
| **Reference anchor fallback** | If Postgres payloads are unavailable, use local metadata variant price plus local Shopify/Amazon sales exports as a clearly labeled fallback source |
| **Research depth** | All 6 known competitors + Amazon/Home Depot/Walmart — users accept wait time |
| **Repo** | `suncomanny/PRD-research-tool` (private, GitHub) |
| **Project location** | `Claude Workbook/PRD-research-tool/` (separate from main workspace) |
| **Runtime** | Node.js (matching PRD Generator pattern: `xlsx` + `docx` npm packages + SharePoint Graph API) |
| **Stackline integration** | Yes — PM exports from Atlas, renames per convention, uploads to SharePoint. Tool expects Stackline by default for Amazon/Home Depot market context, preserves retailer-scoped bundles side by side for channel comparison, and falls back to web collection if a matching segment file is missing |
| **Stackline local path** | `C:\Users\Sunco\Sunco Lighting\Product - Manny Tools\PRD Research\Stackline Data\` |
| **Stackline naming convention** | Preferred: `Stackline_[StacklineSegment]_[YYYY-MM]_[type].csv` (type = summary, traffic, or sales). Fallback: valid Stackline CSVs can still be discovered by schema, segment label, and retailer scope when teammates upload inconsistent filenames. |

---

## Problem Statement

**Goal:** Build a two-part system:
1. A **category-agnostic Excel template** that any PM fills out with product ideations
2. A **competitive research engine** that takes the filled template and generates an Excel workbook with one research sheet per ideation, containing pricing targets, spec recommendations, and top competitors

**Current State:** PMs manually research competitors per product. No standardized ideation input. Research findings don't feed into PRD Generator.

**Desired State:** PM fills Excel template → runs tool → gets research workbook → uses findings to fill PRD Generator Template → generates PRDs.

---

## System Architecture

```
SharePoint: Manny Tools/
├── PRD Research/
│   ├── PRD Research Template.xlsx           ← PM FILLS THIS (one row per ideation)
│   ├── Research Reports/
│   │   └── [Category] Research [Date].xlsx  ← TOOL OUTPUTS (one sheet per ideation)
│   └── Stackline Data/                      ← PM UPLOADS ATLAS EXPORTS HERE
│       ├── Stackline_Ceiling_Panels_2026-04_summary.csv
│       ├── Stackline_Ceiling_Panels_2026-04_traffic.csv
│       └── Stackline_Ceiling_Panels_2026-04_sales.csv
├── PRD Generator Template.xlsx              ← EXISTING (PM fills next, informed by research)
└── Generated PRDs/                          ← EXISTING PRD outputs

Local sync: C:\Users\Sunco\Sunco Lighting\Product - Manny Tools\
  (tool reads Stackline files from local sync — no Graph API needed for read)
```

**Flow:**
1. PM has ideation(s) → fills out PRD Research Template.xlsx (one row per product)
2. PM asks Claude: "run research for [category]"
3. Tool downloads template from SharePoint via Graph API
4. For each ideation row:
   a. Look up Sunco Reference SKU via Redshift/Postgres MCP → pull vendor, cost, specs
   b. Enrich with metadata/specs CSVs for any gaps
   c. Research competitors (Competitors.md + Amazon/HD/Walmart) via WebFetch/WebSearch
   d. Analyze: pricing targets, spec recommendations, competitor ranking
5. Tool generates Excel workbook (one sheet per ideation) with:
   - Ideation summary + reference SKU data
   - Amazon Competitors (Top 5-10)
   - Brick-and-Mortar Competitors (Top 5-10)
   - Pricing analysis (min/median/max, recommended MSRP, price-per-watt, price-per-lumen)
   - Spec recommendations ("increase lumens +15% to beat 8/10 competitors")
   - PRD Generator pre-fill summary
6. Tool uploads workbook to SharePoint `Research Reports/`
7. PM reviews → fills PRD Generator Template → generates PRDs

---

## Part A: Excel Input Template (PRD Research Template.xlsx)

### Design Principles
- **Category-agnostic:** One template for all 42 subcategories
- **Not every field required:** PM fills what they know; Reference SKU fills gaps
- **Reference SKU drives lookup:** Sunco or internal vendor SKU → DB query → auto-populate

### Template Columns

#### Section 1: Ideation Identity
| Column | Description | Required? |
|--------|-------------|-----------|
| Category | Main category (Indoor Commercial, Industrial, Indoor Residential) | Yes |
| Subcategory | e.g., Panels, Emergency, Vaportights, Lamps, Ceiling Fixtures | Yes |
| Ideation Name | Descriptive name for the product concept | Yes |
| Sunco Reference SKU | Existing Sunco or internal vendor SKU this is based on | Yes |
| Reference SKU Source | "Sunco Branded" or "Internal Vendor" (auto-detected via DB) | Auto |
| Strategy | New Product / Upgrade / Cost Reduction / Vendor Swap | Yes |

#### Section 2: Core Electrical Specs
| Column | Description | Applies To |
|--------|-------------|------------|
| Voltage | e.g., 120V, 120-277V, 12V | All |
| Wattage (Primary) | Main wattage or lowest selectable | All |
| Wattage (Max) | Highest selectable wattage (blank if fixed) | All |
| Selectable Wattage? | Yes/No | All |
| CCT (Primary) | Main color temp or lowest selectable | All |
| CCT (Max) | Highest selectable CCT (blank if fixed) | All |
| Selectable CCT? | Yes/No | All |
| CRI | Color Rendering Index target | All |
| Lumens (Target) | Target brightness | All |
| Dimmable? | Yes/No | All |
| Dimming Type | 0-10V, Triac, ELV, Phase-cut | If dimmable |

#### Section 3: Physical / Mechanical Specs
| Column | Description | Applies To |
|--------|-------------|------------|
| Size / Form Factor | e.g., 2x4, 4ft, 6", Round, A19 | All |
| Mounting Type | Recessed, Surface, Pendant, Troffer | All |
| Material | Steel, Aluminum, Polycarbonate | All |
| Finish / Color | White, Black, Brushed Nickel | All |
| IP Rating | IP20, IP65 | Industrial, Outdoor |
| Moisture Rating | Dry, Damp, Wet | All |
| Weight (lbs) | Target weight | Optional |

#### Section 4: Features & Special Requirements
| Column | Description | Applies To |
|--------|-------------|------------|
| Emergency/Battery Backup? | Yes/No | Commercial, Industrial |
| Motion Sensor? | Yes/No | Commercial, Industrial |
| Daylight Sensor / Auto-Dimming? | Yes/No | Commercial |
| Smart/Connected? | Yes/No | Residential, Commercial |
| Linkable? | Yes/No | Strips, Under Cabinet |
| Pull Chain? | Yes/No | Fans, some fixtures |
| Bulb Base Type | E26, GU24, Integrated | Bulbs, Lamps |
| Bulb Shape | A19, BR30, PAR38 | Bulbs |
| Additional Features | Free text | All |

#### Section 5: Business Targets
| Column | Description | Required? |
|--------|-------------|-----------|
| Target MSRP | What we want to sell it for | Optional |
| Target Margin % (Shopify) | Default 60% | Optional |
| Target Margin % (Amazon) | Default 70% | Optional |
| Target Vendor Cost | Max supplier cost | Optional |
| Certifications Needed | UL, DLC, Energy Star, ETL | Yes |
| Lifetime Hours | Target rated life | Optional |
| Warranty (Years) | Target warranty period | Optional |

#### Section 6: Research Guidance
| Column | Description | Required? |
|--------|-------------|-----------|
| Known Competitors | Specific competitor products to compare | Optional |
| Priority Channels | Amazon, Home Depot, Walmart, Direct | Optional |
| Stackline Data? | Yes/No — triggers lookup for Stackline Atlas CSV bundle for that subcategory/segment | Optional |
| Research Notes | Context for the research tool | Optional |

---

## Part B: Excel Output (Research Report Workbook)

### One Sheet Per Ideation — Layout

Each sheet is named after the ideation (e.g., "Panel 2x4 50W Selectable")

**Sheet Sections (top to bottom):**

#### Section A: Ideation Summary
- Product name, category, strategy, reference SKU
- All specs (user-entered + auto-filled from DB)
- Reference product sales data (revenue, units, margin)

#### Section B: Amazon Competitors (Top 5-10)
| Rank | Brand | Product | Price | Wattage | Lumens | CCT | CRI | Dimmable | Features | Rating | Reviews | URL |
|------|-------|---------|-------|---------|--------|-----|-----|----------|----------|--------|---------|-----|

#### Section C: Brick-and-Mortar / Direct Competitors (Top 5-10)
Same columns as above, sourced from Home Depot, Walmart, Lowe's, and known competitors (Duralec, Amico, NuWatt, Maxxima, 1000Bulbs, NSL USA)

#### Section D: Pricing Analysis
- Price range: min / median / max (Amazon vs B&M separately)
- Recommended MSRP
- Price-per-watt ranking
- Price-per-lumen ranking
- Where ideation would rank in price spectrum

#### Section E: Spec Recommendations
For each relevant attribute:
- Current ideation value vs competitive landscape
- Recommendation + rationale
- Impact assessment ("beats X of Y competitors")

#### Section F: PRD Generator Pre-Fill
Maps directly to PRD Generator Template columns — ready to copy/paste:
- Name, Voltage, Wattages, CCTs, CRI, Dimming, Certifications, etc.

---

## Implementation Steps (Incremental, Git-Checkpointed)

### Step 1: Attribute Research & Template Design
**Goal:** Finalize template columns and create the Excel input file
- [ ] Cross-reference all 42 subcategories against metadata/specs CSVs
- [ ] Validate template covers edge cases (bulbs: base type, fans: CFM, grow lights: PPF)
- [ ] Create PRD Research Template.xlsx with formatting, dropdowns, validation
- [ ] Git checkpoint: "Step 1 - Ideation template created"

### Step 2: Reference SKU Lightweight Lookup
**Goal:** Script that takes a Reference SKU and returns only what's needed — the Reference SKU is a *similar* existing product (inspiration), not the new product itself. We use it primarily as a category / feature-schema anchor and only secondarily as a light commercial sanity check. We only pull baseline context, not full specs.

**Fallback behavior:** Postgres MCP remains the preferred source for current listing price and last-12-month channel sales, but the tool can now fall back to local metadata + Shopify/Amazon sales exports when MCP payloads are unavailable. Fallback values must stay clearly labeled in the report.
- A batch helper now generates one Postgres MCP query bundle plus a merge-ready payload template for all unique reference SKUs in a workbook/session, so true DB enrichment can be applied in one rerun instead of by hand per row.
- The batch Postgres query helper now ranks exact SKU hits first and then falls back to family-level title/listing candidates so reference baselines are less likely to stay blank when only the SKU family exists in Postgres.
- When Postgres returns a listing price that materially diverges from the fallback metadata listing price, the tool now keeps the fallback value and records the rejected Postgres candidate as a note for QA instead of silently overriding the report baseline.
- A reference-baseline audit pass now classifies each unique reference SKU as `fully_trusted`, `trusted_with_fallback`, or `unresolved_manual_followup` and writes both JSON and Markdown QA artifacts into the session root.
- Reports now frame the reference SKU as a **reference anchor**: useful for category fit, expected feature schema, and Sunco-family context, but not the primary driver of final MSRP or feature-priority decisions.

**What we pull (and why):**
| Data | Source | Why |
|------|--------|-----|
| Product image URL | Metadata CSV (`Image Src`) | Placeholder image for the final PRD document (PM can swap later) |
| Current selling price | Postgres (`pricing_listingprice`) | Baseline: "our similar product sells at $X" |
| Shopify sales (12mo) | Postgres (channel 12585 — revenue, units) | DTC channel performance, always separate |
| Amazon sales (12mo) | Postgres (channel 11929 — revenue, units) | Marketplace channel performance, always separate |
| Product title + category | Metadata CSV | Validates the SKU and gives research engine search context |

**What we do NOT pull:** Full spec sheet, vendor cost/margin, cascading attribute extraction — PM already entered target specs on the template.

**Important:** Shopify and Amazon are different customer segments — always report them as separate line items, never combined.

**Tasks:**
- [ ] Read metadata CSV → match Reference SKU → return image URL, title, category
- [ ] Query Postgres for listing price + 12mo sales split by channel (Shopify + Amazon separately)
- [ ] Return simple object: `{ image_url, title, category, price, shopify_revenue, shopify_units, amazon_revenue, amazon_units }`
- [ ] Handle "SKU not found" gracefully (flag in output)
- [ ] Test with 3-5 SKUs across different categories
- [ ] Git checkpoint: "Step 2 - Reference SKU lookup working"

### Step 3: Template Parser + Enrichment
**Goal:** Script that reads filled Excel, attaches Reference SKU lookup data, and prepares ideation objects for research
- [ ] Read Excel rows (openpyxl / xlsx) → parse all columns per row
- [ ] For each row: run Step 2 lookup → attach image, price, sales context
- [ ] User-entered specs are the *target* specs (not overwritten by reference data)
- [ ] Output: list of enriched ideation objects ready for competitive research
- [ ] Git checkpoint: "Step 3 - Template parser working"

### Step 4: Competitive Research Engine
**Goal:** For each ideation, research competitors and collect data without making the workflow brittle or token-bound to a single model session
- [x] `4A` Packet generation: produce one ideation packet per row with target profile, pricing hypothesis, demand hypothesis, channel order, and evidence-to-collect prompts
- [x] `4B` Resumable workspace: initialize a shared artifact folder with row packets, schemas, placeholder raw/normalized/analysis files, Claude/Codex handoff instructions, and a manifest updater
- [x] `4C` Amazon collection: collect raw Amazon competitor candidates only
- [x] `4D` Brick-and-mortar collection: collect raw Home Depot / Walmart / Lowe's candidates only
- [x] `4E` Brand-site collection: collect raw candidates from Duralec, Amico, NuWatt, Maxxima, 1000Bulbs, NSL USA, and other direct competitors only
- [x] `4F` Normalization and dedupe: merge raw artifacts into one comparable competitor set, carry Stackline seeds forward as provisional candidates, and normalize pack/spec fields for analysis
- [x] `4G` Post-collection repair / price enrichment: salvage malformed raw artifacts and backfill missing prices from reachable product pages before analysis refresh
- [ ] **Shared schema / manifest contract:**
  - Raw collection files use a shared competitor-result schema so Claude and Codex can switch without re-explaining prior work
  - `manifest.json` is the source of truth for row status and next-step ownership
  - Claude is the preferred raw collector (`4C` / `4D` / `4E`) when its sessions are stable
  - Session instructions also expose a generic collector handoff so Codex can take over the same `1 row x 1 channel` task flow when Claude is unstable
  - Codex owns workspace maintenance, normalization, analysis, report generation, and malformed raw-artifact repair
  - Session refresh must tolerate an open/locked workbook by rebuilding from existing packet/artifact files when needed
  - Session tooling exposes `status`, `next-batch`, and raw artifact validation so Claude batches can be resumed without rereading prior chat
  - Session tooling also exposes raw-artifact repair so Codex can salvage partially successful collection runs instead of blocking on Claude retries
  - A shared `STEP4_PROMPT.md` template defines the `1 row x 1 channel` raw-collection workflow
- [ ] **Stackline integration:** Treat Stackline as the default Amazon / Home Depot market-intelligence layer unless a row explicitly opts out:
  - Read from local sync: `C:\Users\Sunco\Sunco Lighting\Product - Manny Tools\PRD Research\Stackline Data\`
  - Match files by Stackline segment label / alias: prefer `Stackline_[StacklineSegment]_*.csv`, but fall back to CSV schema sniffing when filenames are inconsistent
  - Pick newest by inferred YYYY-MM period; use filename when available, otherwise infer the period from export dates inside the CSV
  - Parse `_summary` CSV as the primary source for product-level revenue, units, price, brand share, and competitor ranking
  - Parse `_traffic` CSV as the supplemental source for total traffic and derived conversion rate
  - Treat `_sales` CSV as secondary / optional until its schema is confirmed across multiple segments
  - Preserve retailer scope (`amazon`, `home_depot`, etc.) so retailer-specific Stackline bundles are not treated like an all-retailer market view
  - Preserve multiple retailer-scoped bundles for the same subcategory so Amazon and Home Depot can be compared side by side instead of forcing one "winner" bundle
  - Merge into the ideation performance-estimation context, not just the raw competitor dump
  - Surface channel comparison snapshots in packets, analysis outputs, and reports so PMs can compare Amazon vs Home Depot price bands, sales, units, and Sunco share
  - If no matching Stackline bundle is found, mark the row as `web_fallback` and continue with web collection instead of failing
- [ ] Git checkpoint: "Step 4 - Research engine working"

### Step 5: Analysis & Recommendations Generator
**Goal:** Turn raw competitor data into actionable recommendations
- [x] `5A` Analysis artifacts: write resumable `analysis/row_###_analysis.json` files with pricing benchmarks, spec coverage, launch outlook, and provisional recommendations
- [x] Add a first-pass gate-readiness rubric layer for `G1` / `G2`, with explicit `N/A` handling, pillar redistribution, evidence confidence, and highest-impact vendor request outputs
- [x] Persist the CC V2 methodology extract in `docs/cc_v2_methodology_spec.md` and align early-gate activation rules so `Q15-Q17` stay out of `G1/G2` weighted scoring until their required inputs exist
- [x] Treat the question-level activation rules as runtime source of truth when they conflict with the deck's aggregate gate counts by one question
- [x] Add a `family_metrics_postgres_batch.py` helper plus `--family-metrics-json` packet merge path so Q2/Q3/Q4 monthly sales and customer-concentration data can be injected without blocking other scoring work
- [ ] `5B` Calculate pricing targets (MSRP, price-per-watt, price-per-lumen)
- [x] Add a targeted price-enrichment pass so later rows do not rely only on snippet-level price capture
- [ ] `5C` Generate per-attribute recommendations with impact scores and rank competitors by relevance
- [ ] Generate PRD Generator pre-fill section
- [ ] Git checkpoint: "Step 5 - Analysis engine working"

### Step 6: Excel Report Generator + Skill Definition
**Goal:** Produce final Excel workbook and create the skill trigger
- [x] `6A` Report artifacts: generate row-level `.xlsx` research report files from completed analysis artifacts
- [x] Build a combined workbook with a summary sheet plus one sheet per completed ideation
- [ ] Generate .xlsx with one sheet per ideation (sections A-F)
- [ ] Include Reference SKU image URL in PRD Generator pre-fill (Section F) — PM can swap before generating
- [ ] Style: brand colors, headers, conditional formatting
- [ ] Upload to SharePoint via Graph API
- [ ] Create SKILL.md definition (matching PRD Generator pattern)
- [ ] Test end-to-end with real ideations
- [ ] Git checkpoint: "Step 6 - Full tool working"

---

## Dependencies

- **Data:** Redshift/Postgres MCP, SUNCO ALL METADATA.csv, SUNCO ALL SPECS REFERENCE.csv, Stackline summary / traffic CSVs
- **Tools:** PRD Generator skill (downstream consumer), xlsx npm package, Microsoft Graph API
- **Research:** WebFetch, WebSearch, Competitors.md
- **Infrastructure:** SharePoint drive, Node.js runtime

---

## Success Criteria

- [ ] PM can fill out one Excel template for any of the 42 subcategories
- [ ] Tool generates research workbook with one sheet per ideation
- [ ] Each sheet has Amazon + brick-and-mortar competitor tables
- [ ] Pricing recommendation is within 10% of manual research
- [ ] Spec recommendations are actionable ("increase X by Y%")
- [ ] PRD Generator pre-fill section maps 1:1 to PRD Generator columns
