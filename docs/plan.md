---
planStatus:
  planId: plan-prd-research-tool
  title: "PRD Research Tool - Ideation Template + Competitive Research Engine"
  status: draft
  planType: feature
  priority: high
  owner: Manny Hernandez
  stakeholders: [Jesse Harper, Stephanie Barrera, Nelson Chu]
  tags: [tool-building, prd, competitive-research, category-agnostic]
  created: "2026-04-10"
  updated: "2026-04-10T00:00:00.000Z"
  progress: 0
---

# PRD Research Tool - Ideation Template + Competitive Research Engine

## Problem Statement

**Goal:** Build a two-part system:
1. A **category-agnostic Excel template** that any PM fills out with a product ideation
2. A **competitive research engine** that takes the filled template and generates a research report with pricing targets, spec recommendations, and top competitors

**Current State:** PMs manually research competitors on a per-product basis. No standardized input format for ideations across categories. Research findings are not structured to feed into the PRD Generator.

**Desired State:** PM fills out a single Excel template → runs tool → gets a research report that directly informs PRD Generator input.

---

## System Architecture

```
SharePoint: Manny Tools/
├── PRD Research Template.xlsx        ← USER FILLS THIS (one row per ideation)
├── Research Reports/                 ← TOOL OUTPUTS HERE
│   └── [Category] Research [Date].md (or .html)
└── PRD Generator Template.xlsx       ← EXISTING (user fills next, informed by research)
```

**Flow:**
1. PM has an ideation → fills out PRD Research Template.xlsx
2. PM asks Claude: "run research for [category]"
3. Tool reads the template, looks up the Sunco Reference SKU for attribute data
4. Tool researches competitors (Competitors.md list + Amazon/Home Depot/Walmart)
5. Tool generates research report with:
   - Top 5-10 competitors (Amazon vs brick-and-mortar split)
   - Competitive pricing targets (recommended MSRP)
   - Spec recommendations ("increase lumens +15% to beat 8/10 competitors")
   - Summary formatted to easily fill in PRD Generator Template
6. PM reviews report → fills in PRD Generator Template → generates PRDs

---

## Part A: Excel Template Design

### Design Principles
- **Category-agnostic:** One template works for Panels, Emergency, Vaportights, Lamps, Chandeliers, etc.
- **Not every field is required:** PM fills what they know; tool uses Reference SKU to fill gaps
- **Reference SKU drives attribute lookup:** The Sunco or internal vendor SKU tells us what product family this ideation is based on

### Proposed Template Columns

#### Section 1: Ideation Identity
| Column | Description | Required? |
|--------|-------------|-----------|
| Category | Main category (Indoor Commercial, Industrial, Indoor Residential) | Yes |
| Subcategory | Subcategory (Panels, Emergency, Vaportights, Lamps, etc.) | Yes |
| Ideation Name | Descriptive name for the product concept | Yes |
| Sunco Reference SKU | Existing Sunco or internal vendor SKU this is based on | Yes |
| Reference SKU Source | "Sunco Branded" or "Internal Vendor" | Yes |
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
| Dimming Type | 0-10V, Triac, ELV, Phase-cut, etc. | If dimmable |

#### Section 3: Physical / Mechanical Specs
| Column | Description | Applies To |
|--------|-------------|------------|
| Size / Form Factor | e.g., 2x4, 4ft, 6", Round, A19 | All |
| Mounting Type | Recessed, Surface, Pendant, Troffer, etc. | All |
| Material | Steel, Aluminum, Polycarbonate, etc. | All |
| Finish / Color | White, Black, Brushed Nickel, etc. | All |
| IP Rating | IP20, IP65, etc. | Industrial, Outdoor |
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
| Bulb Base Type | E26, GU24, Integrated, etc. | Bulbs, Lamps |
| Bulb Shape | A19, BR30, PAR38, etc. | Bulbs |
| Additional Features | Free text for anything not covered | All |

#### Section 5: Business Targets
| Column | Description | Required? |
|--------|-------------|-----------|
| Target MSRP | What we want to sell it for | Optional |
| Target Margin % (Shopify) | Default 60% | Optional |
| Target Margin % (Amazon) | Default 70% | Optional |
| Target Vendor Cost | Max we'd pay supplier | Optional |
| Certifications Needed | UL, DLC, Energy Star, ETL, etc. | Yes |
| Lifetime Hours | Target rated life | Optional |
| Warranty (Years) | Target warranty period | Optional |

#### Section 6: Research Guidance
| Column | Description | Required? |
|--------|-------------|-----------|
| Known Competitors | Specific competitor products to compare against | Optional |
| Priority Channels | Amazon, Home Depot, Walmart, Direct — which matter most | Optional |
| Research Notes | Any context for the research tool | Optional |

### Reference SKU Lookup Logic
When the tool reads the Reference SKU, it will:
1. Look up in `SUNCO ALL METADATA.csv` → pull specs (wattage, CCT, CRI, brightness, images, etc.)
2. Look up in `SUNCO ALL SPECS REFERENCE.csv` → pull detailed specs (voltage, dimming, IP, material, etc.)
3. Look up in `SUNCO 2025 ALL SALES Shopify - Categorized.csv` → pull sales performance data
4. Look up in `SKU Decoder - Manny Version.csv` → decode SKU components (size, features, color, CCT)
5. Fill in any blank template fields with reference SKU data (user-entered values take priority)

---

## Part B: Competitive Research Engine

### Research Sources (Priority Order)
1. **Known Competitors** (from Competitors.md): Duralec, Amico Light, NuWatt, Maxxima, 1000Bulbs, NSL (National Specialty Lighting USA)
2. **Amazon** — search for products matching the ideation specs
3. **Home Depot** — search product catalog
4. **Walmart** — search product catalog
5. **Lowe's** — search product catalog
6. **Other distributors** — as discovered

### Research Outputs

#### 1. Competitor Product Matrix
| Field | Description |
|-------|-------------|
| Competitor Name | Brand/seller |
| Channel | Amazon / Home Depot / Walmart / Direct / Distributor |
| Product Name | Full product title |
| Product URL | Link to listing |
| Price | Listed price |
| Wattage | Spec comparison |
| Lumens | Spec comparison |
| CCT Options | Spec comparison |
| CRI | Spec comparison |
| Dimmable | Yes/No |
| Special Features | Emergency, smart, motion, etc. |
| Rating | Customer review rating |
| Review Count | Number of reviews |

Split into two tables:
- **Amazon Competitors (Top 5-10)**
- **Brick-and-Mortar / Direct Competitors (Top 5-10)**

#### 2. Pricing Analysis
- Competitive price range (min / median / max)
- Recommended MSRP to be competitive
- Price-per-watt comparison
- Price-per-lumen comparison
- Where the ideation product would rank in the price spectrum

#### 3. Spec Recommendations
For each attribute, analyze the competitive landscape and recommend:
- **Wattage:** "Competitors range 30-60W. Your 40W is mid-pack. At 50W you'd beat 7/10 competitors on output."
- **Lumens:** "Average competitor output is 5,200 lm. Your target of 5,500 lm puts you top 30%."
- **CRI:** "Only 2/10 competitors offer CRI 90+. CRI 90 would differentiate."
- **CCT:** "Most competitors offer 3-5 CCT options. Your 3 options is standard."
- **Features:** "3/10 competitors offer emergency backup. Adding this fills a gap."
- Apply this pattern across ALL relevant attributes

#### 4. PRD Generator Summary
A pre-formatted section that maps research findings to PRD Generator columns:
```
Recommended PRD Generator Input:
  Name: [Ideation Name]
  Voltage: [From research or reference]
  Wattage_1-5: [Recommended based on competitive analysis]
  CCT_1-5: [Recommended based on competitive analysis]
  CRI: [Recommended]
  Dimming: [Recommended]
  Moisture Rating: [From reference or research]
  ...etc
```

---

## Implementation Steps (Incremental, Git-Checkpointed)

### Step 1: Attribute Research & Template Design
**Goal:** Finalize the exact columns for the Excel template
- [ ] Cross-reference all 42 subcategories against the metadata/specs CSVs to confirm which attributes apply to which categories
- [ ] Validate template columns cover edge cases (bulbs need base type, fans need CFM, grow lights need PPF, etc.)
- [ ] Create the Excel template file with formatting, dropdowns, and validation
- [ ] **Git checkpoint: "Step 1 - Ideation template created"**

### Step 2: Reference SKU Lookup Module
**Goal:** Script that reads a Reference SKU and returns all known attributes
- [ ] Build lookup against metadata CSV, specs reference CSV, sales CSV, SKU decoder
- [ ] Handle both Sunco-branded and internal vendor SKUs
- [ ] Output: dictionary of all known attributes for the SKU
- [ ] Test with 3-5 SKUs across different categories
- [ ] **Git checkpoint: "Step 2 - Reference SKU lookup working"**

### Step 3: Template Parser
**Goal:** Script that reads a filled Excel template and merges with Reference SKU data
- [ ] Read Excel rows (one ideation per row)
- [ ] For each row: look up Reference SKU, merge user-entered + lookup data
- [ ] Identify which fields are still missing (need research)
- [ ] Output: enriched ideation data structure ready for research
- [ ] **Git checkpoint: "Step 3 - Template parser working"**

### Step 4: Competitive Research Engine (Claude + WebFetch)
**Goal:** For each ideation, research competitors and generate findings
- [ ] Use WebFetch to search competitor sites from Competitors.md
- [ ] Use WebSearch to find Amazon/Home Depot/Walmart competitors
- [ ] Collect: pricing, specs, ratings, review counts
- [ ] Split results: Amazon competitors vs brick-and-mortar competitors
- [ ] **Git checkpoint: "Step 4 - Research engine working"**

### Step 5: Analysis & Recommendations Generator
**Goal:** Turn raw competitor data into actionable recommendations
- [ ] Calculate pricing targets (competitive MSRP)
- [ ] Generate per-attribute recommendations ("increase X by Y% to beat Z competitors")
- [ ] Rank competitors by relevance
- [ ] Generate PRD Generator summary section
- [ ] **Git checkpoint: "Step 5 - Analysis engine working"**

### Step 6: Report Generator & Skill Definition
**Goal:** Produce final output and create the skill trigger
- [ ] Generate Markdown/HTML research report
- [ ] Save to SharePoint output folder
- [ ] Create SKILL.md definition (matching PRD Generator pattern)
- [ ] Test end-to-end with a real ideation
- [ ] **Git checkpoint: "Step 6 - Full tool working"**

---

## Open Questions

1. **Template format:** Excel (.xlsx) with dropdowns/validation, or simpler CSV? (Leaning Excel for better UX with dropdowns)
2. **Multiple ideations per run?** Can a user put 5 ideations in one template and run research for all at once?
3. **Research depth vs speed:** WebFetch for each competitor × each ideation could be slow. Should we limit to top 3 competitors per ideation, or go wide?
4. **Output format:** Markdown report? HTML report? Both?
5. **How should "Internal Vendor" SKUs be identified?** By the NSL prefixes (PRE-, SP-, LO-, etc.) or does the user explicitly mark them?

---

## Dependencies

- Existing resources: SUNCO ALL METADATA.csv, SUNCO ALL SPECS REFERENCE.csv, sales CSV, SKU Decoder
- Existing tools: PRD Generator skill (output feeds into this)
- WebFetch/WebSearch for competitor research
- Competitors.md for known competitor list

---

## Success Criteria

- [ ] PM can fill out one Excel template for any category
- [ ] Tool generates competitive research report in < 5 minutes
- [ ] Report clearly shows top 5-10 competitors split by channel
- [ ] Pricing recommendation is within 10% of manual research
- [ ] Spec recommendations are actionable ("increase X by Y%")
- [ ] PRD Generator summary is copy-pasteable into PRD template
