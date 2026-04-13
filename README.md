# PRD Research Tool

Sunco Lighting — Category-agnostic ideation template and competitive research engine.

## What This Does

1. **Ideation Template** (Excel) — PM fills out product ideation details for any lighting category
2. **Competitive Research Engine** — Researches competitors, pricing, and specs across Amazon + brick-and-mortar
3. **Research Report** — Generates actionable report formatted to feed into the PRD Generator

## Workflow

```
PM fills template → Runs research tool → Gets competitive report → Fills PRD Generator template
```

## Project Structure

```
├── templates/          # Excel template files
├── src/                # Tool source code
│   ├── lookup/         # Reference SKU lookup modules
│   ├── research/       # Competitive research engine
│   └── report/         # Report generation
├── output/             # Generated reports (gitignored)
├── docs/               # Plans and documentation
└── tests/              # Test scripts
```

## Setup

```bash
npm install
```

## Dependencies

- Node.js 18+
- npm packages: xlsx, docx, node-fetch
