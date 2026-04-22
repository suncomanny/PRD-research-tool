# Step 4 Resumable Workflow

## Why This Split Exists

Step 4 is the most token-intensive part of the tool. The work is intentionally split so a collector and Codex can hand off through files instead of through chat memory.

## Step 4 Chunks

- `4A` Packet generation: build one ideation packet per row with search targets, pricing hypotheses, demand hypotheses, and channel order.
- `4B` Resumable workspace: initialize a shared session folder with packets, schemas, placeholders, instructions, and a manifest.
- `4C` Amazon collection: collect raw Amazon competitor candidates only.
- `4D` Brick-and-mortar collection: collect raw Home Depot, Walmart, and Lowe's competitor candidates only.
- `4E` Brand-site collection: collect raw candidates from direct competitor brand sites only.
- `4F` Normalization and dedupe: merge raw artifacts into a comparable competitor set.

## Ownership Split

- Claude is the preferred collector for `4C`, `4D`, and `4E` when its web session is stable.
- Codex owns `4A`, `4B`, `4F`, `5A`, `5B`, `5C`, and `6A`.
- Codex is also the fallback repair owner for malformed raw artifacts and can take over smaller raw-collection batches when Claude is unavailable.

## Handoff Rule

- A collector handles raw collection.
- Codex structures and analyzes.
- Both models must write through the session artifact files and refresh the manifest.
- If a raw artifact exists but fails schema validation, repair it instead of discarding it.

## Session Layout

```text
output/research_sessions/<session_name>/
|-- manifest.json
|-- instructions/
|   |-- COLLECTOR_NEXT.md
|   |-- CLAUDE_NEXT.md
|   |-- CODEX_NEXT.md
|   `-- STEP4_PROMPT.md
|-- schemas/
|   |-- collection-artifact.schema.json
|   |-- competitor-result.schema.json
|   |-- analysis-artifact.schema.json
|   `-- research-manifest.schema.json
|-- packets/
|   `-- row_###_packet.json
|-- raw/
|   |-- amazon/
|   |   `-- row_###_amazon_raw.json
|   |-- brick_and_mortar/
|   |   `-- row_###_brick_and_mortar_raw.json
|   `-- brand_sites/
|       `-- row_###_brand_sites_raw.json
|-- normalized/
|   `-- row_###_competitors_normalized.json
|-- analysis/
|   `-- row_###_analysis.json
`-- reports/
    `-- row_###_research_report.xlsx
```

## Commands

Initialize or refresh a session:

```powershell
python tools/research_session_manager.py init "C:\path\to\PRD_Research_Template.xlsx"
```

Update manifest status after editing artifacts:

```powershell
python tools/research_session_manager.py update "C:\path\to\output\research_sessions\<session_name>"
```

Show the session summary plus the next raw collection tasks:

```powershell
python tools/research_session_manager.py status "C:\path\to\output\research_sessions\<session_name>"
```

Return the next `N` raw collection tasks for the active collector:

```powershell
python tools/research_session_manager.py next-batch "C:\path\to\output\research_sessions\<session_name>" --limit 3
```

Validate raw collection artifacts against the shared contract:

```powershell
python tools/research_session_manager.py validate "C:\path\to\output\research_sessions\<session_name>" --rows 6,7
```

Repair common raw-artifact schema issues when a collection run partially succeeds:

```powershell
python tools/research_session_manager.py repair-raw "C:\path\to\output\research_sessions\<session_name>" --rows 8
```

If the workbook is open in Excel or locked by OneDrive, rerunning `init` now falls back to the existing packet/session files instead of failing, as long as the session already exists.

Normalize available raw artifacts into comparable competitor outputs:

```powershell
python tools/competitor_normalizer.py "C:\path\to\output\research_sessions\<session_name>"
```

Analyze normalized competitors into row-level pricing, spec, and performance guidance:

```powershell
python tools/competitive_analysis.py "C:\path\to\output\research_sessions\<session_name>"
```

Build row-level Excel report artifacts from completed analysis outputs:

```powershell
python tools/research_report_builder.py "C:\path\to\output\research_sessions\<session_name>"
```

Build a combined workbook with a summary sheet plus one sheet per completed ideation:

```powershell
python tools/research_report_builder.py "C:\path\to\output\research_sessions\<session_name>" --combined
```

Use `instructions/STEP4_PROMPT.md` as the one-task template for raw collection. `instructions/COLLECTOR_NEXT.md` is the generic handoff file when Claude is unstable or Codex is taking over. The unit of work is always `1 row x 1 channel`.

## Status Contract

- `not_started`: placeholder exists but collection or analysis has not begun.
- `in_progress`: someone is actively working the artifact.
- `complete`: artifact is ready for the next stage.
- `blocked`: the artifact cannot be completed without intervention.

## Required Behavior

- Raw collection files must stay raw. No dedupe or recommendation logic belongs there.
- Normalized files are the first place where products become comparable across channels.
- Analysis files are where pricing targets, spec gaps, and performance-estimation logic get applied.
- The manifest is the source of truth for what is done next.
- Stackline packet seeds can be carried into normalized files before raw collection finishes, but those rows should still be treated as in-progress until real collection artifacts arrive.
- Analysis files can be written before raw collection is complete, but they must stay clearly provisional until non-seed competitor collection exists.
