# PRD Research Tool

Category-agnostic ideation template plus competitive research workflow for Sunco product concepts.

## Core Flow

1. PM fills the Excel ideation template.
2. Tool initializes a research session and packetizes each row.
3. Raw competitor collection is completed per row/channel.
4. Tool normalizes, analyzes, and reports each ideation.
5. PM reviews the workbook and uses the findings to refine the ideation / PRD.

## Canonical Template Location

The preferred filled template path is the shared-drive copy:

- `C:\Users\Sunco\Sunco Lighting\Product - Manny Tools\PRD Research\Templates\PRD_Research_Template.xlsx`

If that file exists, the CLI defaults now prefer it automatically over the repo-local template copy.

## Main Entry Point

Use the orchestrator for the normal workflow:

```powershell
cmd /c C:\Windows\py.exe C:\Users\Sunco\Projects\PRD-research-tool\tools\research_orchestrator.py prepare "C:\path\to\workbook.xlsx" --session-name my_session --output-root "C:\path\to\output\research_sessions"
cmd /c C:\Windows\py.exe C:\Users\Sunco\Projects\PRD-research-tool\tools\research_orchestrator.py finalize "C:\path\to\output\research_sessions\my_session"
cmd /c C:\Windows\py.exe C:\Users\Sunco\Projects\PRD-research-tool\tools\research_orchestrator.py refresh "C:\path\to\output\research_sessions\my_session" --postgres-json "C:\path\to\reference_postgres_payload_template.json" --family-metrics-json "C:\path\to\family_metrics_payload_template.json"
cmd /c C:\Windows\py.exe C:\Users\Sunco\Projects\PRD-research-tool\tools\research_orchestrator.py publish "C:\path\to\output\research_sessions\my_session"
```

## Operator Workflow

For the current low-token user -> Codex -> Claude -> Codex flow, see:

- [docs/operator_workflow.md](docs/operator_workflow.md)

To generate the exact Claude collection prompt for an existing session:

```powershell
cmd /c C:\Windows\py.exe C:\Users\Sunco\Projects\PRD-research-tool\tools\render_claude_collect_prompt.py "C:\path\to\output\research_sessions\my_session"
```

## Orchestrator Modes

- `prepare`
  - initializes or refreshes the session
  - generates `reference_postgres_*` and `family_metrics_*` handoff files
  - can apply the local Amazon family-metrics fallback immediately
- `refresh`
  - re-inits a session from workbook plus payload JSONs
  - reruns normalize, price enrichment, analysis, row reports, combined workbook, and manifest update
- `finalize`
  - validates raw collection artifacts
  - runs normalize, price enrichment, analysis, row reports, combined workbook, and manifest update
- `status`
  - shows session summary plus next raw collection tasks
- `publish`
  - copies the combined workbook into the locally synced SharePoint reports folder
  - can optionally also copy the row-level reports into a session subfolder
  - by default names the published combined workbook as `CategoryOwner_Category_YYYYMMDD.xlsx`
  - falls back to `GOALS.md` category ownership mapping when the workbook/session does not carry a category owner yet
  - falls back to `UnknownOwner` only when neither the workbook nor `GOALS.md` yields an owner
  - appends `_02`, `_03`, etc. instead of overwriting if multiple reports are published on the same day

## Building Blocks

- `tools/research_session_manager.py`
- `tools/competitor_normalizer.py`
- `tools/price_enrichment.py`
- `tools/competitive_analysis.py`
- `tools/research_report_builder.py`
- `tools/reference_postgres_batch.py`
- `tools/family_metrics_postgres_batch.py`

## Current Boundaries

- Stackline is expected by default, but the workflow degrades gracefully when it is missing.
- Some reference SKUs may have sparse history; this lowers evidence confidence but does not block the batch.
- SharePoint download/upload automation is still a separate future step.
- SharePoint output publishing is currently implemented via the locally synced folder at `Product - Manny Tools\PRD Research\Research Reports`; Graph upload can be layered in later if needed.
