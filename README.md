# PRD Research Tool

Category-agnostic ideation template plus competitive research workflow for Sunco product concepts.

## Core Flow

1. PM fills the Excel ideation template.
2. Tool initializes a research session and packetizes each row.
3. Raw competitor collection is completed per row/channel.
4. Tool normalizes, analyzes, and reports each ideation.
5. PM reviews the workbook and uses the findings to refine the ideation / PRD.

## Main Entry Point

Use the orchestrator for the normal workflow:

```powershell
cmd /c C:\Windows\py.exe C:\Users\Sunco\Projects\PRD-research-tool\tools\research_orchestrator.py prepare "C:\path\to\workbook.xlsx" --session-name my_session --output-root "C:\path\to\output\research_sessions"
cmd /c C:\Windows\py.exe C:\Users\Sunco\Projects\PRD-research-tool\tools\research_orchestrator.py finalize "C:\path\to\output\research_sessions\my_session"
cmd /c C:\Windows\py.exe C:\Users\Sunco\Projects\PRD-research-tool\tools\research_orchestrator.py refresh "C:\path\to\output\research_sessions\my_session" --postgres-json "C:\path\to\reference_postgres_payload_template.json" --family-metrics-json "C:\path\to\family_metrics_payload_template.json"
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
