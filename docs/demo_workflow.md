# PRD Research Demo Workflow

This is the demo-safe operator path for showing the PRD Research Tool to the team.

Goal:

- latest filled shared template in
- fresh session created
- fresh workbook published to `Research Reports`
- use Codex first
- use Claude only if Codex proves a real remaining collection gap

## Demo Trigger

Use this exact prompt with Codex:

```text
PRD Research Demo Run
```

## Demo Rules

1. Always use the latest shared template:
   - `C:\Users\Sunco\Sunco Lighting\Product - Manny Tools\PRD Research\Working Tool Files\Templates\PRD_Research_Template.xlsx`
2. Treat the current shared template as the only source of truth.
3. Assume the reports/output folder may have been intentionally cleaned before the demo.
4. Create a fresh session for the run.
5. Prefer a Codex-only run when the current template aligns with already-completed research coverage.
6. Only escalate to Claude if Codex confirms that missing raw competitor collection is the actual blocker.
7. Do not ask the user to touch PowerShell.
8. Return a final workbook path that can be opened immediately in:
   - `C:\Users\Sunco\Sunco Lighting\Product - Manny Tools\PRD Research\Working Tool Files\Research Reports`

## Expected Codex Behavior

When the user says `PRD Research Demo Run`, Codex should:

1. Read the latest shared template.
2. Create a fresh session under the canonical session root.
3. Seed all deterministic context:
   - reference SKU context
   - Stackline context
   - existing structured/local artifacts
4. Reuse prior completed research artifacts only when they materially match the current template inputs.
5. Finalize and publish immediately if no true collection gap remains.
6. If a collection gap remains, stop and hand back the smallest possible Claude batch prompt.

## Success Definition

A successful demo run means:

1. The template is clearly the latest one.
2. A fresh session is created.
3. The report reflects the newest output logic.
4. A fresh combined workbook is published.
5. The user can open the published workbook immediately for the demo.
