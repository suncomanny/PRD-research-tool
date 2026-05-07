"""Render a minimal Claude collection prompt for the next missing task batch.

Usage:
  python tools/render_claude_collect_prompt.py "C:\\path\\to\\research_session"
  python tools/render_claude_collect_prompt.py "C:\\path\\to\\research_session" --max-rows 2 --max-artifacts 6
  python tools/render_claude_collect_prompt.py "C:\\path\\to\\research_session" --rows 7,8
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from research_session_manager import packet_path_for, read_json, session_status


TOOLS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TOOLS_DIR.parent
PROMPT_TEMPLATE = REPO_ROOT / "prompts" / "claude_prd_research_collect_template.txt"


def parse_rows_argument(value: str | None) -> list[int] | None:
    """Parse an optional comma-separated row list."""
    if not value:
        return None
    rows = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        rows.append(int(part))
    return rows or None


def group_batch(
    tasks: list[dict[str, Any]],
    *,
    max_rows: int,
    max_artifacts: int,
) -> list[dict[str, Any]]:
    """Select the next smallest useful batch while preserving manifest order."""
    batch: list[dict[str, Any]] = []
    seen_rows: list[int] = []
    for task in tasks:
        row_number = int(task["row_number"])
        if row_number not in seen_rows:
            if seen_rows and len(seen_rows) >= max_rows:
                break
            seen_rows.append(row_number)
        if len(batch) >= max_artifacts:
            break
        batch.append(task)
    return batch


def packet_scope_lines(session_root: Path, row_numbers: list[int]) -> list[str]:
    """Return the packet file paths for the selected rows."""
    lines = []
    for row_number in row_numbers:
        packet_path = packet_path_for(session_root, row_number)
        rel = packet_path.resolve().relative_to(session_root.resolve())
        lines.append(f"- Row {row_number}: {rel}")
    return lines


def ideation_scope_lines(batch: list[dict[str, Any]]) -> list[str]:
    """Return one ideation line per selected row."""
    seen: set[int] = set()
    lines: list[str] = []
    for task in batch:
        row_number = int(task["row_number"])
        if row_number in seen:
            continue
        seen.add(row_number)
        ideation_name = str(task.get("ideation_name") or "").strip()
        lines.append(f"- Row {row_number}: {ideation_name}")
    return lines


def artifact_scope_lines(batch: list[dict[str, Any]]) -> list[str]:
    """Return one artifact line per selected task."""
    lines = []
    for task in batch:
        row_number = int(task["row_number"])
        output_file = str(task.get("output_file") or "")
        lines.append(f"- Row {row_number}: {output_file}")
    return lines


def build_prompt(session_root: Path, batch: list[dict[str, Any]], batch_index: int) -> str:
    """Substitute the session path and exact batch scope into the prompt template."""
    template = PROMPT_TEMPLATE.read_text(encoding="utf-8")
    row_numbers = []
    for task in batch:
        row_number = int(task["row_number"])
        if row_number not in row_numbers:
            row_numbers.append(row_number)

    replacements = {
        "{{SESSION_ROOT}}": str(session_root.resolve()),
        "{{BATCH_LABEL}}": f"Batch {batch_index}",
        "{{ARTIFACT_COUNT}}": str(len(batch)),
        "{{ROW_COUNT}}": str(len(row_numbers)),
        "{{ARTIFACT_SCOPE}}": "\n".join(artifact_scope_lines(batch)),
        "{{IDEATION_SCOPE}}": "\n".join(ideation_scope_lines(batch)),
        "{{PACKET_SCOPE}}": "\n".join(packet_scope_lines(session_root, row_numbers)),
    }
    for needle, value in replacements.items():
        template = template.replace(needle, value)
    return template


def estimate_batch_index(all_tasks: list[dict[str, Any]], batch: list[dict[str, Any]]) -> int:
    """Estimate the 1-based batch number from the selected task offset."""
    if not batch:
        return 1
    first = batch[0]
    for index, task in enumerate(all_tasks):
        if task == first:
            return index + 1
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render a minimal Claude raw-collection prompt for the next missing batch."
    )
    parser.add_argument("session_root", help="Path to the existing research session root.")
    parser.add_argument(
        "--rows",
        default=None,
        help="Optional comma-separated row numbers to constrain the batch.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=2,
        help="Maximum distinct rows to include in the next Claude batch (default: 2).",
    )
    parser.add_argument(
        "--max-artifacts",
        type=int,
        default=6,
        help="Maximum artifact tasks to include in the next Claude batch (default: 6).",
    )
    parser.add_argument(
        "--status-limit",
        type=int,
        default=50,
        help="How many pending tasks to inspect before choosing a batch (default: 50).",
    )
    args = parser.parse_args()

    session_root = Path(args.session_root).resolve()
    status = session_status(
        str(session_root),
        rows=parse_rows_argument(args.rows),
        limit=args.status_limit,
    )
    tasks = status.get("next_tasks", [])
    batch = group_batch(tasks, max_rows=args.max_rows, max_artifacts=args.max_artifacts)

    print("SESSION")
    print(session_root)
    print()
    print("SUMMARY")
    print(status.get("summary"))
    print()

    if not batch:
        print("NO_CLAUDE_COLLECTION_NEEDED")
        return

    batch_index = estimate_batch_index(tasks, batch)
    print(f"BATCH_LABEL\nBatch {batch_index}")
    print()
    print("BATCH_TASKS")
    for task in batch:
        print(
            f"row {task.get('row_number')} | {task.get('stage_key')} | "
            f"{task.get('channel')} | {task.get('output_file')}"
        )
    print()
    print("CLAUDE_PROMPT_START")
    print(build_prompt(session_root, batch, batch_index))
    print("CLAUDE_PROMPT_END")


if __name__ == "__main__":
    main()
