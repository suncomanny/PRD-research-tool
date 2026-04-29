"""Render the lowest-token Claude collection prompt for an existing session.

Usage:
  python tools/render_claude_collect_prompt.py "C:\\path\\to\\research_session"
"""

from __future__ import annotations

import argparse
from pathlib import Path

from research_session_manager import session_status


TOOLS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TOOLS_DIR.parent
PROMPT_TEMPLATE = REPO_ROOT / "prompts" / "claude_prd_research_collect_template.txt"


def render_prompt(session_root: Path) -> str:
    """Substitute the session path into the prompt template."""
    template = PROMPT_TEMPLATE.read_text(encoding="utf-8")
    return template.replace("{{SESSION_ROOT}}", str(session_root.resolve()))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render the session-specific Claude raw-collection prompt."
    )
    parser.add_argument("session_root", help="Path to the existing research session root.")
    parser.add_argument(
        "--limit",
        type=int,
        default=12,
        help="How many pending tasks to show in the local summary (default: 12).",
    )
    args = parser.parse_args()

    session_root = Path(args.session_root).resolve()
    status = session_status(str(session_root), rows=None, limit=args.limit)

    print("SESSION")
    print(session_root)
    print()
    print("SUMMARY")
    print(status.get("summary"))
    print()
    print("NEXT_TASKS")
    for task in status.get("next_tasks", []):
        print(
            f"row {task.get('row_number')} | {task.get('stage_key')} | "
            f"{task.get('channel')} | {task.get('output_file')}"
        )
    print()
    print("CLAUDE_PROMPT_START")
    print(render_prompt(session_root))
    print("CLAUDE_PROMPT_END")


if __name__ == "__main__":
    main()
