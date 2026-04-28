"""Top-level workflow wrapper for the PRD Research Tool.

This script does not replace the existing step-specific tools. It provides a
single entrypoint that strings them together into the real operating phases:

- prepare: initialize/refresh a session and generate external-data handoff files
- refresh: re-init from workbook plus payload JSONs, then rebuild downstream artifacts
- finalize: validate collected raw artifacts and push them through normalization/reporting
- status: show current manifest status and next collection tasks
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from competitor_normalizer import normalize_session
from competitive_analysis import analyze_session
from family_metrics_local_fallback import enrich_payload_rows, load_payload_rows
from family_metrics_postgres_batch import (
    build_query_bundle as build_family_query_bundle,
    output_stem_for as family_output_stem_for,
)
from price_enrichment import enrich_session
from reference_postgres_batch import (
    build_query_bundle as build_reference_query_bundle,
    output_stem_for as reference_output_stem_for,
)
from research_report_builder import build_reports
from research_session_manager import (
    initialize_session,
    session_status,
    update_session,
    validate_raw_artifacts,
)


DEFAULT_WORKBOOK = Path(__file__).resolve().parents[1] / "templates" / "PRD_Research_Template.xlsx"


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


def write_json(path: Path, payload: Any) -> Path:
    """Write a JSON payload with consistent formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def resolve_refresh_context(
    target: Path,
    *,
    session_name: str | None,
    output_root: str | None,
    sheet_name: str | None,
) -> dict[str, Any]:
    """Resolve workbook/session inputs for refresh mode."""
    if target.is_dir():
        manifest_path = target / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"Session manifest not found at {manifest_path}.")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        return {
            "workbook_path": Path(manifest["workbook_path"]).resolve(),
            "session_name": session_name or target.name,
            "output_root": output_root or str(target.parent),
            "sheet_name": sheet_name or manifest.get("sheet_name") or "Ideations",
            "session_root": target.resolve(),
        }

    workbook_path = target.resolve()
    resolved_session_name = session_name or workbook_path.stem.lower().replace(" ", "_")
    resolved_output_root = output_root or str(workbook_path.parent / "output" / "research_sessions")
    session_root = Path(resolved_output_root).resolve() / resolved_session_name
    return {
        "workbook_path": workbook_path,
        "session_name": resolved_session_name,
        "output_root": resolved_output_root,
        "sheet_name": sheet_name or "Ideations",
        "session_root": session_root,
    }


def build_reference_query_files(
    target: Path,
    *,
    sheet_name: str,
    start_date: str | None,
    end_date: str | None,
    output_prefix: str | None = None,
) -> dict[str, Any]:
    """Generate the reference-baseline Postgres query bundle files."""
    bundle = build_reference_query_bundle(
        target=target,
        sheet_name=sheet_name,
        start_date=start_date,
        end_date=end_date,
    )
    output_prefix_path = Path(output_prefix).resolve() if output_prefix else reference_output_stem_for(target)
    queries_path = output_prefix_path.with_name(output_prefix_path.name + "_queries.json")
    payload_path = output_prefix_path.with_name(output_prefix_path.name + "_payload_template.json")

    write_json(
        queries_path,
        {
            "generated_at": bundle["generated_at"],
            "source_path": bundle["source_path"],
            "sales_window": bundle["sales_window"],
            "selection_guidance": bundle["selection_guidance"],
            "sku_count": bundle["sku_count"],
            "row_count": bundle["row_count"],
            "items": bundle["items"],
        },
    )
    write_json(payload_path, bundle["payload_template"])

    return {
        "queries_file": str(queries_path.resolve()),
        "payload_file": str(payload_path.resolve()),
        "sku_count": bundle["sku_count"],
        "row_count": bundle["row_count"],
        "sales_window": bundle["sales_window"],
    }


def build_family_query_files(
    target: Path,
    *,
    sheet_name: str,
    monthly_start_date: str | None,
    monthly_end_date: str | None,
    customer_start_date: str | None,
    customer_end_date: str | None,
    output_prefix: str | None = None,
) -> dict[str, Any]:
    """Generate the family-metrics Postgres query bundle files."""
    bundle = build_family_query_bundle(
        target=target,
        sheet_name=sheet_name,
        monthly_start_date=monthly_start_date,
        monthly_end_date=monthly_end_date,
        customer_start_date=customer_start_date,
        customer_end_date=customer_end_date,
    )
    output_prefix_path = Path(output_prefix).resolve() if output_prefix else family_output_stem_for(target)
    queries_path = output_prefix_path.with_name(output_prefix_path.name + "_postgres_queries.json")
    payload_path = output_prefix_path.with_name(output_prefix_path.name + "_payload_template.json")

    write_json(
        queries_path,
        {
            "generated_at": bundle["generated_at"],
            "source_path": bundle["source_path"],
            "windows": bundle["windows"],
            "selection_guidance": bundle["selection_guidance"],
            "sku_count": bundle["sku_count"],
            "row_count": bundle["row_count"],
            "items": bundle["items"],
        },
    )
    write_json(payload_path, bundle["payload_template"])

    return {
        "queries_file": str(queries_path.resolve()),
        "payload_file": str(payload_path.resolve()),
        "sku_count": bundle["sku_count"],
        "row_count": bundle["row_count"],
        "windows": bundle["windows"],
    }


def apply_local_family_fallback(payload_path: Path) -> dict[str, Any]:
    """Backfill local Amazon monthly family metrics into a payload file."""
    payload_rows = load_payload_rows(payload_path)
    enriched_rows, summary = enrich_payload_rows(payload_rows)
    write_json(payload_path, enriched_rows)
    return {"payload_file": str(payload_path.resolve()), **summary}


def finalize_session(
    session_root: str,
    *,
    rows: list[int] | None,
    run_price_enrichment: bool,
    build_combined: bool,
    fail_on_invalid_raw: bool,
) -> dict[str, Any]:
    """Validate raw artifacts and push them through the downstream pipeline."""
    validation = validate_raw_artifacts(session_root, rows=rows)
    invalid_count = int(validation.get("invalid_artifact_count") or 0)
    if invalid_count and fail_on_invalid_raw:
        raise SystemExit(
            json.dumps(
                {
                    "error": "raw_validation_failed",
                    "invalid_artifact_count": invalid_count,
                    "validation": validation,
                },
                indent=2,
            )
        )

    normalize_result = normalize_session(session_root, rows=rows)
    enrichment_result = enrich_session(session_root, rows=rows) if run_price_enrichment else None
    analysis_result = analyze_session(session_root, rows=rows)
    report_result = build_reports(session_root, rows=rows)
    combined_result = build_reports(session_root, combined=True) if build_combined else None
    update_result = update_session(session_root)
    status_result = session_status(session_root, rows=rows, limit=5)

    return {
        "session_root": str(Path(session_root).resolve()),
        "validation": validation,
        "normalized": normalize_result,
        "price_enrichment": enrichment_result,
        "analysis": analysis_result,
        "reports": report_result,
        "combined": combined_result,
        "update": update_result,
        "status": status_result,
    }


def command_prepare(args: argparse.Namespace) -> dict[str, Any]:
    """Initialize a session and generate handoff/query bundle artifacts."""
    init_result = initialize_session(
        workbook_path=args.workbook,
        session_name=args.session_name,
        output_root=args.output_root,
        postgres_json=args.postgres_json,
        family_metrics_json=args.family_metrics_json,
        include_queries=args.include_queries,
        include_stackline_raw=args.include_stackline_raw,
        start_date=args.start_date,
        end_date=args.end_date,
        stackline_folder=args.stackline_folder,
        stackline_brand=args.stackline_brand,
        sheet_name=args.sheet,
    )
    session_root = Path(init_result["session_root"]).resolve()

    reference_queries = None
    family_queries = None
    local_family_fallback = None

    if not args.skip_reference_queries:
        reference_queries = build_reference_query_files(
            session_root,
            sheet_name=args.sheet,
            start_date=args.start_date,
            end_date=args.end_date,
        )

    if not args.skip_family_queries:
        family_queries = build_family_query_files(
            session_root,
            sheet_name=args.sheet,
            monthly_start_date=args.monthly_start_date,
            monthly_end_date=args.monthly_end_date,
            customer_start_date=args.customer_start_date,
            customer_end_date=args.customer_end_date,
        )
        if args.apply_local_family_fallback and family_queries:
            local_family_fallback = apply_local_family_fallback(Path(family_queries["payload_file"]))

    return {
        "mode": "prepare",
        "init": init_result,
        "reference_queries": reference_queries,
        "family_queries": family_queries,
        "local_family_fallback": local_family_fallback,
        "status": session_status(str(session_root), limit=args.limit),
    }


def command_refresh(args: argparse.Namespace) -> dict[str, Any]:
    """Re-init a session from workbook + payloads, then rebuild downstream artifacts."""
    context = resolve_refresh_context(
        Path(args.target).expanduser().resolve(),
        session_name=args.session_name,
        output_root=args.output_root,
        sheet_name=args.sheet,
    )
    session_root = Path(context["session_root"]).resolve()

    postgres_json = Path(args.postgres_json).resolve() if args.postgres_json else session_root / "reference_postgres_payload_template.json"
    family_metrics_json = Path(args.family_metrics_json).resolve() if args.family_metrics_json else session_root / "family_metrics_payload_template.json"

    local_family_fallback = None
    if args.apply_local_family_fallback and family_metrics_json.exists():
        local_family_fallback = apply_local_family_fallback(family_metrics_json)

    init_result = initialize_session(
        workbook_path=str(context["workbook_path"]),
        session_name=context["session_name"],
        output_root=context["output_root"],
        postgres_json=str(postgres_json) if postgres_json.exists() else None,
        family_metrics_json=str(family_metrics_json) if family_metrics_json.exists() else None,
        include_queries=args.include_queries,
        include_stackline_raw=args.include_stackline_raw,
        start_date=args.start_date,
        end_date=args.end_date,
        stackline_folder=args.stackline_folder,
        stackline_brand=args.stackline_brand,
        sheet_name=context["sheet_name"],
    )

    finalize_result = finalize_session(
        init_result["session_root"],
        rows=parse_rows_argument(args.rows),
        run_price_enrichment=not args.skip_price_enrichment,
        build_combined=not args.skip_combined,
        fail_on_invalid_raw=not args.allow_invalid_raw,
    )

    return {
        "mode": "refresh",
        "local_family_fallback": local_family_fallback,
        "init": init_result,
        "finalize": finalize_result,
    }


def command_finalize(args: argparse.Namespace) -> dict[str, Any]:
    """Run the downstream post-collection pipeline for a session."""
    result = finalize_session(
        args.session_root,
        rows=parse_rows_argument(args.rows),
        run_price_enrichment=not args.skip_price_enrichment,
        build_combined=not args.skip_combined,
        fail_on_invalid_raw=not args.allow_invalid_raw,
    )
    return {"mode": "finalize", **result}


def command_status(args: argparse.Namespace) -> dict[str, Any]:
    """Return session status plus next collection tasks."""
    return {
        "mode": "status",
        "status": session_status(
            args.session_root,
            rows=parse_rows_argument(args.rows),
            limit=args.limit,
        ),
    }


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""
    parser = argparse.ArgumentParser(
        description="Top-level workflow wrapper for the PRD Research Tool."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser(
        "prepare",
        help="Initialize a session and generate Postgres/family query bundle files.",
    )
    prepare.add_argument(
        "workbook",
        nargs="?",
        default=str(DEFAULT_WORKBOOK),
        help="Path to the filled PRD research workbook.",
    )
    prepare.add_argument("--session-name", default=None, help="Optional stable session name.")
    prepare.add_argument("--output-root", default=None, help="Optional custom session root parent.")
    prepare.add_argument("--sheet", default="Ideations", help="Worksheet name to parse.")
    prepare.add_argument("--postgres-json", default=None, help="Optional reference-baseline payload JSON to merge during init.")
    prepare.add_argument("--family-metrics-json", default=None, help="Optional family-metrics payload JSON to merge during init.")
    prepare.add_argument("--include-queries", action="store_true", help="Embed MCP query templates in packets.")
    prepare.add_argument("--include-stackline-raw", action="store_true", help="Embed full Stackline analysis in packets.")
    prepare.add_argument("--start-date", default=None, help="Override reference-baseline sales-window start date.")
    prepare.add_argument("--end-date", default=None, help="Override reference-baseline sales-window end date.")
    prepare.add_argument("--monthly-start-date", default=None, help="Override family-metrics monthly-sales start date.")
    prepare.add_argument("--monthly-end-date", default=None, help="Override family-metrics monthly-sales end date.")
    prepare.add_argument("--customer-start-date", default=None, help="Override family-metrics customer-concentration start date.")
    prepare.add_argument("--customer-end-date", default=None, help="Override family-metrics customer-concentration end date.")
    prepare.add_argument("--stackline-folder", default=None, help="Override Stackline export folder.")
    prepare.add_argument("--stackline-brand", default="Sunco Lighting", help="Internal brand name for Stackline.")
    prepare.add_argument("--skip-reference-queries", action="store_true", help="Do not generate reference Postgres query bundle files.")
    prepare.add_argument("--skip-family-queries", action="store_true", help="Do not generate family-metrics Postgres query bundle files.")
    prepare.add_argument("--apply-local-family-fallback", action="store_true", help="Apply the local Amazon family-metrics fallback to the generated family payload template.")
    prepare.add_argument("--limit", type=int, default=5, help="How many next collection tasks to include in the summary.")

    refresh = subparsers.add_parser(
        "refresh",
        help="Re-init a session from workbook + payloads, then rebuild downstream artifacts.",
    )
    refresh.add_argument(
        "target",
        nargs="?",
        default=str(DEFAULT_WORKBOOK),
        help="Workbook path or existing session root.",
    )
    refresh.add_argument("--session-name", default=None, help="Optional stable session name when target is a workbook.")
    refresh.add_argument("--output-root", default=None, help="Optional custom session root parent when target is a workbook.")
    refresh.add_argument("--sheet", default=None, help="Worksheet name override.")
    refresh.add_argument("--postgres-json", default=None, help="Reference-baseline payload JSON. Defaults to session_root\\reference_postgres_payload_template.json.")
    refresh.add_argument("--family-metrics-json", default=None, help="Family-metrics payload JSON. Defaults to session_root\\family_metrics_payload_template.json.")
    refresh.add_argument("--apply-local-family-fallback", action="store_true", help="Apply the local Amazon family-metrics fallback before refresh if a family payload file exists.")
    refresh.add_argument("--include-queries", action="store_true", help="Embed MCP query templates in packets.")
    refresh.add_argument("--include-stackline-raw", action="store_true", help="Embed full Stackline analysis in packets.")
    refresh.add_argument("--start-date", default=None, help="Override reference-baseline sales-window start date.")
    refresh.add_argument("--end-date", default=None, help="Override reference-baseline sales-window end date.")
    refresh.add_argument("--stackline-folder", default=None, help="Override Stackline export folder.")
    refresh.add_argument("--stackline-brand", default="Sunco Lighting", help="Internal brand name for Stackline.")
    refresh.add_argument("--rows", default=None, help="Optional comma-separated row numbers for downstream steps.")
    refresh.add_argument("--skip-price-enrichment", action="store_true", help="Skip the post-collection price enrichment pass.")
    refresh.add_argument("--skip-combined", action="store_true", help="Skip rebuilding the combined workbook.")
    refresh.add_argument("--allow-invalid-raw", action="store_true", help="Continue even if raw validation still reports invalid artifacts.")

    finalize = subparsers.add_parser(
        "finalize",
        help="Validate raw artifacts and run normalize/analyze/report for a session.",
    )
    finalize.add_argument("session_root", help="Path to an initialized research session.")
    finalize.add_argument("--rows", default=None, help="Optional comma-separated row numbers to finalize.")
    finalize.add_argument("--skip-price-enrichment", action="store_true", help="Skip the post-collection price enrichment pass.")
    finalize.add_argument("--skip-combined", action="store_true", help="Skip rebuilding the combined workbook.")
    finalize.add_argument("--allow-invalid-raw", action="store_true", help="Continue even if raw validation still reports invalid artifacts.")

    status = subparsers.add_parser(
        "status",
        help="Show current manifest summary plus next raw collection tasks.",
    )
    status.add_argument("session_root", help="Path to an initialized research session.")
    status.add_argument("--rows", default=None, help="Optional comma-separated row numbers.")
    status.add_argument("--limit", type=int, default=5, help="Maximum number of next tasks to return.")

    return parser


def main() -> None:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "prepare":
        result = command_prepare(args)
    elif args.command == "refresh":
        result = command_refresh(args)
    elif args.command == "finalize":
        result = command_finalize(args)
    else:
        result = command_status(args)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
