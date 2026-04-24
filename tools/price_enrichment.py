r"""
Post-collection price enrichment for existing raw competitor artifacts.

Usage:
  cmd /c C:\Windows\py.exe tools\price_enrichment.py "C:\path\to\research_session"
  cmd /c C:\Windows\py.exe tools\price_enrichment.py "C:\path\to\research_session" --rows 4,13,14
"""

from __future__ import annotations

import argparse
import html
import json
import re
from pathlib import Path
from typing import Any, Iterable

import requests

from research_session_manager import (
    artifact_path_for,
    read_json,
    utc_now,
    write_json,
)


COLLECTION_STAGE_KEYS = [
    "amazon_collection",
    "brick_and_mortar_collection",
    "brand_site_collection",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)

PRICE_HINTS = [
    "product:price:amount",
    "og:price:amount",
    '"price"',
    '"priceAmount"',
    '"lowPrice"',
    '"highPrice"',
    '"salePrice"',
    '"currentPrice"',
]


def normalize_text(value: Any) -> str | None:
    """Normalize optional values into stripped strings."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "Yes" if value else "No"
    text = str(value).strip()
    return text or None


def parse_rows_argument(value: str | None) -> list[int] | None:
    """Parse a comma-separated row list."""
    if not value:
        return None
    rows = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        rows.append(int(part))
    return rows or None


def parse_price_number(value: Any) -> float | None:
    """Parse a numeric-ish value into a float."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if 0 < number < 100000 else None
    text = str(value).strip().replace("$", "").replace(",", "")
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return number if 0 < number < 100000 else None


def unique_preserve_order(values: Iterable[str]) -> list[str]:
    """Return unique strings while preserving order."""
    seen = set()
    result = []
    for value in values:
        text = normalize_text(value)
        if not text:
            continue
        marker = text.lower()
        if marker in seen:
            continue
        seen.add(marker)
        result.append(text)
    return result


def walk_json(value: Any) -> Iterable[Any]:
    """Yield all nested JSON-like nodes."""
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk_json(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_json(child)


def parse_json_ld_blocks(text: str) -> list[Any]:
    """Extract and parse JSON-LD script blocks from HTML."""
    blocks = []
    pattern = re.compile(
        r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for raw in pattern.findall(text):
        candidate = html.unescape(raw).strip()
        if not candidate:
            continue
        try:
            blocks.append(json.loads(candidate))
            continue
        except json.JSONDecodeError:
            pass
        cleaned = re.sub(r"^\s*//.*$", "", candidate, flags=re.MULTILINE)
        cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
        try:
            blocks.append(json.loads(cleaned))
        except json.JSONDecodeError:
            continue
    return blocks


def extract_price_from_json_ld(text: str) -> tuple[float | None, str | None, str | None]:
    """Extract a price from JSON-LD offers when available."""
    for block in parse_json_ld_blocks(text):
        for node in walk_json(block):
            if not isinstance(node, dict):
                continue
            for offer in [node.get("offers"), node]:
                if isinstance(offer, list):
                    offers = offer
                else:
                    offers = [offer]
                for entry in offers:
                    if not isinstance(entry, dict):
                        continue
                    for price_key in ("price", "lowPrice", "highPrice"):
                        price = parse_price_number(entry.get(price_key))
                        if price is None:
                            continue
                        currency = normalize_text(entry.get("priceCurrency")) or "USD"
                        return price, currency, f"json-ld:{price_key}"
    return None, None, None


def extract_price_from_meta(text: str) -> tuple[float | None, str | None, str | None]:
    """Extract a price from common metadata tags."""
    meta_patterns = [
        (
            re.compile(
                r"<meta[^>]+(?:property|itemprop)=[\"'](?:product:price:amount|og:price:amount|price)[\"'][^>]+content=[\"'](?P<price>\d+(?:\.\d+)?)",
                flags=re.IGNORECASE,
            ),
            "meta:price",
        ),
        (
            re.compile(
                r"<meta[^>]+content=[\"'](?P<price>\d+(?:\.\d+)?)[\"'][^>]+(?:property|itemprop)=[\"'](?:product:price:amount|og:price:amount|price)[\"']",
                flags=re.IGNORECASE,
            ),
            "meta:price",
        ),
    ]
    currency_patterns = [
        re.compile(
            r"<meta[^>]+(?:property|itemprop)=[\"'](?:product:price:currency|og:price:currency|priceCurrency)[\"'][^>]+content=[\"'](?P<currency>[A-Z]{3})",
            flags=re.IGNORECASE,
        ),
        re.compile(
            r"<meta[^>]+content=[\"'](?P<currency>[A-Z]{3})[\"'][^>]+(?:property|itemprop)=[\"'](?:product:price:currency|og:price:currency|priceCurrency)[\"']",
            flags=re.IGNORECASE,
        ),
    ]
    currency = None
    for pattern in currency_patterns:
        match = pattern.search(text)
        if match:
            currency = match.group("currency").upper()
            break
    for pattern, source in meta_patterns:
        match = pattern.search(text)
        if not match:
            continue
        price = parse_price_number(match.group("price"))
        if price is not None:
            return price, currency or "USD", source
    return None, None, None


def extract_price_from_generic_patterns(text: str) -> tuple[float | None, str | None, str | None]:
    """Extract a price using generic JSON / HTML patterns."""
    regexes = [
        (re.compile(r'"price"\s*:\s*"(?P<price>\d+(?:\.\d+)?)"', flags=re.IGNORECASE), 'json:price'),
        (re.compile(r'"price"\s*:\s*(?P<price>\d+(?:\.\d+)?)', flags=re.IGNORECASE), 'json:price'),
        (re.compile(r'"priceAmount"\s*:\s*"(?P<price>\d+(?:\.\d+)?)"', flags=re.IGNORECASE), 'json:priceAmount'),
        (re.compile(r'"lowPrice"\s*:\s*"(?P<price>\d+(?:\.\d+)?)"', flags=re.IGNORECASE), 'json:lowPrice'),
        (re.compile(r'"salePrice"\s*:\s*"?(?P<price>\d+(?:\.\d+)?)"?', flags=re.IGNORECASE), 'json:salePrice'),
        (re.compile(r"\$\s?(?P<price>\d{1,5}(?:,\d{3})?(?:\.\d{2}))"), 'html:dollar'),
    ]
    for pattern, source in regexes:
        match = pattern.search(text)
        if not match:
            continue
        price = parse_price_number(match.group("price"))
        if price is not None:
            return price, "USD", source
    return None, None, None


def extract_price_payload(text: str) -> tuple[float | None, str | None, str | None]:
    """Extract a product price from raw HTML."""
    for extractor in (
        extract_price_from_json_ld,
        extract_price_from_meta,
        extract_price_from_generic_patterns,
    ):
        price, currency, source = extractor(text)
        if price is not None:
            return price, currency, source
    return None, None, None


def should_attempt_enrichment(item: dict[str, Any]) -> bool:
    """Return whether this item should be fetched for price enrichment."""
    if parse_price_number(item.get("price")) is not None:
        return False
    url = normalize_text(item.get("url"))
    if not url or url.startswith("stackline://"):
        return False
    return True


def fetch_price(session: requests.Session, url: str) -> tuple[float | None, str | None, str | None]:
    """Fetch one product page and try to extract a price."""
    try:
        response = session.get(url, timeout=20)
    except requests.RequestException:
        return None, None, None
    if response.status_code >= 400:
        return None, None, None
    text = response.text or ""
    if not text:
        return None, None, None
    if not any(hint in text for hint in PRICE_HINTS) and "$" not in text:
        return None, None, None
    return extract_price_payload(text)


def append_extraction_note(existing: Any, note: str) -> str:
    """Append a short extraction note without duplicating prior text."""
    current = normalize_text(existing)
    if not current:
        return note
    if note in current:
        return current
    return f"{current} | {note}"


def artifact_paths_for_row(session_dir: Path, row_number: int) -> list[Path]:
    """Return all raw artifact paths for a row."""
    return [
        artifact_path_for(session_dir, row_number, stage_key)
        for stage_key in COLLECTION_STAGE_KEYS
    ]


def enrich_artifact(path: Path, http: requests.Session) -> dict[str, Any]:
    """Enrich one raw artifact in place."""
    payload = read_json(path)
    changed_items = 0
    attempted = 0
    errors = 0

    items = payload.get("items", [])
    if not isinstance(items, list) or not items:
        return {"changed": False, "changed_items": 0, "attempted": 0, "errors": 0}

    for item in items:
        if not isinstance(item, dict):
            continue
        if not should_attempt_enrichment(item):
            continue
        attempted += 1
        url = normalize_text(item.get("url"))
        if not url:
            continue
        price, currency, source = fetch_price(http, url)
        if price is None:
            errors += 1
            continue
        item["price"] = price
        if currency:
            item["currency"] = currency
        source_note = f"price_enrichment:{source}"
        item["extraction_notes"] = append_extraction_note(item.get("extraction_notes"), source_note)
        changed_items += 1

    if changed_items == 0:
        return {"changed": False, "changed_items": 0, "attempted": attempted, "errors": errors}

    notes = payload.get("notes", [])
    if not isinstance(notes, list):
        notes = []
    notes.append(f"price_enrichment.py populated {changed_items} prices on {utc_now()}.")
    payload["notes"] = unique_preserve_order(notes)
    payload["updated_at"] = utc_now()
    write_json(path, payload)
    return {
        "changed": True,
        "changed_items": changed_items,
        "attempted": attempted,
        "errors": errors,
    }


def enrich_session(session_root: str, rows: list[int] | None = None) -> dict[str, Any]:
    """Run price enrichment against selected session rows."""
    session_dir = Path(session_root).resolve()
    manifest = read_json(session_dir / "manifest.json")
    target_rows = set(rows or [row["row_number"] for row in manifest.get("rows", [])])

    http = requests.Session()
    http.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )

    changed_rows: list[int] = []
    total_changed_items = 0
    total_attempted = 0
    total_errors = 0
    artifact_results = []

    for row_number in sorted(target_rows):
        row_changed = False
        for path in artifact_paths_for_row(session_dir, row_number):
            result = enrich_artifact(path, http)
            artifact_results.append(
                {
                    "row_number": row_number,
                    "artifact_file": str(path),
                    **result,
                }
            )
            total_changed_items += result["changed_items"]
            total_attempted += result["attempted"]
            total_errors += result["errors"]
            row_changed = row_changed or result["changed"]
        if row_changed:
            changed_rows.append(row_number)

    return {
        "session_root": str(session_dir),
        "rows_requested": sorted(target_rows),
        "rows_changed": changed_rows,
        "changed_item_count": total_changed_items,
        "attempted_fetch_count": total_attempted,
        "unresolved_fetch_count": total_errors,
        "artifact_results": artifact_results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill missing competitor prices from product pages."
    )
    parser.add_argument("session_root", help="Path to an initialized research session.")
    parser.add_argument(
        "--rows",
        default=None,
        help="Optional comma-separated row numbers to enrich.",
    )
    args = parser.parse_args()

    result = enrich_session(
        session_root=args.session_root,
        rows=parse_rows_argument(args.rows),
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
