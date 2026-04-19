"""Helpers for extracting EOAA application status tables from the Greek page."""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import requests
from bs4 import BeautifulSoup

DEFAULT_PAGE_URL = "https://eoaa.org.cy/data-transparent-organization/building-application-status/"
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "el,en;q=0.9",
}
EXPECTED_HEADERS = [
    "Τύπος αίτησης",
    "Περιγραφή αίτησης",
    "Κατάσταση αίτησης",
    "Υπο-κατάσταση αίτησης",
    "Ημερομηνία λήψης",
    "Ημερομηνία ολοκλήρωσης",
]
MODIFIED_TIME_PATTERN = re.compile(
    r'<meta property="article:modified_time" content="([^"]+)"',
    re.IGNORECASE,
)
EN_DASH = "\u2013"
APPLICATION_TYPE_SPLIT_PATTERN = re.compile(rf"\s*[{EN_DASH}-]\s*", re.UNICODE)


@dataclass(frozen=True)
class ApplicationsPage:
    """Structured representation of the EOAA applications page."""

    records: list[dict[str, Any]]
    source_url: str
    source_modified_at: str | None
    fetched_at: str
    table_count: int


class ApplicationsPageError(ValueError):
    """Raised when the EOAA applications page cannot be extracted."""


def fetch_page_html(url: str = DEFAULT_PAGE_URL, timeout: int = 30) -> str:
    """Fetch the EOAA page HTML with a browser-like user agent."""
    response = requests.get(url, headers=REQUEST_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def extract_applications_from_html(
    html: str,
    source_url: str = DEFAULT_PAGE_URL,
) -> ApplicationsPage:
    """Extract all application rows from the EOAA Greek page HTML."""
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    modified_match = MODIFIED_TIME_PATTERN.search(html)
    fetched_at = datetime.now(UTC).isoformat()

    records: list[dict[str, Any]] = []
    occurrence_by_base_key: defaultdict[tuple[str | None, ...], int] = defaultdict(int)
    matched_table_count = 0

    for table_index, table in enumerate(tables, start=1):
        rows = table.find_all("tr")
        if not rows:
            continue

        header_cells = rows[0].find_all(["th", "td"])
        headers = [_clean_text(cell.get_text(" ", strip=True)) for cell in header_cells]
        if headers != EXPECTED_HEADERS:
            continue

        matched_table_count += 1

        for row_index, row in enumerate(rows[1:], start=1):
            cells = [
                _clean_text(cell.get_text(" ", strip=True))
                for cell in row.find_all(["th", "td"])
            ]
            if not cells or all(cell is None for cell in cells):
                continue

            padded_cells = cells[: len(EXPECTED_HEADERS)]
            if len(padded_cells) < len(EXPECTED_HEADERS):
                padded_cells.extend([None] * (len(EXPECTED_HEADERS) - len(padded_cells)))

            (
                original_application_type,
                application_description,
                status,
                sub_status,
                raw_received_date,
                raw_completion_date,
            ) = padded_cells

            received_date = _parse_date(raw_received_date)
            completion_date = _parse_date(raw_completion_date)
            application_type = _extract_application_type(original_application_type)
            source_month = received_date[:7] if received_date else None

            base_key = (
                source_month,
                original_application_type,
                application_description,
                received_date,
            )
            occurrence_by_base_key[base_key] += 1
            occurrence_index = occurrence_by_base_key[base_key]

            records.append(
                {
                    "record_id": _build_record_id(*base_key, occurrence_index),
                    "application_type": application_type,
                    "original_application_type": original_application_type,
                    "application_description": application_description,
                    "status": status,
                    "sub_status": sub_status,
                    "received_date": received_date,
                    "completion_date": completion_date,
                    "source_month": source_month,
                    "source_table_index": table_index,
                    "source_row_index": row_index,
                    "source_headers_json": json.dumps(headers, ensure_ascii=False),
                    "source_occurrence_index": occurrence_index,
                    "source_page_modified_at": (
                        modified_match.group(1) if modified_match else None
                    ),
                    "source_url": source_url,
                    "fetched_at": fetched_at,
                    "row_content_hash": _build_row_content_hash(
                        original_application_type,
                        application_description,
                        status,
                        sub_status,
                        received_date,
                        completion_date,
                    ),
                }
            )

    if not records:
        raise ApplicationsPageError(
            "Could not find any EOAA application tables with the expected headers"
        )

    return ApplicationsPage(
        records=records,
        source_url=source_url,
        source_modified_at=modified_match.group(1) if modified_match else None,
        fetched_at=fetched_at,
        table_count=matched_table_count,
    )


def fetch_applications_page(
    url: str = DEFAULT_PAGE_URL,
    timeout: int = 30,
) -> ApplicationsPage:
    """Fetch the EOAA applications page and return normalized records."""
    html = fetch_page_html(url=url, timeout=timeout)
    return extract_applications_from_html(html=html, source_url=url)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    return text or None


def _parse_date(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None

    for date_format in ("%d/%m/%y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, date_format).date().isoformat()
        except ValueError:
            continue

    raise ApplicationsPageError(f"Unsupported EOAA date format: {text}")


def _extract_application_type(value: str | None) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None

    return APPLICATION_TYPE_SPLIT_PATTERN.split(text, maxsplit=1)[0].strip() or None


def _build_record_id(
    source_month: str | None,
    original_application_type: str | None,
    application_description: str | None,
    received_date: str | None,
    occurrence_index: int,
) -> str:
    raw_key = "||".join(
        [
            source_month or "",
            original_application_type or "",
            application_description or "",
            received_date or "",
            str(occurrence_index),
        ]
    )
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def _build_row_content_hash(*values: str | None) -> str:
    raw_value = "||".join(value or "" for value in values)
    return hashlib.sha256(raw_value.encode("utf-8")).hexdigest()
