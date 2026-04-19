"""Helpers for extracting the EOAA Visualizer table payload."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import requests

DEFAULT_PAGE_URL = (
    "https://eoaa.org.cy/en/eea-statistics-transparency-agency/"
    "building-application-status/"
)
DEFAULT_CHART_ID = "2323"
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
RAW_COLUMN_NAMES = (
    "record_number",
    "application_type_code",
    "development_type",
    "application_status",
    "submission_date",
    "decision_date",
    "extra_column_1",
    "extra_column_2",
    "extra_column_3",
)
MODIFIED_TIME_PATTERN = re.compile(
    r'<meta property="article:modified_time" content="([^"]+)"',
    re.IGNORECASE,
)


@dataclass(frozen=True)
class VisualizerChart:
    """Structured representation of a Visualizer chart table."""

    chart_id: str
    title: str | None
    headers: list[str]
    rows: list[list[Any]]
    source_url: str
    source_modified_at: str | None
    fetched_at: str


class VisualizerPayloadError(ValueError):
    """Raised when the EOAA page payload cannot be extracted."""


@dataclass(frozen=True)
class VisualizerPayload:
    """Raw Visualizer page payload plus the marker used to find it."""

    marker: str
    data: dict[str, Any]


def fetch_page_html(url: str = DEFAULT_PAGE_URL, timeout: int = 30) -> str:
    """Fetch the EOAA page HTML with a browser-like user agent."""
    response = requests.get(url, headers=REQUEST_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def extract_chart_from_html(
    html: str,
    chart_id: str = DEFAULT_CHART_ID,
    source_url: str = DEFAULT_PAGE_URL,
) -> VisualizerChart:
    """Extract a Visualizer chart payload from EOAA page HTML."""
    payload = _extract_visualizer_payload(html)
    charts = payload.data.get("charts", {})
    chart_key = _resolve_chart_key(charts, chart_id)
    if chart_key is None:
        available = ", ".join(sorted(charts))
        raise VisualizerPayloadError(
            f"Chart '{chart_id}' not found in payload. Available charts: {available}"
        )

    chart = charts[chart_key]
    data = chart.get("data", [])
    if not data:
        raise VisualizerPayloadError(f"Chart '{chart_id}' does not contain tabular data")

    series = chart.get("series") or []
    headers = _extract_headers(chart, data)
    row_values = data if series else data[1:]
    rows = [list(row) for row in row_values]
    modified_match = MODIFIED_TIME_PATTERN.search(html)

    return VisualizerChart(
        chart_id=chart_id,
        title=chart.get("title"),
        headers=headers,
        rows=rows,
        source_url=source_url,
        source_modified_at=modified_match.group(1) if modified_match else None,
        fetched_at=datetime.now(UTC).isoformat(),
    )


def fetch_chart(
    url: str = DEFAULT_PAGE_URL,
    chart_id: str = DEFAULT_CHART_ID,
    timeout: int = 30,
) -> VisualizerChart:
    """Fetch the EOAA page and return the requested chart."""
    html = fetch_page_html(url=url, timeout=timeout)
    return extract_chart_from_html(html=html, chart_id=chart_id, source_url=url)


def normalize_chart_rows(chart: VisualizerChart) -> list[dict[str, Any]]:
    """Normalize Visualizer rows into a stable load schema."""
    records: list[dict[str, Any]] = []

    for row in chart.rows:
        padded_row = list(row[: len(RAW_COLUMN_NAMES)])
        if len(padded_row) < len(RAW_COLUMN_NAMES):
            padded_row.extend([""] * (len(RAW_COLUMN_NAMES) - len(padded_row)))

        raw_record = dict(zip(RAW_COLUMN_NAMES, padded_row, strict=True))
        records.append(
            {
                "record_number": _parse_int(raw_record["record_number"]),
                "application_type_code": _clean_text(raw_record["application_type_code"]),
                "development_type": _clean_text(raw_record["development_type"]),
                "application_status": _clean_text(raw_record["application_status"]),
                "submission_date": _parse_date(raw_record["submission_date"]),
                "decision_date": _parse_date(raw_record["decision_date"]),
                "extra_column_1": _clean_text(raw_record["extra_column_1"]),
                "extra_column_2": _clean_text(raw_record["extra_column_2"]),
                "extra_column_3": _clean_text(raw_record["extra_column_3"]),
                "source_chart_id": chart.chart_id,
                "source_table_title": chart.title,
                "source_headers_json": json.dumps(chart.headers, ensure_ascii=False),
                "source_url": chart.source_url,
                "source_modified_at": chart.source_modified_at,
                "fetched_at": chart.fetched_at,
            }
        )

    return records


def _extract_visualizer_payload(html: str) -> VisualizerPayload:
    for marker in ("var visualizer =", "var visualizerObj ="):
        marker_index = html.find(marker)
        if marker_index == -1:
            continue

        start_index = html.find("{", marker_index)
        if start_index == -1:
            raise VisualizerPayloadError(
                "Could not locate start of visualizer JSON payload"
            )

        decoder = json.JSONDecoder()
        try:
            payload, _ = decoder.raw_decode(html[start_index:])
        except json.JSONDecodeError as exc:
            raise VisualizerPayloadError(
                f"Failed to decode visualizer JSON payload for marker '{marker}'"
            ) from exc

        return VisualizerPayload(marker=marker, data=payload)

    raise VisualizerPayloadError("Could not find a supported Visualizer payload marker")


def _resolve_chart_key(charts: dict[str, Any], chart_id: str) -> str | None:
    if chart_id in charts:
        return chart_id

    for key in charts:
        if key.startswith(f"visualizer-{chart_id}-"):
            return key

    return None


def _extract_headers(chart: dict[str, Any], rows: list[list[Any]]) -> list[str]:
    series = chart.get("series") or []
    if series:
        return [str(item.get("label", "")).strip() for item in series]

    if rows:
        return [str(value).strip() for value in rows[0]]

    return []


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    return text or None


def _parse_int(value: Any) -> int | None:
    text = _clean_text(value)
    if text is None:
        return None

    try:
        return int(text)
    except ValueError:
        return None


def _parse_date(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None

    return datetime.strptime(text, "%d/%m/%Y").date().isoformat()
