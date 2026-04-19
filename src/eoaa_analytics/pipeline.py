"""DLT pipeline for loading EOAA data into DuckDB."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import dlt

from eoaa_analytics.extractor import (
    DEFAULT_CHART_ID,
    DEFAULT_PAGE_URL,
    fetch_chart,
    normalize_chart_rows,
)

DEFAULT_DATABASE_PATH = Path("data") / "eoaa.duckdb"
DEFAULT_DATASET_NAME = "eoaa_data"
DEFAULT_PIPELINE_NAME = "eoaa_building_application_status"
DEFAULT_TABLE_NAME = "building_application_status"
RESOURCE_COLUMNS = {
    "record_number": {"data_type": "bigint"},
    "application_type_code": {"data_type": "text"},
    "development_type": {"data_type": "text"},
    "application_status": {"data_type": "text"},
    "submission_date": {"data_type": "date"},
    "decision_date": {"data_type": "date"},
    "extra_column_1": {"data_type": "text"},
    "extra_column_2": {"data_type": "text"},
    "extra_column_3": {"data_type": "text"},
    "source_chart_id": {"data_type": "text"},
    "source_table_title": {"data_type": "text"},
    "source_headers_json": {"data_type": "text"},
    "source_url": {"data_type": "text"},
    "source_modified_at": {"data_type": "text"},
    "fetched_at": {"data_type": "text"},
}


@dlt.resource(
    name=DEFAULT_TABLE_NAME,
    write_disposition="replace",
    primary_key="record_number",
    columns=RESOURCE_COLUMNS,
)
def building_application_status_resource(
    url: str = DEFAULT_PAGE_URL,
    chart_id: str = DEFAULT_CHART_ID,
) -> list[dict[str, Any]]:
    """Return normalized EOAA building application status rows."""
    chart = fetch_chart(url=url, chart_id=chart_id)
    return normalize_chart_rows(chart)


def run_pipeline(
    url: str = DEFAULT_PAGE_URL,
    chart_id: str = DEFAULT_CHART_ID,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    dataset_name: str = DEFAULT_DATASET_NAME,
    pipeline_name: str = DEFAULT_PIPELINE_NAME,
):
    """Run the EOAA DLT pipeline and load data into DuckDB."""
    database_path = Path(database_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.duckdb(str(database_path)),
        dataset_name=dataset_name,
    )

    return pipeline.run(building_application_status_resource(url=url, chart_id=chart_id))
