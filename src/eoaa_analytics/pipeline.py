"""DLT pipeline for loading EOAA data into DuckDB."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import dlt

from eoaa_analytics.extractor import DEFAULT_PAGE_URL, fetch_applications_page

DEFAULT_DATABASE_PATH = Path("data") / "eoaa_db.duckdb"
DEFAULT_APPLICATION_TYPES_CSV_PATH = Path("data") / "application_types.csv"
DEFAULT_DATASET_NAME = "eoaa_data"
DEFAULT_PIPELINE_NAME = "eoaa_building_application_status"
DEFAULT_TABLE_NAME = "building_application_status"
APPLICATION_TYPES_TABLE_NAME = "application_types"

RESOURCE_COLUMNS = {
    "application_type": {"data_type": "text"},
    "original_application_type": {"data_type": "text"},
    "application_description": {"data_type": "text"},
    "status": {"data_type": "text"},
    "sub_status": {"data_type": "text"},
    "received_date": {"data_type": "date"},
    "completion_date": {"data_type": "date"},
    "source_month": {"data_type": "text"},
    "source_table_index": {"data_type": "bigint"},
    "source_row_index": {"data_type": "bigint"},
    "source_headers_json": {"data_type": "text"},
    "source_page_modified_at": {"data_type": "text"},
    "source_url": {"data_type": "text"},
    "fetched_at": {"data_type": "text"},
    "row_content_hash": {"data_type": "text"},
}

APPLICATION_TYPES_COLUMNS = {
    "type": {"data_type": "text"},
    "description_en": {"data_type": "text"},
    "description_gr": {"data_type": "text"},
}


@dlt.resource(
    name=DEFAULT_TABLE_NAME,
    write_disposition="replace",
    columns=RESOURCE_COLUMNS,
)
def building_application_status_resource(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return EOAA application rows for loading."""
    return records


@dlt.resource(
    name=APPLICATION_TYPES_TABLE_NAME,
    write_disposition="replace",
    columns=APPLICATION_TYPES_COLUMNS,
)
def application_types_resource(
    records: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Return application type reference rows for loading."""
    return records


def run_pipeline(
    url: str = DEFAULT_PAGE_URL,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    application_types_csv_path: str | Path = DEFAULT_APPLICATION_TYPES_CSV_PATH,
    dataset_name: str = DEFAULT_DATASET_NAME,
    pipeline_name: str = DEFAULT_PIPELINE_NAME,
):
    """Run the EOAA DLT pipeline and load data into DuckDB."""
    database_path = Path(database_path)
    application_types_csv_path = Path(application_types_csv_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    page = fetch_applications_page(url=url)
    application_types = load_application_types_csv(application_types_csv_path)

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.duckdb(str(database_path)),
        dataset_name=dataset_name,
    )

    return pipeline.run(
        [
            building_application_status_resource(page.records),
            application_types_resource(application_types),
        ]
    )


def load_application_types_csv(csv_path: str | Path) -> list[dict[str, str]]:
    """Load the local application types reference CSV."""
    csv_path = Path(csv_path)
    with csv_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        expected_fields = ["type", "description_en", "description_gr"]
        if reader.fieldnames != expected_fields:
            raise ValueError(f"Unexpected application types CSV headers: {reader.fieldnames}")
        return [
            {
                "type": (row["type"] or "").strip(),
                "description_en": (row["description_en"] or "").strip(),
                "description_gr": (row["description_gr"] or "").strip(),
            }
            for row in reader
        ]
