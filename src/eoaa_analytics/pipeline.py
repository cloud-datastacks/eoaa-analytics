"""DLT pipeline for loading EOAA data into DuckDB."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import dlt
import duckdb

from eoaa_analytics.extractor import DEFAULT_PAGE_URL, fetch_applications_page

DEFAULT_DATABASE_PATH = Path("data") / "eoaa_db.duckdb"
DEFAULT_DATASET_NAME = "eoaa_data"
DEFAULT_PIPELINE_NAME = "eoaa_building_application_status_gr_v2"
DEFAULT_TABLE_NAME = "building_application_status"
RESOURCE_COLUMNS = {
    "record_id": {"data_type": "text"},
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
    "source_occurrence_index": {"data_type": "bigint"},
    "source_page_modified_at": {"data_type": "text"},
    "source_url": {"data_type": "text"},
    "fetched_at": {"data_type": "text"},
    "row_content_hash": {"data_type": "text"},
}


@dlt.resource(
    name=DEFAULT_TABLE_NAME,
    write_disposition="merge",
    primary_key="record_id",
    columns=RESOURCE_COLUMNS,
)
def building_application_status_resource(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return EOAA application rows for loading."""
    return records


def run_pipeline(
    url: str = DEFAULT_PAGE_URL,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    dataset_name: str = DEFAULT_DATASET_NAME,
    pipeline_name: str = DEFAULT_PIPELINE_NAME,
    force_refresh: bool = False,
):
    """Run the EOAA DLT pipeline and load data into DuckDB."""
    database_path = Path(database_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    page = fetch_applications_page(url=url)
    if (
        not force_refresh
        and page.source_modified_at
        and _is_page_already_loaded(
            database_path=database_path,
            dataset_name=dataset_name,
            table_name=DEFAULT_TABLE_NAME,
            source_modified_at=page.source_modified_at,
        )
    ):
        return {
            "status": "skipped",
            "reason": "source page unchanged",
            "source_page_modified_at": page.source_modified_at,
            "row_count": len(page.records),
            "table_count": page.table_count,
        }

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.duckdb(str(database_path)),
        dataset_name=dataset_name,
    )

    return pipeline.run(building_application_status_resource(page.records))


def _is_page_already_loaded(
    database_path: Path,
    dataset_name: str,
    table_name: str,
    source_modified_at: str,
) -> bool:
    if not database_path.exists():
        return False

    query = f"""
        SELECT max(source_page_modified_at)
        FROM {dataset_name}.{table_name}
    """

    try:
        connection = duckdb.connect(str(database_path), read_only=True)
        try:
            current_value = connection.execute(query).fetchone()[0]
        finally:
            connection.close()
    except duckdb.Error:
        return False

    return current_value == source_modified_at
