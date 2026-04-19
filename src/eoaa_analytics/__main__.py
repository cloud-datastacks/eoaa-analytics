"""Command-line entrypoint for the EOAA loader."""

from __future__ import annotations

import argparse

from eoaa_analytics.extractor import DEFAULT_PAGE_URL
from eoaa_analytics.pipeline import (
    DEFAULT_DATABASE_PATH,
    DEFAULT_DATASET_NAME,
    DEFAULT_PIPELINE_NAME,
    run_pipeline,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Load EOAA building application status data into DuckDB with dlt."
    )
    parser.add_argument("--url", default=DEFAULT_PAGE_URL, help="Source EOAA page URL.")
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DATABASE_PATH),
        help="Path to the DuckDB database file.",
    )
    parser.add_argument(
        "--dataset-name",
        default=DEFAULT_DATASET_NAME,
        help="DuckDB schema name created by dlt.",
    )
    parser.add_argument(
        "--pipeline-name",
        default=DEFAULT_PIPELINE_NAME,
        help="dlt pipeline name.",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Ignore the stored page modified timestamp and reload the source.",
    )
    return parser


def main() -> int:
    """Run the EOAA DLT pipeline from the command line."""
    args = build_parser().parse_args()
    load_info = run_pipeline(
        url=args.url,
        database_path=args.db_path,
        dataset_name=args.dataset_name,
        pipeline_name=args.pipeline_name,
        force_refresh=args.force_refresh,
    )
    print(load_info)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
