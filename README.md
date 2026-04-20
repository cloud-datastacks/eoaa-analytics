# eoaa-analytics

Loads the EOAA Greek `Κατάσταση Πολεοδομικών Αιτήσεων` monthly tables into DuckDB with `dlt`.

## Install

```bash
uv sync --active
```

## Run

```bash
python -m eoaa_analytics
```

Or:

```bash
eoaa-load-building-status
```

By default this writes to `data/eoaa_db.duckdb` and loads the table into the
`eoaa_data.building_application_status` and `eoaa_data.application_types` tables.

The loader uses the Greek page:

```text
https://eoaa.org.cy/data-transparent-organization/building-application-status/
```

Every run re-reads the full published page and replaces the target DuckDB table.
The same run also loads the local `data/application_types.csv` reference file into
`eoaa_data.application_types`.

## Query

```bash
duckdb data/eoaa_db.duckdb "select * from eoaa_data.building_application_status limit 5"
```

## Streamlit dashboard

```bash
streamlit run app.py
```

The first page shows the number of applications per year from
`eoaa_data.building_application_status`, grouped by `received_date`.
