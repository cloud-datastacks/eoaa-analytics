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
`eoaa_data.building_application_status` table.

The loader uses the Greek page:

```text
https://eoaa.org.cy/data-transparent-organization/building-application-status/
```

The initial load ingests all monthly tables currently published on the page.
On later runs, the loader compares the page `article:modified_time` against the
already loaded data and skips the run if the page has not changed. When the page
does change, it re-reads the full published page and merges rows into DuckDB.

This is the safest incremental behavior available from the current source
because the site does not expose row-level update timestamps or a dedicated
incremental API endpoint.

## Query

```bash
duckdb data/eoaa_db.duckdb "select * from eoaa_data.building_application_status limit 5"
```
