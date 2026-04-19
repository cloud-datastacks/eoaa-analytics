# eoaa-analytics

Loads the EOAA "Status of Planning Applications" Visualizer table into DuckDB with `dlt`.

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

By default this writes to `data/eoaa.duckdb` and loads the table into the
`eoaa_data.building_application_status` table.

## Query

```bash
duckdb data/eoaa.duckdb "select * from eoaa_data.building_application_status limit 5"
```
