# Chicago Crime Analytics

A local, open-source data lake and analytics UI for the City of Chicago "Crimes - 2001 to Present" dataset. Ingestion runs on demand against the Socrata SODA API and stores a partitioned Parquet lake on disk. The Dash app reads only the local Parquet lake.

## Setup (local)

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
```

Run a one-off ingest:

```bash
python -m chicago_crime.ingest.ingest_crimes --once
```

Run the Dash app:

```bash
python scripts/run_app.py
```

Startup/shutdown helpers (Docker-first):

```bash
./scripts/startup.sh
./scripts/shutdown.sh
```

## Docker workflow

Build images:

```bash
docker compose build
```

Run the app:

```bash
docker compose up
```

Run ingest on demand:

```bash
docker compose run --rm chicago_crime_ingest
```

Optional loop mode for ingest:

```bash
CHI_INGEST_LOOP=1 CHI_INGEST_INTERVAL_HOURS=24 docker compose run --rm chicago_crime_ingest
```

For production scheduling, run the ingest command from the host (cron, systemd timers, GitHub Actions runner) and keep the `data/` directory persistent.

## Data lake layout

Partitioned by year/month/day under `./data/lake/crimes/`:

```
data/
  lake/
    crimes/
      year=YYYY/
        month=MM/
          day=DD/
            *.parquet
  state/
    ingest_state.json
  staging/
```

## Community areas GeoJSON cache

The app uses Chicago Community Area boundaries for choropleths. The GeoJSON and a matching dimension table are cached under:

```
data/
  dim/
    community_areas/
      community_areas.geojson
      community_areas.parquet
```

Refresh boundaries manually:

```bash
python -m chicago_crime.ingest.ingest_dimensions --force
```

The choropleth joins crimes to boundaries via `feature["id"]` derived from the `area_num_1` property and matches it to `community_area`.

## Map modes

The dashboard supports an auto mode that chooses points for small ranges and choropleth for larger ranges. You can manually switch between points and choropleth in the sidebar controls.

## Configuration

Environment variables (see `.env.example`):

- `CHI_CRIME_DATASET_ID` (default `ijzp-q8t2`)
- `SODA_APP_TOKEN` (optional, recommended for higher rate limits)
- `START_DATE` (ISO timestamp, default `today - 365 days` if lake is empty)
- `BACKFILL_DAYS` (default `14`)
- `PAGE_LIMIT` (default `50000`)
- `DATA_DIR` (default `./data`)
- `LOG_LEVEL` (default `INFO`)
- `DASH_HOST` (default `0.0.0.0`)
- `DASH_PORT` (default `8050`)
- `MAX_MAP_POINTS` (default `25000`)
- `MAP_MAX_DAYS_POINTS` (default `90`)
- `COMMUNITY_AREAS_DATASET_ID` (default `igwz-8jzy`)
- `COMMUNITY_AREAS_GEOJSON_URL` (GeoJSON export endpoint)
- `COMMUNITY_AREA_NUMBER_FIELD` (default `area_num_1`)
- `COMMUNITY_AREA_NAME_FIELD` (default `community`)
- `DIM_MAX_AGE_DAYS` (default `30`)
- `MAP_MODE_DEFAULT` (default `auto`)
- `CHOROPLETH_METRIC_DEFAULT` (default `count`)

## Notes on Socrata rate limits

The SODA API enforces rate limits. Set `SODA_APP_TOKEN` to increase throughput. The ingest client retries transient failures (429/5xx) with exponential backoff.

## Troubleshooting

- **Empty UI**: run ingest first and verify `data/lake/crimes/` contains Parquet files.
- **Map too dense**: the app automatically downsamples if the range is large or points exceed `MAX_MAP_POINTS`.
- **Choropleth missing**: ensure `data/dim/community_areas/community_areas.geojson` exists by running the dimension ingest.
- **No ingest state**: `data/state/ingest_state.json` is written after a successful ingest.
