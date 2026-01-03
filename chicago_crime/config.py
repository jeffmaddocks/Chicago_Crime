from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DEFAULT_DATASET_ID = "ijzp-q8t2"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class Settings:
    dataset_id: str
    soda_app_token: str | None
    start_date: datetime
    backfill_days: int
    page_limit: int
    data_dir: Path
    log_level: str
    dash_host: str
    dash_port: int
    max_map_points: int
    map_max_days_points: int
    community_areas_dataset_id: str
    community_areas_geojson_url: str
    community_area_number_field: str
    community_area_name_field: str
    dim_max_age_days: int
    acs_most_recent_dataset_id: str
    acs_multiyear_dataset_id: str
    use_acs_multiyear: bool
    acs_community_area_field: str | None
    acs_year_field: str | None
    acs_population_field: str | None
    acs_dim_max_age_days: int
    map_mode_default: str
    choropleth_metric_default: str

    @property
    def lake_dir(self) -> Path:
        return self.data_dir / "lake" / "crimes"

    @property
    def state_path(self) -> Path:
        return self.data_dir / "state" / "ingest_state.json"

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "staging"

    @property
    def population_dim_path(self) -> Path:
        return self.data_dir / "dim" / "population" / "community_area_population.parquet"

    @property
    def acs_dim_path(self) -> Path:
        return self.data_dir / "dim" / "acs_demographics" / "acs_demographics.parquet"


_SETTINGS: Settings | None = None


def get_settings() -> Settings:
    global _SETTINGS
    if _SETTINGS is not None:
        return _SETTINGS

    data_dir = Path(os.getenv("DATA_DIR", "./data")).resolve()
    start_date_env = _parse_date(os.getenv("START_DATE"))
    if start_date_env is None:
        start_date_env = _utc_now() - timedelta(days=365)

    settings = Settings(
        dataset_id=os.getenv("CHI_CRIME_DATASET_ID", DEFAULT_DATASET_ID),
        soda_app_token=os.getenv("SODA_APP_TOKEN"),
        start_date=start_date_env,
        backfill_days=int(os.getenv("BACKFILL_DAYS", "14")),
        page_limit=int(os.getenv("PAGE_LIMIT", "50000")),
        data_dir=data_dir,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        dash_host=os.getenv("DASH_HOST", "0.0.0.0"),
        dash_port=int(os.getenv("DASH_PORT", "8050")),
        max_map_points=int(os.getenv("MAX_MAP_POINTS", "25000")),
        map_max_days_points=int(os.getenv("MAP_MAX_DAYS_POINTS", "90")),
        community_areas_dataset_id=os.getenv("COMMUNITY_AREAS_DATASET_ID", "igwz-8jzy"),
        community_areas_geojson_url=os.getenv(
            "COMMUNITY_AREAS_GEOJSON_URL",
            "https://data.cityofchicago.org/api/geospatial/igwz-8jzy?method=export&format=GeoJSON",
        ),
        community_area_number_field=os.getenv("COMMUNITY_AREA_NUMBER_FIELD", "area_num_1"),
        community_area_name_field=os.getenv("COMMUNITY_AREA_NAME_FIELD", "community"),
        dim_max_age_days=int(os.getenv("DIM_MAX_AGE_DAYS", "30")),
        acs_most_recent_dataset_id=os.getenv("ACS_MOST_RECENT_DATASET_ID", "7umk-8dtw"),
        acs_multiyear_dataset_id=os.getenv("ACS_MULTIYEAR_DATASET_ID", "t68z-cikk"),
        use_acs_multiyear=os.getenv("USE_ACS_MULTIYEAR", "0") == "1",
        acs_community_area_field=os.getenv("ACS_COMMUNITY_AREA_FIELD") or None,
        acs_year_field=os.getenv("ACS_YEAR_FIELD") or None,
        acs_population_field=os.getenv("ACS_POPULATION_FIELD") or None,
        acs_dim_max_age_days=int(os.getenv("ACS_DIM_MAX_AGE_DAYS", "30")),
        map_mode_default=os.getenv("MAP_MODE_DEFAULT", "choropleth"),
        choropleth_metric_default=os.getenv("CHOROPLETH_METRIC_DEFAULT", "count"),
    )
    _SETTINGS = settings
    return settings
