import logging

from chicago_crime.app.server import main as app_main
from chicago_crime.ingest.ingest_dimensions import ensure_community_areas_dim
from chicago_crime.config import get_settings
from chicago_crime.logging_config import setup_logging

logger = logging.getLogger(__name__)

def _community_dim_paths():
    settings = get_settings()
    dim_dir = settings.data_dir / "dim" / "community_areas"
    return (
        dim_dir / "community_areas.geojson",
        dim_dir / "community_areas.parquet",
    )


def main() -> None:
    setup_logging()
    try:
        geojson_path, dim_path = _community_dim_paths()
        if geojson_path.exists() and dim_path.exists():
            logger.info("Community area data already present; skipping download.")
        else:
            ensure_community_areas_dim()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Community area dim ingest failed: %s", exc)
    app_main()

if __name__ == "__main__":
    main()
