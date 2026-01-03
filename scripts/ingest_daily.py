import logging

from chicago_crime.ingest.ingest_crimes import main as ingest_main
from chicago_crime.ingest.ingest_dimensions import ensure_community_areas_dim
from chicago_crime.logging_config import setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    setup_logging()
    try:
        ensure_community_areas_dim()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Community area dim ingest failed: %s", exc)
    ingest_main()

if __name__ == "__main__":
    main()
