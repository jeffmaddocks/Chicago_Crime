import logging

from chicago_crime.ingest.ingest_acs import ensure_acs_demographics_dim, ensure_population_dim
from chicago_crime.ingest.ingest_dimensions import ensure_community_areas_dim
from chicago_crime.logging_config import setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Ingest all dimension datasets")
    parser.add_argument("--force", action="store_true", help="Force refresh of cached dims")
    parser.add_argument("--max-age-days", type=int, default=None, help="Override max age for cached dims")
    args = parser.parse_args()

    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    force = args.force or os.getenv("FORCE", "0") == "1"

    for name, func in (
        ("community areas", ensure_community_areas_dim),
        ("population", ensure_population_dim),
        ("ACS demographics", ensure_acs_demographics_dim),
    ):
        try:
            func(force=force, max_age_days=args.max_age_days)
        except Exception as exc:  # noqa: BLE001
            logger.warning("%s dim ingest failed: %s", name, exc)


if __name__ == "__main__":
    main()
