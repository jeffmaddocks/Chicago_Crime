from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

from chicago_crime.config import get_settings
from chicago_crime.logging_config import setup_logging

logger = logging.getLogger(__name__)


def _duckdb_path(settings) -> Path:
    return settings.lake_dir.parent / "chicago_crime.duckdb"


def _lake_glob(settings) -> str:
    return str(settings.lake_dir / "**" / "*.parquet")


def _community_areas_path(settings) -> Path:
    return settings.data_dir / "dim" / "community_areas" / "community_areas.parquet"


def _population_path(settings) -> Path:
    return settings.population_dim_path


def _acs_path(settings) -> Path:
    return settings.acs_dim_path


def _escape_path(path: str) -> str:
    return path.replace("\\", "/").replace("'", "''")


def _get_columns(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> set[str]:
    rows = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)",
        [str(parquet_path)],
    ).fetchall()
    return {row[0] for row in rows}


def _create_parquet_view(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    parquet_path: Path,
) -> set[str]:
    escaped = _escape_path(str(parquet_path))
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{escaped}')")
    return _get_columns(con, parquet_path)


def build_duckdb(rebuild: bool = False) -> Path:
    settings = get_settings()
    setup_logging(settings.log_level)

    lake_files = list(settings.lake_dir.rglob("*.parquet"))
    if not lake_files:
        raise ValueError(f"No parquet files found under {settings.lake_dir}")

    db_path = _duckdb_path(settings)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if rebuild and db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))

    lake_glob = _escape_path(_lake_glob(settings))
    con.execute(
        f"CREATE OR REPLACE VIEW crimes AS SELECT * FROM read_parquet('{lake_glob}')"
    )

    community_cols: set[str] | None = None
    population_cols: set[str] | None = None
    acs_cols: set[str] | None = None

    community_path = _community_areas_path(settings)
    if community_path.exists():
        community_cols = _create_parquet_view(con, "community_areas", community_path)
    else:
        logger.info("Community areas dim not found at %s", community_path)

    population_path = _population_path(settings)
    if population_path.exists():
        population_cols = _create_parquet_view(con, "population", population_path)
    else:
        logger.info("Population dim not found at %s", population_path)

    acs_path = _acs_path(settings)
    if acs_path.exists():
        acs_cols = _create_parquet_view(con, "acs_demographics", acs_path)
    else:
        logger.info("ACS demographics dim not found at %s", acs_path)

    joins = []
    select_parts = ["c.*"]

    if community_cols and "community_area" in community_cols:
        joins.append(
            "LEFT JOIN community_areas ca "
            "ON TRY_CAST(c.community_area AS INTEGER) = TRY_CAST(ca.community_area AS INTEGER)"
        )
        if "community_area_name" in community_cols:
            select_parts.append("ca.community_area_name AS community_area_name")
        else:
            logger.warning("community_areas missing community_area_name column")
            select_parts.append("NULL AS community_area_name")
    else:
        if community_cols is not None:
            logger.warning("community_areas missing community_area column")
        select_parts.append("NULL AS community_area_name")

    if population_cols and "community_area" in population_cols:
        joins.append(
            "LEFT JOIN population p "
            "ON TRY_CAST(c.community_area AS INTEGER) = TRY_CAST(p.community_area AS INTEGER)"
        )
        if "population" in population_cols:
            select_parts.append("p.population AS population")
        else:
            logger.warning("population dim missing population column")
            select_parts.append("NULL AS population")
    else:
        if population_cols is not None:
            logger.warning("population dim missing community_area column")
        select_parts.append("NULL AS population")

    if acs_cols and "community_area" in acs_cols:
        acs_extra = sorted(col for col in acs_cols if col != "community_area")
        if acs_extra:
            joins.append(
                "LEFT JOIN acs_demographics a "
                "ON TRY_CAST(c.community_area AS INTEGER) = TRY_CAST(a.community_area AS INTEGER)"
            )
            select_parts.append("a.* EXCLUDE (community_area)")
        else:
            logger.warning("acs_demographics has no extra columns beyond community_area")
    else:
        if acs_cols is not None:
            logger.warning("acs_demographics missing community_area column")

    select_list = ", ".join(select_parts)
    join_clause = " ".join(joins)
    con.execute(
        f"CREATE OR REPLACE VIEW crimes_enriched AS "
        f"SELECT {select_list} FROM crimes c {join_clause}"
    )

    con.close()
    logger.info("DuckDB bridge written to %s", db_path)
    return db_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build DuckDB bridge for Superset")
    parser.add_argument("--rebuild", action="store_true", help="Recreate the DuckDB file from scratch")
    args = parser.parse_args()
    build_duckdb(rebuild=args.rebuild)


if __name__ == "__main__":
    main()
