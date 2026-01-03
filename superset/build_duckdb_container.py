from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _escape_path(path: str) -> str:
    return path.replace("\\", "/").replace("'", "''")


def _lake_glob(data_dir: Path) -> str:
    return str(data_dir / "lake" / "crimes" / "**" / "*.parquet")


def _dim_path(data_dir: Path, *parts: str) -> Path:
    return data_dir.joinpath("dim", *parts)


def build_duckdb(data_dir: Path, rebuild: bool = False) -> Path:
    lake_glob = _lake_glob(data_dir)
    if not Path(lake_glob.split("**")[0]).exists():
        raise ValueError(f"No parquet files found under {data_dir / 'lake' / 'crimes'}")

    db_path = data_dir / "lake" / "chicago_crime.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if rebuild and db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))
    con.execute(
        f"CREATE OR REPLACE VIEW crimes AS SELECT * FROM read_parquet('{_escape_path(lake_glob)}')"
    )

    community_path = _dim_path(data_dir, "community_areas", "community_areas.parquet")
    if community_path.exists():
        con.execute(
            "CREATE OR REPLACE VIEW community_areas AS "
            f"SELECT * FROM read_parquet('{_escape_path(str(community_path))}')"
        )

    population_path = _dim_path(data_dir, "population", "community_area_population.parquet")
    if population_path.exists():
        con.execute(
            "CREATE OR REPLACE VIEW population AS "
            f"SELECT * FROM read_parquet('{_escape_path(str(population_path))}')"
        )

    acs_path = _dim_path(data_dir, "acs_demographics", "acs_demographics.parquet")
    if acs_path.exists():
        con.execute(
            "CREATE OR REPLACE VIEW acs_demographics AS "
            f"SELECT * FROM read_parquet('{_escape_path(str(acs_path))}')"
        )

    join_parts = ["FROM crimes c"]
    select_parts = ["c.*"]

    if community_path.exists():
        join_parts.append(
            "LEFT JOIN community_areas ca "
            "ON TRY_CAST(c.community_area AS INTEGER) = TRY_CAST(ca.community_area AS INTEGER)"
        )
        select_parts.append("ca.community_area_name AS community_area_name")
    else:
        select_parts.append("NULL AS community_area_name")

    if population_path.exists():
        join_parts.append(
            "LEFT JOIN population p "
            "ON TRY_CAST(c.community_area AS INTEGER) = TRY_CAST(p.community_area AS INTEGER)"
        )
        select_parts.append("p.population AS population")
    else:
        select_parts.append("NULL AS population")

    if acs_path.exists():
        join_parts.append(
            "LEFT JOIN acs_demographics a "
            "ON TRY_CAST(c.community_area AS INTEGER) = TRY_CAST(a.community_area AS INTEGER)"
        )
        select_parts.append("a.* EXCLUDE (community_area)")

    con.execute(
        "CREATE OR REPLACE VIEW crimes_enriched AS "
        f"SELECT {', '.join(select_parts)} {' '.join(join_parts)}"
    )

    con.close()
    logging.info("DuckDB bridge written to %s", db_path)
    return db_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build DuckDB bridge inside container")
    parser.add_argument("--rebuild", action="store_true", help="Recreate the DuckDB file from scratch")
    parser.add_argument("--data-dir", default="/data", help="Base data directory inside container")
    args = parser.parse_args()
    build_duckdb(Path(args.data_dir), rebuild=args.rebuild)


if __name__ == "__main__":
    main()
