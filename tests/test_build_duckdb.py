from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from chicago_crime import config
from chicago_crime.ingest import build_duckdb
from chicago_crime.ingest.parquet_writer import add_partition_columns


def _reset_settings(monkeypatch, data_dir: Path) -> None:
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    config._SETTINGS = None


def _write_partitioned(lake_dir: Path, df: pd.DataFrame) -> None:
    partitioned = add_partition_columns(df)
    for (year, month, day), group in partitioned.groupby(["year", "month", "day"]):
        partition_dir = lake_dir / f"year={year}" / f"month={month}" / f"day={day}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        group.to_parquet(partition_dir / "part-000.parquet", index=False)


def test_build_duckdb_creates_views(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    lake_dir = data_dir / "lake" / "crimes"
    _reset_settings(monkeypatch, data_dir)

    crimes_df = pd.DataFrame(
        {
            "id": ["1", "2"],
            "date": [
                datetime(2024, 3, 1, 10, tzinfo=timezone.utc),
                datetime(2024, 3, 2, 11, tzinfo=timezone.utc),
            ],
            "primary_type": ["THEFT", "BATTERY"],
            "arrest": [True, False],
            "domestic": [False, True],
            "district": [1, 2],
            "community_area": [1, 1],
        }
    )
    _write_partitioned(lake_dir, crimes_df)

    community_dir = data_dir / "dim" / "community_areas"
    community_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "community_area": [1],
            "community_area_name": ["Rogers Park"],
        }
    ).to_parquet(community_dir / "community_areas.parquet", index=False)

    population_dir = data_dir / "dim" / "population"
    population_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "community_area": [1],
            "population": [55000],
        }
    ).to_parquet(population_dir / "community_area_population.parquet", index=False)

    db_path = build_duckdb.build_duckdb(rebuild=True)
    assert db_path.exists()

    con = duckdb.connect(str(db_path))
    count = con.execute("SELECT COUNT(*) FROM crimes").fetchone()[0]
    assert count == 2

    enriched = con.execute(
        "SELECT community_area_name, population FROM crimes_enriched ORDER BY id"
    ).fetchall()
    assert enriched == [("Rogers Park", 55000), ("Rogers Park", 55000)]
    con.close()
