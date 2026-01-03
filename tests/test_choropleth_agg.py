from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from chicago_crime import config
from chicago_crime.analytics import queries
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


def test_community_area_counts(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    lake_dir = data_dir / "lake" / "crimes"
    dim_dir = data_dir / "dim" / "community_areas"
    dim_dir.mkdir(parents=True, exist_ok=True)
    _reset_settings(monkeypatch, data_dir)

    dim_df = pd.DataFrame(
        {
            "community_area": [1, 2],
            "community_area_name": ["Rogers Park", "West Ridge"],
        }
    )
    dim_df.to_parquet(dim_dir / "community_areas.parquet", index=False)

    crimes = pd.DataFrame(
        {
            "id": ["1", "2", "3"],
            "date": [
                datetime(2024, 3, 1, 10, tzinfo=timezone.utc),
                datetime(2024, 3, 2, 11, tzinfo=timezone.utc),
                datetime(2024, 3, 3, 12, tzinfo=timezone.utc),
            ],
            "community_area": [1, 2, 2],
            "primary_type": ["THEFT", "BATTERY", "THEFT"],
        }
    )
    _write_partitioned(lake_dir, crimes)

    min_date, max_date = queries.get_available_date_range()
    result = queries.community_area_counts(min_date, max_date, None, None, None, None)
    assert {"community_area", "community_area_name", "crime_count"}.issubset(result.columns)
    counts = {row["community_area"]: row["crime_count"] for _, row in result.iterrows()}
    assert counts[1] == 1
    assert counts[2] == 2
