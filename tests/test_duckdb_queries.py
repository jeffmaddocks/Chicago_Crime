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


def test_queries_return_expected_columns(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    lake_dir = data_dir / "lake" / "crimes"
    _reset_settings(monkeypatch, data_dir)

    df = pd.DataFrame(
        {
            "id": ["1", "2", "3"],
            "date": [
                datetime(2024, 3, 1, 10, tzinfo=timezone.utc),
                datetime(2024, 3, 2, 11, tzinfo=timezone.utc),
                datetime(2024, 3, 3, 12, tzinfo=timezone.utc),
            ],
            "primary_type": ["THEFT", "BATTERY", "THEFT"],
            "arrest": [True, False, True],
            "domestic": [False, True, False],
            "district": [1, 2, 1],
            "latitude": [41.9, 41.8, 41.85],
            "longitude": [-87.6, -87.7, -87.65],
        }
    )
    _write_partitioned(lake_dir, df)

    min_date, max_date = queries.get_available_date_range()
    assert min_date is not None
    assert max_date is not None

    ts = queries.time_series_counts(min_date, max_date, None, None, None, None)
    assert {"bucket", "count"}.issubset(ts.columns)

    top = queries.top_n_primary_types(min_date, max_date, None, None, None, None)
    assert {"primary_type", "count"}.issubset(top.columns)

    heatmap = queries.dow_hour_heatmap(min_date, max_date, None, None, None, None)
    assert {"dow", "hour", "count"}.issubset(heatmap.columns)

    arrest_rate = queries.arrest_rate_by_type(min_date, max_date, None, None, None, None)
    assert {"primary_type", "arrest_rate"}.issubset(arrest_rate.columns)
