from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from chicago_crime.ingest.parquet_writer import add_partition_columns, merge_partitions, write_staged_parquet


def _write_partition(lake_dir: Path, df: pd.DataFrame) -> None:
    partitioned = add_partition_columns(df)
    for (year, month, day), group in partitioned.groupby(["year", "month", "day"]):
        partition_dir = lake_dir / f"year={year}" / f"month={month}" / f"day={day}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        group.to_parquet(partition_dir / "part-000.parquet", index=False)


def test_merge_partitions_dedupes_across_partitions(tmp_path: Path) -> None:
    lake_dir = tmp_path / "lake" / "crimes"
    staging_dir = tmp_path / "staging"

    existing = pd.DataFrame(
        {
            "id": ["1", "3"],
            "date": [
                datetime(2024, 1, 1, 8, tzinfo=timezone.utc),
                datetime(2024, 1, 1, 9, tzinfo=timezone.utc),
            ],
            "primary_type": ["THEFT", "BATTERY"],
        }
    )
    _write_partition(lake_dir, existing)

    staged = pd.DataFrame(
        {
            "id": ["1", "2"],
            "date": [
                datetime(2024, 2, 1, 10, tzinfo=timezone.utc),
                datetime(2024, 2, 1, 11, tzinfo=timezone.utc),
            ],
            "primary_type": ["THEFT", "ROBBERY"],
        }
    )
    staged = add_partition_columns(staged)
    staged_path = write_staged_parquet(staged, staging_dir)

    merge_partitions(lake_dir, staged_path)

    con = duckdb.connect()
    result = con.execute(
        "SELECT id, MAX(date) AS max_date, COUNT(*) AS cnt FROM read_parquet(?) GROUP BY id",
        [str(lake_dir / "**" / "*.parquet")],
    ).fetchall()
    con.close()

    rows = {row[0]: (row[1], row[2]) for row in result}
    assert rows["1"][1] == 1
    assert rows["1"][0] == datetime(2024, 2, 1, 10, tzinfo=timezone.utc)
    assert rows["2"][1] == 1
    assert rows["3"][1] == 1
