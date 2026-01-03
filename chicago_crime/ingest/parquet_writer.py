from __future__ import annotations

import logging
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


def add_partition_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    dt = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df["date"] = dt
    df["year"] = dt.dt.year.astype("Int64").astype(str)
    df["month"] = dt.dt.month.astype("Int64").astype(str).str.zfill(2)
    df["day"] = dt.dt.day.astype("Int64").astype(str).str.zfill(2)
    return df


def write_staged_parquet(df: pd.DataFrame, staging_dir: Path) -> Path:
    staging_dir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    path = staging_dir / f"crimes_stage_{ts}.parquet"
    df.to_parquet(path, index=False)
    return path


def _partition_dir(lake_dir: Path, year: str, month: str, day: str) -> Path:
    return lake_dir / f"year={year}" / f"month={month}" / f"day={day}"


def _lake_glob(lake_dir: Path) -> str:
    return str(lake_dir / "**" / "*.parquet")


def _lake_has_data(lake_dir: Path) -> bool:
    return lake_dir.exists() and any(lake_dir.rglob("*.parquet"))


def merge_partitions(lake_dir: Path, staged_path: Path) -> tuple[int, datetime | None]:
    con = duckdb.connect()
    staged_df = con.execute(
        "SELECT DISTINCT year, month, day FROM read_parquet(?)",
        [str(staged_path)],
    ).fetchdf()

    touched_df = staged_df.copy()
    if _lake_has_data(lake_dir):
        lake_partitions = con.execute(
            "SELECT DISTINCT year, month, day FROM read_parquet(?) "
            "WHERE id IN (SELECT DISTINCT id FROM read_parquet(?))",
            [_lake_glob(lake_dir), str(staged_path)],
        ).fetchdf()
        if not lake_partitions.empty:
            touched_df = pd.concat([touched_df, lake_partitions]).drop_duplicates()
    con.close()

    total_rows = 0
    max_date: datetime | None = None

    for _, row in touched_df.iterrows():
        year = str(row["year"])
        month = str(row["month"])
        day = str(row["day"])
        partition_dir = _partition_dir(lake_dir, year, month, day)
        existing_glob = str(partition_dir / "*.parquet")
        has_existing = partition_dir.exists() and any(partition_dir.glob("*.parquet"))

        con = duckdb.connect()
        if has_existing:
            query = (
                "SELECT * EXCLUDE (rn) FROM ("
                "SELECT *, row_number() OVER (PARTITION BY id ORDER BY date DESC) AS rn "
                "FROM (SELECT * FROM read_parquet(?) UNION ALL SELECT * FROM read_parquet(?))"
                ") WHERE rn = 1"
            )
            params = [existing_glob, str(staged_path)]
        else:
            query = (
                "SELECT * EXCLUDE (rn) FROM ("
                "SELECT *, row_number() OVER (PARTITION BY id ORDER BY date DESC) AS rn "
                "FROM read_parquet(?)"
                ") WHERE rn = 1"
            )
            params = [str(staged_path)]
        merged_df = con.execute(
            query + " AND year = ? AND month = ? AND day = ?",
            params + [year, month, day],
        ).fetchdf()
        con.close()

        if merged_df.empty:
            if partition_dir.exists():
                shutil.rmtree(partition_dir)
                logger.info("Removed empty partition %s/%s/%s", year, month, day)
            continue

        temp_dir = partition_dir.parent / f".tmp_{partition_dir.name}_{int(time.time())}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        out_path = temp_dir / "part-000.parquet"
        merged_df.to_parquet(out_path, index=False)

        if partition_dir.exists():
            shutil.rmtree(partition_dir)
        temp_dir.rename(partition_dir)

        total_rows += len(merged_df)
        max_partition_date = pd.to_datetime(merged_df["date"], utc=True).max()
        if pd.notna(max_partition_date):
            dt = max_partition_date.to_pydatetime()
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            max_date = dt if max_date is None else max(max_date, dt)

        logger.info("Wrote partition %s/%s/%s with %s rows", year, month, day, len(merged_df))

    return total_rows, max_date
