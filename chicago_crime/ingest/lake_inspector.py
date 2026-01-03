from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb


def _parquet_files_exist(lake_path_glob: str) -> bool:
    root = Path(lake_path_glob.split("*")[0]).resolve()
    if not root.exists():
        return False
    return any(root.rglob("*.parquet"))


def get_max_date_from_lake(lake_path_glob: str) -> datetime | None:
    if not _parquet_files_exist(lake_path_glob):
        return None
    con = duckdb.connect()
    result = con.execute(
        "SELECT MAX(date) AS max_date FROM read_parquet(?)",
        [lake_path_glob],
    ).fetchone()
    con.close()
    max_date = result[0] if result else None
    if max_date is None:
        return None
    if isinstance(max_date, datetime):
        if max_date.tzinfo is None:
            max_date = max_date.replace(tzinfo=timezone.utc)
        return max_date.astimezone(timezone.utc)
    return None


def get_available_date_range(lake_path_glob: str) -> tuple[datetime | None, datetime | None]:
    if not _parquet_files_exist(lake_path_glob):
        return None, None
    con = duckdb.connect()
    result = con.execute(
        "SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM read_parquet(?)",
        [lake_path_glob],
    ).fetchone()
    con.close()
    if not result:
        return None, None
    min_date, max_date = result
    if isinstance(min_date, datetime) and min_date.tzinfo is None:
        min_date = min_date.replace(tzinfo=timezone.utc)
    if isinstance(max_date, datetime) and max_date.tzinfo is None:
        max_date = max_date.replace(tzinfo=timezone.utc)
    return (
        min_date.astimezone(timezone.utc) if isinstance(min_date, datetime) else None,
        max_date.astimezone(timezone.utc) if isinstance(max_date, datetime) else None,
    )
