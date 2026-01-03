from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import duckdb
import pandas as pd

from chicago_crime.config import get_settings


def _lake_glob() -> str:
    settings = get_settings()
    return str(settings.lake_dir / "**" / "*.parquet")


def _lake_has_data() -> bool:
    settings = get_settings()
    if not settings.lake_dir.exists():
        return False
    return any(settings.lake_dir.rglob("*.parquet"))


def _community_dim_path() -> str:
    settings = get_settings()
    return str(settings.data_dir / "dim" / "community_areas" / "community_areas.parquet")


def _community_dim_exists() -> bool:
    settings = get_settings()
    return (settings.data_dir / "dim" / "community_areas" / "community_areas.parquet").exists()


def _base_from_clause() -> tuple[str, list]:
    if _community_dim_exists():
        return (
            "FROM read_parquet(?) AS c LEFT JOIN read_parquet(?) AS ca "
            "ON TRY_CAST(c.community_area AS INTEGER) = ca.community_area",
            [_lake_glob(), _community_dim_path()],
        )
    return "FROM read_parquet(?) AS c", [_lake_glob()]


def _community_area_name_expr() -> str:
    if _community_dim_exists():
        return "COALESCE(ca.community_area_name, CONCAT('CA ', TRY_CAST(c.community_area AS VARCHAR)))"
    return "CONCAT('CA ', TRY_CAST(c.community_area AS VARCHAR))"


def get_available_date_range() -> tuple[datetime | None, datetime | None]:
    if not _lake_has_data():
        return None, None
    con = duckdb.connect()
    result = con.execute(
        "SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM read_parquet(?)",
        [_lake_glob()],
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


def _build_filters(
    date_start: datetime | None,
    date_end: datetime | None,
    primary_types: Iterable[str] | None,
    district: str | None,
    arrest: bool | None,
    domestic: bool | None,
) -> tuple[str, list]:
    filters = []
    params: list = []
    if date_start:
        filters.append("c.date >= ?")
        params.append(date_start)
    if date_end:
        filters.append("c.date <= ?")
        params.append(date_end)
    if primary_types:
        primary_list = list(primary_types)
        placeholders = ",".join(["?"] * len(primary_list))
        filters.append(f"c.primary_type IN ({placeholders})")
        params.extend(primary_list)
    if district is not None:
        filters.append("c.district = ?")
        params.append(district)
    if arrest is not None:
        filters.append("c.arrest = ?")
        params.append(arrest)
    if domestic is not None:
        filters.append("c.domestic = ?")
        params.append(domestic)
    clause = " AND ".join(filters)
    if clause:
        clause = "WHERE " + clause
    return clause, params


def _append_condition(clause: str, condition: str) -> str:
    if clause:
        return f"{clause} AND {condition}"
    return f"WHERE {condition}"


def filter_crimes(
    date_start: datetime | None,
    date_end: datetime | None,
    primary_types: Iterable[str] | None,
    district: str | None,
    arrest: bool | None,
    domestic: bool | None,
) -> pd.DataFrame:
    if not _lake_has_data():
        return pd.DataFrame()
    clause, params = _build_filters(date_start, date_end, primary_types, district, arrest, domestic)
    con = duckdb.connect()
    from_clause, base_params = _base_from_clause()
    select_list = "c.*, ca.community_area_name" if _community_dim_exists() else "c.*, NULL AS community_area_name"
    query = f"SELECT {select_list} {from_clause} {clause}"
    df = con.execute(query, base_params + params).fetchdf()
    con.close()
    return df


def time_series_counts(
    date_start: datetime | None,
    date_end: datetime | None,
    primary_types: Iterable[str] | None,
    district: str | None,
    arrest: bool | None,
    domestic: bool | None,
    grain: str = "day",
) -> pd.DataFrame:
    if not _lake_has_data():
        return pd.DataFrame()
    clause, params = _build_filters(date_start, date_end, primary_types, district, arrest, domestic)
    bucket = "date_trunc('day', date)" if grain == "day" else "date_trunc('week', date)"
    from_clause, base_params = _base_from_clause()
    query = (
        f"SELECT {bucket} AS bucket, COUNT(*) AS count "
        f"{from_clause} {clause} GROUP BY 1 ORDER BY 1"
    )
    con = duckdb.connect()
    df = con.execute(query, base_params + params).fetchdf()
    con.close()
    return df


def top_n_primary_types(
    date_start: datetime | None,
    date_end: datetime | None,
    primary_types: Iterable[str] | None,
    district: str | None,
    arrest: bool | None,
    domestic: bool | None,
    n: int = 15,
) -> pd.DataFrame:
    if not _lake_has_data():
        return pd.DataFrame()
    clause, params = _build_filters(date_start, date_end, primary_types, district, arrest, domestic)
    from_clause, base_params = _base_from_clause()
    query = (
        f"SELECT c.primary_type AS primary_type, COUNT(*) AS count "
        f"{from_clause} {clause} GROUP BY 1 ORDER BY 2 DESC LIMIT {n}"
    )
    con = duckdb.connect()
    df = con.execute(query, base_params + params).fetchdf()
    con.close()
    return df


def dow_hour_heatmap(
    date_start: datetime | None,
    date_end: datetime | None,
    primary_types: Iterable[str] | None,
    district: str | None,
    arrest: bool | None,
    domestic: bool | None,
) -> pd.DataFrame:
    if not _lake_has_data():
        return pd.DataFrame()
    clause, params = _build_filters(date_start, date_end, primary_types, district, arrest, domestic)
    from_clause, base_params = _base_from_clause()
    query = (
        "SELECT strftime(c.date, '%w') AS dow, strftime(c.date, '%H') AS hour, COUNT(*) AS count "
        f"{from_clause} {clause} GROUP BY 1, 2 ORDER BY 1, 2"
    )
    con = duckdb.connect()
    df = con.execute(query, base_params + params).fetchdf()
    con.close()
    return df


def arrest_rate_by_type(
    date_start: datetime | None,
    date_end: datetime | None,
    primary_types: Iterable[str] | None,
    district: str | None,
    arrest: bool | None,
    domestic: bool | None,
) -> pd.DataFrame:
    if not _lake_has_data():
        return pd.DataFrame()
    clause, params = _build_filters(date_start, date_end, primary_types, district, arrest, domestic)
    from_clause, base_params = _base_from_clause()
    query = (
        "SELECT c.primary_type AS primary_type, AVG(CASE WHEN c.arrest THEN 1 ELSE 0 END) AS arrest_rate "
        f"{from_clause} {clause} GROUP BY 1 ORDER BY 2 DESC"
    )
    con = duckdb.connect()
    df = con.execute(query, base_params + params).fetchdf()
    con.close()
    return df


def community_area_counts(
    date_start: datetime | None,
    date_end: datetime | None,
    primary_types: Iterable[str] | None,
    district: str | None,
    arrest: bool | None,
    domestic: bool | None,
) -> pd.DataFrame:
    if not _lake_has_data():
        return pd.DataFrame()
    clause, params = _build_filters(date_start, date_end, primary_types, district, arrest, domestic)
    from_clause, base_params = _base_from_clause()
    clause = _append_condition(clause, "c.community_area IS NOT NULL")
    query = (
        "SELECT TRY_CAST(c.community_area AS INTEGER) AS community_area, "
        f"{_community_area_name_expr()} AS community_area_name, COUNT(*) AS crime_count "
        f"{from_clause} {clause} "
        "GROUP BY 1, 2 ORDER BY 3 DESC"
    )
    con = duckdb.connect()
    df = con.execute(query, base_params + params).fetchdf()
    con.close()
    return df


def community_area_arrest_rate(
    date_start: datetime | None,
    date_end: datetime | None,
    primary_types: Iterable[str] | None,
    district: str | None,
    arrest: bool | None,
    domestic: bool | None,
) -> pd.DataFrame:
    if not _lake_has_data():
        return pd.DataFrame()
    clause, params = _build_filters(date_start, date_end, primary_types, district, arrest, domestic)
    from_clause, base_params = _base_from_clause()
    clause = _append_condition(clause, "c.community_area IS NOT NULL")
    query = (
        "SELECT TRY_CAST(c.community_area AS INTEGER) AS community_area, "
        f"{_community_area_name_expr()} AS community_area_name, "
        "SUM(CASE WHEN c.arrest THEN 1 ELSE 0 END)::DOUBLE / COUNT(*) AS arrest_rate, "
        "COUNT(*) AS crime_count "
        f"{from_clause} {clause} "
        "GROUP BY 1, 2 ORDER BY 4 DESC"
    )
    con = duckdb.connect()
    df = con.execute(query, base_params + params).fetchdf()
    con.close()
    return df


def distinct_primary_types() -> list[str]:
    if not _lake_has_data():
        return []
    con = duckdb.connect()
    rows = con.execute(
        "SELECT DISTINCT primary_type FROM read_parquet(?) WHERE primary_type IS NOT NULL ORDER BY 1",
        [_lake_glob()],
    ).fetchall()
    con.close()
    return [row[0] for row in rows if row and row[0]]


def distinct_districts() -> list[str]:
    if not _lake_has_data():
        return []
    con = duckdb.connect()
    rows = con.execute(
        "SELECT DISTINCT district FROM read_parquet(?) WHERE district IS NOT NULL ORDER BY 1",
        [_lake_glob()],
    ).fetchall()
    con.close()
    districts: list[int] = []
    for row in rows:
        if not row or row[0] is None:
            continue
        try:
            districts.append(int(row[0]))
        except (TypeError, ValueError):
            continue
    return districts


def get_available_community_areas() -> list[dict[str, str]]:
    if not _community_dim_exists():
        return []
    con = duckdb.connect()
    rows = con.execute(
        "SELECT community_area, community_area_name FROM read_parquet(?) ORDER BY 1",
        [_community_dim_path()],
    ).fetchall()
    con.close()
    return [
        {"label": name, "value": int(area)}
        for area, name in rows
        if area is not None and name is not None
    ]
