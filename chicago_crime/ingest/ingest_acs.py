from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

from chicago_crime.config import get_settings
from chicago_crime.ingest.soda_client import SodaClient
from chicago_crime.logging_config import setup_logging

logger = logging.getLogger(__name__)

COMMUNITY_AREA_CANDIDATES = [
    "community_area",
    "communityarea",
    "community_area_number",
    "community_area_num",
    "community_area_id",
    "community_area_no",
    "community_area_",
]

POPULATION_CANDIDATES = [
    "total_population",
    "population",
    "pop",
    "tot_pop",
    "totalpop",
    "pop_total",
]

YEAR_CANDIDATES = ["year", "acs_year", "vintage", "acs_release", "period", "time_period"]


def _dim_is_fresh(path: Path, max_age_days: int) -> bool:
    if not path.exists():
        return False
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    age_days = (datetime.now(timezone.utc) - mtime).days
    return age_days <= max_age_days


def _infer_field(
    keys: Iterable[str],
    candidates: list[str],
    override: str | None,
    env_var: str,
    field_label: str,
) -> str:
    key_list = list(keys)
    key_map = {key.lower(): key for key in key_list}
    if override:
        match = key_map.get(override.lower())
        if match:
            return match
        raise ValueError(
            f"Override {env_var}={override!r} not found. Keys: {sorted(key_list)}"
        )
    for candidate in candidates:
        match = key_map.get(candidate.lower())
        if match:
            return match
    raise ValueError(
        f"Unable to infer {field_label} field from keys: {sorted(key_list)}. "
        f"Set {env_var} to override."
    )


def _fetch_sample(dataset_id: str) -> dict:
    client = SodaClient()
    rows = client.fetch_rows(dataset_id, limit=1)
    if not rows:
        raise ValueError(f"No rows returned from dataset {dataset_id}")
    return rows[0]


def _fetch_all_rows(dataset_id: str) -> list[dict]:
    client = SodaClient()
    rows: list[dict] = []
    offset = 0
    limit = client.settings.page_limit
    while True:
        batch = client.fetch_rows(dataset_id, limit=limit, offset=offset)
        if not batch:
            break
        rows.extend(batch)
        offset += limit
    return rows


def _normalize_community_area(series: pd.Series) -> pd.Series:
    numeric = _coerce_numeric(series)
    lookup = _community_area_lookup()
    if lookup:
        normalized = series.astype(str).str.strip().str.lower()
        mapped = normalized.map(lookup)
        numeric = numeric.where(numeric.notna(), mapped)
    return numeric


def _coerce_numeric(series: pd.Series) -> pd.Series:
    if series.dtype == object:
        cleaned = (
            series.astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
            .str.strip()
        )
        cleaned = cleaned.replace({"": None, "nan": None})
        return pd.to_numeric(cleaned, errors="coerce")
    return pd.to_numeric(series, errors="coerce")


def _community_area_lookup() -> dict[str, int]:
    settings = get_settings()
    dim_path = settings.data_dir / "dim" / "community_areas" / "community_areas.parquet"
    if not dim_path.exists():
        return {}
    df = pd.read_parquet(dim_path)
    if "community_area" not in df.columns or "community_area_name" not in df.columns:
        return {}
    names = df["community_area_name"].astype(str).str.strip().str.lower()
    mapping: dict[str, int] = {}
    for name, area in zip(names, df["community_area"], strict=False):
        if pd.isna(name) or pd.isna(area):
            continue
        if name in mapping:
            continue
        try:
            mapping[name] = int(area)
        except (TypeError, ValueError):
            continue
    return mapping


def ensure_population_dim(force: bool = False, max_age_days: int | None = None) -> Path:
    settings = get_settings()
    max_age_days = max_age_days if max_age_days is not None else settings.acs_dim_max_age_days
    dim_path = settings.population_dim_path
    if not force and _dim_is_fresh(dim_path, max_age_days):
        return dim_path

    dataset_id = settings.acs_most_recent_dataset_id
    sample = _fetch_sample(dataset_id)
    community_field = _infer_field(
        sample.keys(),
        COMMUNITY_AREA_CANDIDATES,
        settings.acs_community_area_field,
        "ACS_COMMUNITY_AREA_FIELD",
        "community area",
    )
    population_field = _infer_field(
        sample.keys(),
        POPULATION_CANDIDATES,
        settings.acs_population_field,
        "ACS_POPULATION_FIELD",
        "population",
    )

    rows = _fetch_all_rows(dataset_id)
    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError(f"No data returned for population dim ({dataset_id})")
    if community_field not in df.columns:
        raise ValueError(
            f"Population dim missing community area field {community_field!r}. "
            f"Keys: {sorted(df.columns)}"
        )
    if population_field not in df.columns:
        raise ValueError(
            f"Population dim missing population field {population_field!r}. "
            f"Keys: {sorted(df.columns)}"
        )

    df["community_area"] = _normalize_community_area(df[community_field])
    df["population"] = _coerce_numeric(df[population_field])
    df = df[df["community_area"].notna()]
    df["community_area"] = df["community_area"].astype(int)
    df = df[df["population"].notna()]
    df["population"] = df["population"].astype(int)

    output = df[["community_area", "population"]].drop_duplicates(subset=["community_area"])
    output = output.sort_values("community_area")
    output["source_dataset_id"] = dataset_id
    output["updated_at"] = datetime.now(timezone.utc)
    if output.empty:
        raise ValueError(
            f"Population dim produced no rows after parsing. "
            f"Check fields {community_field!r}, {population_field!r} and community area mapping."
        )

    dim_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dim_path.with_suffix(".parquet.tmp")
    output.to_parquet(tmp_path, index=False)
    tmp_path.replace(dim_path)
    logger.info("Wrote population dim with %s rows", len(output))
    return dim_path


def _infer_acs_year_field(sample: dict, settings) -> str | None:
    if not settings.use_acs_multiyear:
        return None
    return _infer_field(
        sample.keys(),
        YEAR_CANDIDATES,
        settings.acs_year_field,
        "ACS_YEAR_FIELD",
        "ACS year",
    )


def ensure_acs_demographics_dim(force: bool = False, max_age_days: int | None = None) -> Path:
    settings = get_settings()
    max_age_days = max_age_days if max_age_days is not None else settings.acs_dim_max_age_days
    dim_path = settings.acs_dim_path
    if not force and _dim_is_fresh(dim_path, max_age_days):
        return dim_path

    dataset_id = (
        settings.acs_multiyear_dataset_id if settings.use_acs_multiyear else settings.acs_most_recent_dataset_id
    )
    sample = _fetch_sample(dataset_id)
    community_field = _infer_field(
        sample.keys(),
        COMMUNITY_AREA_CANDIDATES,
        settings.acs_community_area_field,
        "ACS_COMMUNITY_AREA_FIELD",
        "community area",
    )
    year_field = _infer_acs_year_field(sample, settings)

    rows = _fetch_all_rows(dataset_id)
    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError(f"No data returned for ACS demographics dim ({dataset_id})")
    if community_field not in df.columns:
        raise ValueError(
            f"ACS dim missing community area field {community_field!r}. "
            f"Keys: {sorted(df.columns)}"
        )

    df["community_area"] = _normalize_community_area(df[community_field])
    df = df[df["community_area"].notna()]
    df["community_area"] = df["community_area"].astype(int)

    acs_year: pd.Series | None = None
    if year_field:
        if year_field not in df.columns:
            raise ValueError(
                f"ACS dim missing year field {year_field!r}. Keys: {sorted(df.columns)}"
            )
        acs_year = _coerce_numeric(df[year_field])
        df["acs_year"] = acs_year
        if settings.use_acs_multiyear and acs_year.notna().any():
            max_year = int(acs_year.dropna().max())
            df = df[df["acs_year"] == max_year]

    skip_cols = {community_field, "community_area", "community_area_name", "acs_year"}
    if year_field:
        skip_cols.add(year_field)

    numeric_cols: list[str] = []
    for col in df.columns:
        if col in skip_cols:
            continue
        converted = _coerce_numeric(df[col])
        if converted.notna().any():
            df[col] = converted
            numeric_cols.append(col)

    keep_cols = ["community_area"]
    if "acs_year" in df.columns:
        keep_cols.append("acs_year")
    keep_cols.extend(sorted(set(numeric_cols)))
    keep_cols = list(dict.fromkeys(keep_cols))

    output = df[keep_cols].dropna(axis=1, how="all")
    output = output.drop_duplicates(subset=["community_area"]).sort_values("community_area")
    output["source_dataset_id"] = dataset_id
    output["updated_at"] = datetime.now(timezone.utc)
    if output.empty:
        raise ValueError(
            f"ACS demographics dim produced no rows after parsing. "
            f"Check field {community_field!r} and community area mapping."
        )

    dim_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dim_path.with_suffix(".parquet.tmp")
    output.to_parquet(tmp_path, index=False)
    tmp_path.replace(dim_path)
    logger.info("Wrote ACS demographics dim with %s rows", len(output))
    return dim_path


def ensure_all_acs_dims(force: bool = False) -> None:
    ensure_population_dim(force=force)
    ensure_acs_demographics_dim(force=force)


def main() -> None:
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Ingest ACS population/demographics dims")
    parser.add_argument("--force", action="store_true", help="Force refresh of cached dims")
    parser.add_argument("--max-age-days", type=int, default=None, help="Override max age for cached dims")
    args = parser.parse_args()

    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    force = args.force or os.getenv("FORCE", "0") == "1"
    ensure_population_dim(force=force, max_age_days=args.max_age_days)
    ensure_acs_demographics_dim(force=force, max_age_days=args.max_age_days)


if __name__ == "__main__":
    main()
