from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from chicago_crime.config import get_settings
from chicago_crime.logging_config import setup_logging

logger = logging.getLogger(__name__)


def _dim_dir() -> Path:
    settings = get_settings()
    return settings.data_dir / "dim" / "community_areas"


def _geojson_path() -> Path:
    return _dim_dir() / "community_areas.geojson"


def _dim_parquet_path() -> Path:
    return _dim_dir() / "community_areas.parquet"


def ensure_community_areas_geojson(force: bool = False, max_age_days: int | None = None) -> Path:
    settings = get_settings()
    max_age_days = max_age_days if max_age_days is not None else settings.dim_max_age_days
    geojson_path = _geojson_path()
    geojson_path.parent.mkdir(parents=True, exist_ok=True)

    if geojson_path.exists() and not force:
        mtime = datetime.fromtimestamp(geojson_path.stat().st_mtime, tz=timezone.utc)
        age_days = (datetime.now(timezone.utc) - mtime).days
        if age_days <= max_age_days:
            return geojson_path

    logger.info("Downloading community areas GeoJSON")
    response = requests.get(settings.community_areas_geojson_url, timeout=60)
    response.raise_for_status()

    try:
        payload = response.json()
    except ValueError as exc:
        snippet = response.text[:200].replace("\n", " ")
        raise ValueError(
            f"GeoJSON response was not JSON (status {response.status_code}): {snippet}"
        ) from exc
    if payload.get("type") != "FeatureCollection" or not payload.get("features"):
        raise ValueError("Invalid GeoJSON payload")

    tmp_path = geojson_path.with_suffix(".geojson.tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    tmp_path.replace(geojson_path)
    return geojson_path


def extract_dim_from_geojson(geojson_path: Path) -> pd.DataFrame:
    settings = get_settings()
    with geojson_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    features = payload.get("features", [])
    if not features:
        raise ValueError("GeoJSON has no features")

    rows: list[dict[str, Any]] = []
    number_field = settings.community_area_number_field
    name_field = settings.community_area_name_field
    for feature in features:
        props = feature.get("properties", {})
        number = props.get(number_field)
        name = props.get(name_field)
        if number is None or name is None:
            continue
        try:
            area_num = int(number)
        except (TypeError, ValueError):
            continue
        rows.append({"community_area": area_num, "community_area_name": str(name).strip()})

    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError("No community area rows extracted")
    df = df.drop_duplicates(subset=["community_area"]).sort_values("community_area")
    return df


def ensure_community_areas_dim(force: bool = False, max_age_days: int | None = None) -> Path:
    geojson_path = ensure_community_areas_geojson(force=force, max_age_days=max_age_days)
    df = extract_dim_from_geojson(geojson_path)

    dim_path = _dim_parquet_path()
    dim_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dim_path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(dim_path)
    logger.info("Wrote community areas dim with %s rows", len(df))
    return dim_path


def main() -> None:
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Ingest community area dimensions")
    parser.add_argument("--force", action="store_true", help="Force refresh of cached GeoJSON")
    parser.add_argument("--max-age-days", type=int, default=None, help="Override max age for cached GeoJSON")
    args = parser.parse_args()

    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    force = args.force or os.getenv("FORCE", "0") == "1"
    ensure_community_areas_dim(force=force, max_age_days=args.max_age_days)


if __name__ == "__main__":
    main()
