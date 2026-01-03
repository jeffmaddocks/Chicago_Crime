from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from chicago_crime.config import get_settings

MAPBOX_STYLE = "open-street-map"


def load_community_areas_geojson(data_dir: Path | None = None) -> dict[str, Any]:
    settings = get_settings()
    base_dir = data_dir or settings.data_dir
    geojson_path = base_dir / "dim" / "community_areas" / "community_areas.geojson"
    if not geojson_path.exists():
        return {}
    with geojson_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def ensure_feature_id_key(geojson: dict[str, Any], number_field: str) -> dict[str, Any]:
    if not geojson or geojson.get("type") != "FeatureCollection":
        return geojson
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        number = props.get(number_field)
        try:
            feature["id"] = int(number)
        except (TypeError, ValueError):
            continue
    return geojson


def community_area_name_map(data_dir: Path | None = None) -> dict[int, str]:
    settings = get_settings()
    base_dir = data_dir or settings.data_dir
    dim_path = base_dir / "dim" / "community_areas" / "community_areas.parquet"
    if not dim_path.exists():
        return {}
    import pandas as pd

    df = pd.read_parquet(dim_path)
    if df.empty:
        return {}
    return {int(row["community_area"]): str(row["community_area_name"]) for _, row in df.iterrows()}
