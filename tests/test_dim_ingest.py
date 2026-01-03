from __future__ import annotations

import json
from pathlib import Path

from chicago_crime.ingest.ingest_dimensions import extract_dim_from_geojson


def test_extract_dim_from_geojson(tmp_path: Path) -> None:
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"area_num_1": "1", "community": "Rogers Park"},
                "geometry": {"type": "Polygon", "coordinates": []},
            },
            {
                "type": "Feature",
                "properties": {"area_num_1": "2", "community": "West Ridge"},
                "geometry": {"type": "Polygon", "coordinates": []},
            },
        ],
    }
    path = tmp_path / "community.geojson"
    path.write_text(json.dumps(payload), encoding="utf-8")

    df = extract_dim_from_geojson(path)
    assert list(df.columns) == ["community_area", "community_area_name"]
    assert df["community_area"].tolist() == [1, 2]
    assert df["community_area_name"].tolist() == ["Rogers Park", "West Ridge"]
