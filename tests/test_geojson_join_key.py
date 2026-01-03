from __future__ import annotations

from chicago_crime.analytics.geo import ensure_feature_id_key


def test_ensure_feature_id_key_sets_id() -> None:
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "properties": {"area_num_1": "3"}, "geometry": None},
            {"type": "Feature", "properties": {"area_num_1": 7}, "geometry": None},
        ],
    }
    updated = ensure_feature_id_key(geojson, "area_num_1")
    ids = [feature.get("id") for feature in updated["features"]]
    assert ids == [3, 7]
