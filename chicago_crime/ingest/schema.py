from __future__ import annotations

from typing import Dict, List

API_FIELDS: List[str] = [
    "id",
    "date",
    "primary_type",
    "description",
    "location_description",
    "arrest",
    "domestic",
    "beat",
    "district",
    "ward",
    "community_area",
    "latitude",
    "longitude",
    "iucr",
    "fbi_code",
]

SNAKE_CASE_MAP: Dict[str, str] = {
    "primary_type": "primary_type",
    "location_description": "location_description",
    "community_area": "community_area",
    "fbi_code": "fbi_code",
}

NORMALIZED_COLUMNS: List[str] = [
    "id",
    "date",
    "primary_type",
    "description",
    "location_description",
    "arrest",
    "domestic",
    "beat",
    "district",
    "ward",
    "community_area",
    "latitude",
    "longitude",
    "iucr",
    "fbi_code",
]
