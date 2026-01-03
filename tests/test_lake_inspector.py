from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from chicago_crime.ingest.lake_inspector import get_max_date_from_lake


def test_get_max_date_from_lake(tmp_path: Path) -> None:
    lake_dir = tmp_path / "lake" / "crimes" / "year=2024" / "month=01" / "day=02"
    lake_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "id": ["1", "2"],
            "date": [
                datetime(2024, 1, 2, 10, tzinfo=timezone.utc),
                datetime(2024, 1, 2, 12, tzinfo=timezone.utc),
            ],
        }
    )
    df.to_parquet(lake_dir / "part-000.parquet", index=False)

    max_date = get_max_date_from_lake(str(tmp_path / "lake" / "crimes" / "**" / "*.parquet"))
    assert max_date == datetime(2024, 1, 2, 12, tzinfo=timezone.utc)
