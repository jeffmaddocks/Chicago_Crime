import os
from datetime import datetime, timezone

import pandas as pd

import chicago_crime.config as config
from chicago_crime.analytics.queries import filter_crimes


def _reset_settings(data_dir) -> None:
    os.environ["DATA_DIR"] = str(data_dir)
    config._SETTINGS = None


def test_acs_dim_join(tmp_path) -> None:
    _reset_settings(tmp_path)
    settings = config.get_settings()

    lake_dir = settings.lake_dir
    lake_dir.mkdir(parents=True, exist_ok=True)

    crimes = pd.DataFrame(
        {
            "id": ["1", "2"],
            "date": [
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc),
            ],
            "community_area": ["1", "2"],
        }
    )
    crimes.to_parquet(lake_dir / "part-000.parquet", index=False)

    acs = pd.DataFrame(
        {
            "community_area": [1, 2],
            "median_household_income": [55000, 62000],
            "pct_poverty": [12.5, 9.2],
            "source_dataset_id": ["test", "test"],
            "updated_at": [datetime(2024, 1, 1, tzinfo=timezone.utc)] * 2,
        }
    )
    settings.acs_dim_path.parent.mkdir(parents=True, exist_ok=True)
    acs.to_parquet(settings.acs_dim_path, index=False)

    df = filter_crimes(None, None, None, None, None, None)

    assert "median_household_income" in df.columns
    assert "pct_poverty" in df.columns
    assert df.sort_values("id")["median_household_income"].tolist() == [55000, 62000]
