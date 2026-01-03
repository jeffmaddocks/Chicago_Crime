from __future__ import annotations

import pandas as pd


def downsample(df: pd.DataFrame, max_points: int) -> pd.DataFrame:
    if df.empty or len(df) <= max_points:
        return df
    return df.sample(n=max_points, random_state=42)
