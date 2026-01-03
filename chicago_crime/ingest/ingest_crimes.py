from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from chicago_crime.config import get_settings
from chicago_crime.ingest.lake_inspector import get_max_date_from_lake
from chicago_crime.ingest.parquet_writer import add_partition_columns, merge_partitions, write_staged_parquet
from chicago_crime.ingest.soda_client import SodaClient
from chicago_crime.ingest.schema import NORMALIZED_COLUMNS
from chicago_crime.ingest.state import IngestState, load_state, save_state
from chicago_crime.logging_config import setup_logging

logger = logging.getLogger(__name__)


def _normalize_records(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={"primary_type": "primary_type"})
    for col in NORMALIZED_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[NORMALIZED_COLUMNS]
    df = df[df["id"].notna()]
    df["id"] = df["id"].astype(str)
    df["date"] = pd.to_datetime(df.get("date"), utc=True, errors="coerce")
    df = df[df["date"].notna()]
    for col in ["arrest", "domestic"]:
        if col in df:
            df[col] = df[col].astype(str).str.lower().map({"true": True, "false": False})
    numeric_cols = ["beat", "district", "ward", "community_area", "latitude", "longitude"]
    for col in numeric_cols:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _get_query_start(settings, lake_glob: str) -> datetime:
    max_date = get_max_date_from_lake(lake_glob)
    backfill = timedelta(days=settings.backfill_days)
    if max_date is None:
        return settings.start_date
    query_start = max_date - backfill
    if settings.start_date:
        query_start = max(query_start, settings.start_date)
    return query_start


def ingest_once() -> IngestState:
    settings = get_settings()
    settings.lake_dir.mkdir(parents=True, exist_ok=True)
    settings.staging_dir.mkdir(parents=True, exist_ok=True)

    lake_glob = str(settings.lake_dir / "**" / "*.parquet")
    query_start = _get_query_start(settings, lake_glob)

    client = SodaClient()
    records: list[dict] = []
    for row in client.fetch_since(query_start):
        records.append(row)

    df = _normalize_records(records)
    if df.empty:
        state = load_state()
        state.last_run_at = datetime.now(timezone.utc)
        state.rows_last_run = 0
        save_state(state)
        logger.info("No new rows to ingest")
        return state

    df = add_partition_columns(df)
    staged_path = write_staged_parquet(df, settings.staging_dir)
    rows_written, max_date = merge_partitions(settings.lake_dir, staged_path)

    try:
        staged_path.unlink()
    except OSError:
        logger.warning("Failed to remove staged file %s", staged_path)

    lake_max = get_max_date_from_lake(lake_glob) or max_date
    state = load_state()
    state.dataset_id = settings.dataset_id
    state.backfill_days = settings.backfill_days
    state.watermark_max_date = lake_max
    state.last_run_at = datetime.now(timezone.utc)
    state.rows_last_run = rows_written
    save_state(state)
    logger.info("Ingest complete: %s rows written", rows_written)
    return state


def _loop_ingest(interval_hours: int) -> None:
    while True:
        ingest_once()
        time.sleep(interval_hours * 3600)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Chicago crime data")
    parser.add_argument("--once", action="store_true", help="Run one ingest and exit")
    parser.add_argument("--loop", action="store_true", help="Run ingest loop")
    args = parser.parse_args()

    settings = get_settings()
    setup_logging(settings.log_level)

    loop_env = os.getenv("CHI_INGEST_LOOP", "0") == "1"
    interval_hours = int(os.getenv("CHI_INGEST_INTERVAL_HOURS", "24"))

    if args.loop or loop_env:
        _loop_ingest(interval_hours)
    else:
        ingest_once()


if __name__ == "__main__":
    main()
