from __future__ import annotations

from datetime import datetime, timezone

from chicago_crime import config
from chicago_crime.ingest.state import IngestState, load_state, save_state


def _reset_settings(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    config._SETTINGS = None


def test_state_defaults(tmp_path, monkeypatch) -> None:
    _reset_settings(monkeypatch, tmp_path)
    state = load_state()
    settings = config.get_settings()
    assert state.dataset_id == settings.dataset_id
    assert state.watermark_max_date is None
    assert state.last_run_at is None
    assert state.backfill_days == settings.backfill_days
    assert state.rows_last_run == 0


def test_state_roundtrip(tmp_path, monkeypatch) -> None:
    _reset_settings(monkeypatch, tmp_path)
    watermark = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    last_run = datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc)
    state = IngestState(
        dataset_id="ijzp-q8t2",
        watermark_max_date=watermark,
        last_run_at=last_run,
        backfill_days=7,
        rows_last_run=123,
    )
    save_state(state)
    loaded = load_state()
    assert loaded.dataset_id == "ijzp-q8t2"
    assert loaded.watermark_max_date == watermark
    assert loaded.last_run_at == last_run
    assert loaded.backfill_days == 7
    assert loaded.rows_last_run == 123
