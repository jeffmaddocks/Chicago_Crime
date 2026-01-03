from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from chicago_crime.config import get_settings


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _dt_to_str(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


@dataclass
class IngestState:
    dataset_id: str
    watermark_max_date: datetime | None
    last_run_at: datetime | None
    backfill_days: int
    rows_last_run: int

    def to_dict(self) -> dict:
        return {
            "dataset_id": self.dataset_id,
            "watermark_max_date": _dt_to_str(self.watermark_max_date),
            "last_run_at": _dt_to_str(self.last_run_at),
            "backfill_days": self.backfill_days,
            "rows_last_run": self.rows_last_run,
        }


def _default_state() -> IngestState:
    settings = get_settings()
    return IngestState(
        dataset_id=settings.dataset_id,
        watermark_max_date=None,
        last_run_at=None,
        backfill_days=settings.backfill_days,
        rows_last_run=0,
    )


def load_state(path: Path | None = None) -> IngestState:
    settings = get_settings()
    state_path = path or settings.state_path
    if not state_path.exists():
        return _default_state()
    with state_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return IngestState(
        dataset_id=payload.get("dataset_id", settings.dataset_id),
        watermark_max_date=_parse_datetime(payload.get("watermark_max_date")),
        last_run_at=_parse_datetime(payload.get("last_run_at")),
        backfill_days=int(payload.get("backfill_days", settings.backfill_days)),
        rows_last_run=int(payload.get("rows_last_run", 0)),
    )


def save_state(state: IngestState, path: Path | None = None) -> None:
    settings = get_settings()
    state_path = path or settings.state_path
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with state_path.open("w", encoding="utf-8") as handle:
        json.dump(state.to_dict(), handle, indent=2)
