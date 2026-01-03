from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, List

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from chicago_crime.config import get_settings
from chicago_crime.ingest.schema import API_FIELDS

logger = logging.getLogger(__name__)


class SodaError(RuntimeError):
    pass


class SodaClient:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.base_url = "https://data.cityofchicago.org/resource"

    @retry(
        retry=retry_if_exception_type(SodaError),
        wait=wait_exponential(multiplier=1, min=1, max=32),
        stop=stop_after_attempt(5),
    )
    def _get(self, params: dict, dataset_id: str | None = None) -> List[dict]:
        headers = {}
        if self.settings.soda_app_token:
            headers["X-App-Token"] = self.settings.soda_app_token
        dataset = dataset_id or self.settings.dataset_id
        url = f"{self.base_url}/{dataset}.json"
        response = requests.get(url, params=params, headers=headers, timeout=30)
        if response.status_code in (429, 500, 502, 503, 504):
            raise SodaError(f"Transient error {response.status_code}")
        if not response.ok:
            raise SodaError(f"Socrata error {response.status_code}: {response.text}")
        return response.json()

    def fetch_since(self, start_date: datetime) -> Iterable[dict]:
        offset = 0
        limit = self.settings.page_limit
        fields = ",".join(API_FIELDS)
        where_clause = f"date >= '{_format_soda_datetime(start_date)}'"

        while True:
            params = {
                "$select": fields,
                "$limit": limit,
                "$offset": offset,
                "$where": where_clause,
                "$order": "date asc",
            }
            batch = self._get(params)
            if not batch:
                break
            logger.info("Fetched %s rows at offset %s", len(batch), offset)
            for row in batch:
                yield row
            offset += limit

    def fetch_rows(
        self,
        dataset_id: str,
        select: str | None = None,
        where: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        order: str | None = None,
    ) -> List[dict]:
        params = {
            "$limit": limit or self.settings.page_limit,
            "$offset": offset,
        }
        if select:
            params["$select"] = select
        if where:
            params["$where"] = where
        if order:
            params["$order"] = order
        return self._get(params, dataset_id=dataset_id)


def _format_soda_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%dT%H:%M:%S")
