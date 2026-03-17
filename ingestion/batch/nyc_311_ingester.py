"""ingestion/batch/nyc_311_ingester.py — NYC 311 incremental ingester."""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

from ingestion.batch.base_ingester import BaseIngester
from ingestion.schemas.nyc_311 import NYC311Raw

logger = logging.getLogger(__name__)
NYC_311_ENDPOINT = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE = 10_000
MAX_PAGES = 50


class NYC311Ingester(BaseIngester):
    source_name = "nyc/311"
    table_name = "nyc_311_raw"

    def __init__(self, lookback_hours: int = 25, max_records: int | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.lookback_hours = lookback_hours
        self.max_records = max_records
        self.app_token = os.environ.get("NYC_APP_TOKEN", "")

    def fetch_records(self) -> Iterator[dict]:
        since = datetime.now(UTC) - timedelta(hours=self.lookback_hours)
        where = f"created_date >= '{since.strftime('%Y-%m-%dT%H:%M:%S.000')}'"
        offset, pages, total = 0, 0, 0
        logger.info("Fetching NYC 311: %s", where)
        while pages < MAX_PAGES:
            resp = self.session.get(
                NYC_311_ENDPOINT,
                params={
                    "$$app_token": self.app_token,
                    "$limit": PAGE_SIZE,
                    "$offset": offset,
                    "$order": "created_date DESC",
                    "$where": where,
                },
                timeout=30,
            )
            resp.raise_for_status()
            page = resp.json()
            if not page:
                break
            for record in page:
                yield record
                total += 1
                if self.max_records and total >= self.max_records:
                    return
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
            pages += 1
        logger.info("NYC 311 total fetched: %d", total)

    def parse_record(self, raw: dict) -> NYC311Raw:
        return NYC311Raw.from_api_record(raw)
