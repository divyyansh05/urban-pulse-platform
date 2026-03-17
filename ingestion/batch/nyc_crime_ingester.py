"""ingestion/batch/nyc_crime_ingester.py — NYPD Crime daily ingester."""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

from ingestion.batch.base_ingester import MAX_PAGES, PAGE_SIZE, BaseIngester
from ingestion.schemas.nyc_crime import NYCCrimeRaw

logger = logging.getLogger(__name__)
CRIME_ENDPOINT = "https://data.cityofnewyork.us/resource/qgea-i56i.json"


class NYCCrimeIngester(BaseIngester):
    source_name = "nyc/crime"
    table_name = "nyc_crime_raw"

    def __init__(self, lookback_days: int = 2, max_records: int | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.lookback_days = lookback_days
        self.max_records = max_records
        self.app_token = os.environ.get("NYC_APP_TOKEN", "")

    def fetch_records(self) -> Iterator[dict]:
        since = datetime.now(UTC) - timedelta(days=self.lookback_days)
        where = f"rpt_dt >= '{since.strftime('%m/%d/%Y')}'"
        offset, pages, total = 0, 0, 0
        logger.info("Fetching NYC Crime: %s", where)
        while pages < MAX_PAGES:
            resp = self.session.get(
                CRIME_ENDPOINT,
                params={
                    "$$app_token": self.app_token,
                    "$limit": PAGE_SIZE,
                    "$offset": offset,
                    "$order": "cmplnt_fr_dt DESC",
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
        logger.info("NYC Crime total fetched: %d", total)

    def parse_record(self, raw: dict) -> NYCCrimeRaw:
        return NYCCrimeRaw.from_api_record(raw)
