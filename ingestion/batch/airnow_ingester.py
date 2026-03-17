"""ingestion/batch/airnow_ingester.py — AirNow EPA hourly ingester."""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator

from ingestion.batch.base_ingester import BaseIngester
from ingestion.schemas.airnow import AirNowObservationRaw

logger = logging.getLogger(__name__)
AIRNOW_ENDPOINT = "https://www.airnowapi.org/aq/observation/latLong/current/"
NYC_LOCATIONS = [
    {"lat": 40.7128, "lon": -74.0060, "label": "manhattan"},
    {"lat": 40.6782, "lon": -73.9442, "label": "brooklyn"},
    {"lat": 40.7282, "lon": -73.7949, "label": "queens"},
]


class AirNowIngester(BaseIngester):
    source_name = "nyc/air_quality"
    table_name = "airnow_observations_raw"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.api_key = os.environ.get("AIRNOW_API_KEY", "")

    def fetch_records(self) -> Iterator[dict]:
        total = 0
        for loc in NYC_LOCATIONS:
            try:
                resp = self.session.get(
                    AIRNOW_ENDPOINT,
                    params={
                        "format": "application/json",
                        "latitude": loc["lat"],
                        "longitude": loc["lon"],
                        "distance": 15,
                        "API_KEY": self.api_key,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                for obs in resp.json():
                    obs["_location_label"] = loc["label"]
                    yield obs
                    total += 1
            except Exception as e:
                logger.error("AirNow fetch failed %s: %s", loc["label"], e)
        logger.info("AirNow total: %d", total)

    def parse_record(self, raw: dict) -> AirNowObservationRaw:
        raw.pop("_location_label", None)
        return AirNowObservationRaw.from_api_record(raw)
