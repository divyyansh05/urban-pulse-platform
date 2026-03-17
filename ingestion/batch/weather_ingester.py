"""ingestion/batch/weather_ingester.py — Open-Meteo weather ingester."""

from __future__ import annotations

import logging
from collections.abc import Iterator

from ingestion.batch.base_ingester import BaseIngester
from ingestion.schemas.weather import CITY_COORDS, WeatherObservationRaw

logger = logging.getLogger(__name__)
WEATHER_ENDPOINT = "https://api.open-meteo.com/v1/forecast"
WEATHER_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "precipitation_probability",
    "wind_speed_10m",
    "weather_code",
]


class WeatherIngester(BaseIngester):
    source_name = "weather"
    table_name = "weather_observations_raw"

    def __init__(self, cities: list[str] | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.cities = cities or ["nyc", "london"]

    def _fetch_city(self, city: str) -> list[dict]:
        coords = CITY_COORDS[city]
        resp = self.session.get(
            WEATHER_ENDPOINT,
            params={
                "latitude": coords["latitude"],
                "longitude": coords["longitude"],
                "hourly": ",".join(WEATHER_VARIABLES),
                "timezone": "UTC",
                "past_days": 2,
                "forecast_days": 0,
            },
            timeout=30,
        )
        resp.raise_for_status()
        hourly = resp.json().get("hourly", {})
        times = hourly.get("time", [])
        rows = []
        for i, t in enumerate(times):
            row = {"time": t}
            for var in WEATHER_VARIABLES:
                vals = hourly.get(var, [])
                row[var] = vals[i] if i < len(vals) else None
            rows.append(row)
        logger.info("Unpacked %d rows for city=%s", len(rows), city)
        return rows

    def fetch_records(self) -> Iterator[dict]:
        for city in self.cities:
            try:
                for row in self._fetch_city(city):
                    yield {"_city": city, **row}
            except Exception as e:
                logger.error("Weather fetch failed city=%s: %s", city, e)

    def parse_record(self, raw: dict) -> WeatherObservationRaw:
        city = raw.pop("_city", "nyc")
        return WeatherObservationRaw.from_unpacked_row(raw, city=city)
