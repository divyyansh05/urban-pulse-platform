"""ingestion/schemas/weather.py — Open-Meteo hourly weather observations."""

from __future__ import annotations

import json

from pydantic import BaseModel, Field, model_validator

from ingestion.schemas.base import new_uuid, safe_float, safe_int, safe_str, today_utc, utc_now

CITY_COORDS = {
    "nyc": {"latitude": 40.7128, "longitude": -74.0060},
    "london": {"latitude": 51.5074, "longitude": -0.1278},
}


class WeatherObservationRaw(BaseModel):
    ingestion_id: str = Field(default_factory=new_uuid)
    ingestion_timestamp: str = Field(default_factory=lambda: utc_now().isoformat())
    ingestion_date: str = Field(default_factory=lambda: str(today_utc()))
    source_file: str | None = None
    raw_json: str = ""

    observation_time: str | None = None
    city: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    temperature_2m: float | None = None
    relative_humidity_2m: float | None = None
    precipitation: float | None = None
    precipitation_probability: int | None = None
    wind_speed_10m: float | None = None
    weather_code: int | None = None

    is_valid: bool = True
    validation_errors: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_record(self) -> WeatherObservationRaw:
        errors = []
        if not self.observation_time:
            errors.append("missing observation_time")
        if not self.city:
            errors.append("missing city")
        if self.city and self.city not in CITY_COORDS:
            errors.append(f"unknown city: {self.city}")
        if errors:
            self.validation_errors = errors
            self.is_valid = False
        return self

    @classmethod
    def from_unpacked_row(
        cls, row: dict, city: str, source_file: str | None = None
    ) -> WeatherObservationRaw:
        coords = CITY_COORDS.get(city, {})
        return cls(
            raw_json=json.dumps({**row, "city": city}, default=str),
            source_file=source_file,
            observation_time=safe_str(row.get("time")),
            city=city,
            latitude=coords.get("latitude"),
            longitude=coords.get("longitude"),
            temperature_2m=safe_float(row.get("temperature_2m")),
            relative_humidity_2m=safe_float(row.get("relative_humidity_2m")),
            precipitation=safe_float(row.get("precipitation")),
            precipitation_probability=safe_int(row.get("precipitation_probability")),
            wind_speed_10m=safe_float(row.get("wind_speed_10m")),
            weather_code=safe_int(row.get("weather_code")),
        )

    def to_bq_row(self) -> dict:
        return {
            "_ingestion_id": self.ingestion_id,
            "_ingestion_timestamp": self.ingestion_timestamp,
            "_ingestion_date": self.ingestion_date,
            "_source_file": self.source_file,
            "observation_time": self.observation_time,
            "city": self.city,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "temperature_2m": self.temperature_2m,
            "relative_humidity_2m": self.relative_humidity_2m,
            "precipitation": self.precipitation,
            "precipitation_probability": self.precipitation_probability,
            "wind_speed_10m": self.wind_speed_10m,
            "weather_code": self.weather_code,
            "raw_json": self.raw_json,
        }
