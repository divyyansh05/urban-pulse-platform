"""ingestion/schemas/airnow.py — AirNow EPA air quality observations."""

from __future__ import annotations

import json

from pydantic import BaseModel, Field, model_validator

from ingestion.schemas.base import new_uuid, safe_float, safe_int, safe_str, today_utc, utc_now


class AirNowObservationRaw(BaseModel):
    ingestion_id: str = Field(default_factory=new_uuid)
    ingestion_timestamp: str = Field(default_factory=lambda: utc_now().isoformat())
    ingestion_date: str = Field(default_factory=lambda: str(today_utc()))
    source_file: str | None = None
    raw_json: str = ""

    date_observed: str | None = None
    hour_observed: int | None = None
    local_time_zone: str | None = None
    reporting_area: str | None = None
    state_code: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    parameter_name: str | None = None
    aqi: int | None = None
    category_number: int | None = None
    category_name: str | None = None

    is_valid: bool = True
    validation_errors: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_record(self) -> AirNowObservationRaw:
        errors = []
        if not self.parameter_name:
            errors.append("missing parameter_name")
        if not self.date_observed:
            errors.append("missing date_observed")
        if self.hour_observed is not None and not (0 <= self.hour_observed <= 23):
            errors.append(f"invalid hour_observed: {self.hour_observed}")
        if self.aqi is not None and not (0 <= self.aqi <= 500):
            errors.append(f"aqi out of range: {self.aqi}")
        if errors:
            self.validation_errors = errors
            if "missing parameter_name" in errors:
                self.is_valid = False
        return self

    @classmethod
    def from_api_record(cls, record: dict, source_file: str | None = None) -> AirNowObservationRaw:
        category = record.get("Category", {})
        return cls(
            raw_json=json.dumps(record, default=str),
            source_file=source_file,
            date_observed=safe_str(record.get("DateObserved", record.get("date_observed"))),
            hour_observed=safe_int(record.get("HourObserved", record.get("hour_observed"))),
            local_time_zone=safe_str(record.get("LocalTimeZone", record.get("local_time_zone"))),
            reporting_area=safe_str(record.get("ReportingArea", record.get("reporting_area"))),
            state_code=safe_str(record.get("StateCode", record.get("state_code"))),
            latitude=safe_float(record.get("Latitude", record.get("latitude"))),
            longitude=safe_float(record.get("Longitude", record.get("longitude"))),
            parameter_name=safe_str(record.get("ParameterName", record.get("parameter_name"))),
            aqi=safe_int(record.get("AQI", record.get("aqi"))),
            category_number=safe_int(
                category.get("Number")
                if isinstance(category, dict)
                else record.get("category_number")
            ),
            category_name=safe_str(
                category.get("Name") if isinstance(category, dict) else record.get("category_name")
            ),
        )

    def to_bq_row(self) -> dict:
        return {
            "_ingestion_id": self.ingestion_id,
            "_ingestion_timestamp": self.ingestion_timestamp,
            "_ingestion_date": self.ingestion_date,
            "_source_file": self.source_file,
            "date_observed": self.date_observed,
            "hour_observed": self.hour_observed,
            "local_time_zone": self.local_time_zone,
            "reporting_area": self.reporting_area,
            "state_code": self.state_code,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "parameter_name": self.parameter_name,
            "aqi": self.aqi,
            "category_number": self.category_number,
            "category_name": self.category_name,
            "raw_json": self.raw_json,
        }
