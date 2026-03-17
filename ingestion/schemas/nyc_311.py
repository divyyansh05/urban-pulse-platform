"""ingestion/schemas/nyc_311.py — Pydantic model for NYC 311 API records."""

from __future__ import annotations

import json

from pydantic import BaseModel, Field, model_validator

from ingestion.schemas.base import new_uuid, safe_str, today_utc, utc_now

KNOWN_BOROUGHS = {"MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"}


class NYC311Raw(BaseModel):
    ingestion_id: str = Field(default_factory=new_uuid)
    ingestion_timestamp: str = Field(default_factory=lambda: utc_now().isoformat())
    ingestion_date: str = Field(default_factory=lambda: str(today_utc()))
    source_file: str | None = None
    raw_json: str = ""

    unique_key: str | None = None
    created_date: str | None = None
    closed_date: str | None = None
    agency: str | None = None
    agency_name: str | None = None
    complaint_type: str | None = None
    descriptor: str | None = None
    location_type: str | None = None
    incident_zip: str | None = None
    incident_address: str | None = None
    street_name: str | None = None
    borough: str | None = None
    latitude: str | None = None
    longitude: str | None = None
    status: str | None = None
    resolution_description: str | None = None
    community_board: str | None = None
    open_data_channel_type: str | None = None

    has_location: bool = False
    has_borough: bool = False
    is_valid: bool = True
    validation_errors: list[str] = Field(default_factory=list)

    model_config = {"extra": "allow"}

    @model_validator(mode="after")
    def validate_record(self) -> NYC311Raw:
        errors = []
        if not self.unique_key:
            errors.append("missing unique_key")
        if not self.created_date:
            errors.append("missing created_date")
        if not self.complaint_type:
            errors.append("missing complaint_type")
        if self.borough and self.borough.upper() not in KNOWN_BOROUGHS:
            errors.append(f"unknown borough: {self.borough}")
        try:
            lat = float(self.latitude) if self.latitude else None
            lon = float(self.longitude) if self.longitude else None
            self.has_location = lat is not None and lon is not None
        except (ValueError, TypeError):
            self.has_location = False
        self.has_borough = bool(self.borough)
        if errors:
            self.validation_errors = errors
            if "missing unique_key" in errors or "missing created_date" in errors:
                self.is_valid = False
        return self

    @classmethod
    def from_api_record(cls, record: dict, source_file: str | None = None) -> NYC311Raw:
        return cls(
            raw_json=json.dumps(record, default=str),
            source_file=source_file,
            unique_key=safe_str(record.get("unique_key")),
            created_date=safe_str(record.get("created_date")),
            closed_date=safe_str(record.get("closed_date")),
            agency=safe_str(record.get("agency")),
            agency_name=safe_str(record.get("agency_name")),
            complaint_type=safe_str(record.get("complaint_type")),
            descriptor=safe_str(record.get("descriptor")),
            location_type=safe_str(record.get("location_type")),
            incident_zip=safe_str(record.get("incident_zip")),
            incident_address=safe_str(record.get("incident_address")),
            street_name=safe_str(record.get("street_name")),
            borough=safe_str(record.get("borough")),
            latitude=safe_str(record.get("latitude")),
            longitude=safe_str(record.get("longitude")),
            status=safe_str(record.get("status")),
            resolution_description=safe_str(record.get("resolution_description")),
            community_board=safe_str(record.get("community_board")),
            open_data_channel_type=safe_str(record.get("open_data_channel_type")),
        )

    def to_bq_row(self) -> dict:
        return {
            "_ingestion_id": self.ingestion_id,
            "_ingestion_timestamp": self.ingestion_timestamp,
            "_ingestion_date": self.ingestion_date,
            "_source_file": self.source_file,
            "unique_key": self.unique_key,
            "created_date": self.created_date,
            "closed_date": self.closed_date,
            "agency": self.agency,
            "agency_name": self.agency_name,
            "complaint_type": self.complaint_type,
            "descriptor": self.descriptor,
            "location_type": self.location_type,
            "incident_zip": self.incident_zip,
            "incident_address": self.incident_address,
            "street_name": self.street_name,
            "borough": self.borough,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "status": self.status,
            "resolution_description": self.resolution_description,
            "community_board": self.community_board,
            "open_data_channel_type": self.open_data_channel_type,
            "raw_json": self.raw_json,
        }
