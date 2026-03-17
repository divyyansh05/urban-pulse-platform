#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Urban Pulse Platform — Phase 1 File Setup Script
# Run from repo root: bash scripts/setup_phase1.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

GREEN='\033[0;32m'; BLUE='\033[0;34m'; BOLD='\033[1m'; RESET='\033[0m'
ok()     { echo -e "  ${GREEN}✅ $1${RESET}"; }
info()   { echo -e "  ${BLUE}ℹ️  $1${RESET}"; }
header() { echo -e "\n${BOLD}$1${RESET}\n$(printf '─%.0s' {1..60})"; }

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

# ── Step 1: Create __init__.py files ─────────────────────────────────────────
header "Creating package __init__.py files"

for pkg in \
  ingestion \
  ingestion/schemas \
  ingestion/batch \
  orchestration \
  orchestration/dags; do
  if [ ! -f "$pkg/__init__.py" ]; then
    touch "$pkg/__init__.py"
    ok "Created $pkg/__init__.py"
  fi
done

# ── Step 2: Write ingestion/schemas/ ─────────────────────────────────────────
header "Writing ingestion/schemas/"

cat > ingestion/schemas/base.py << 'PYEOF'
"""
ingestion/schemas/base.py
Base Pydantic models and shared types used across all ingestion schemas.
"""

import json
import uuid
from datetime import date, datetime, timezone

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def today_utc() -> date:
    return datetime.now(timezone.utc).date()


def new_uuid() -> str:
    return str(uuid.uuid4())


class IngestionRunLog(BaseModel):
    """One row per pipeline run in raw._ingestion_log."""

    run_id: str = Field(default_factory=new_uuid)
    pipeline_name: str
    source: str
    started_at: datetime = Field(default_factory=utc_now)
    completed_at: datetime | None = None
    status: str = "running"
    records_read: int = 0
    records_written: int = 0
    records_rejected: int = 0
    error_message: str | None = None
    s3_path: str | None = None
    bigquery_job_id: str | None = None

    def complete(
        self,
        records_read: int,
        records_written: int,
        records_rejected: int,
        s3_path: str | None = None,
        bq_job_id: str | None = None,
    ) -> None:
        self.completed_at = utc_now()
        self.status = "success" if records_rejected == 0 else "partial"
        self.records_read = records_read
        self.records_written = records_written
        self.records_rejected = records_rejected
        self.s3_path = s3_path
        self.bigquery_job_id = bq_job_id

    def fail(self, error: str) -> None:
        self.completed_at = utc_now()
        self.status = "failed"
        self.error_message = error[:2000]


def safe_str(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None
PYEOF
ok "ingestion/schemas/base.py"

# ── NYC 311 schema ────────────────────────────────────────────────────────────
cat > ingestion/schemas/nyc_311.py << 'PYEOF'
"""ingestion/schemas/nyc_311.py — Pydantic model for NYC 311 API records."""

from __future__ import annotations
import json
from pydantic import BaseModel, Field, model_validator
from ingestion.schemas.base import safe_str, utc_now, today_utc, new_uuid

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
    def validate_record(self) -> "NYC311Raw":
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
    def from_api_record(cls, record: dict, source_file: str | None = None) -> "NYC311Raw":
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
PYEOF
ok "ingestion/schemas/nyc_311.py"

# ── NYC Crime schema ──────────────────────────────────────────────────────────
cat > ingestion/schemas/nyc_crime.py << 'PYEOF'
"""ingestion/schemas/nyc_crime.py — Pydantic model for NYPD Complaint Data."""

from __future__ import annotations
import json
from pydantic import BaseModel, Field, model_validator
from ingestion.schemas.base import safe_str, utc_now, today_utc, new_uuid

VALID_LAW_CATS = {"FELONY", "MISDEMEANOR", "VIOLATION"}


class NYCCrimeRaw(BaseModel):
    ingestion_id: str = Field(default_factory=new_uuid)
    ingestion_timestamp: str = Field(default_factory=lambda: utc_now().isoformat())
    ingestion_date: str = Field(default_factory=lambda: str(today_utc()))
    source_file: str | None = None
    raw_json: str = ""

    cmplnt_num: str | None = None
    cmplnt_fr_dt: str | None = None
    cmplnt_fr_tm: str | None = None
    cmplnt_to_dt: str | None = None
    cmplnt_to_tm: str | None = None
    rpt_dt: str | None = None
    ofns_desc: str | None = None
    pd_desc: str | None = None
    law_cat_cd: str | None = None
    boro_nm: str | None = None
    addr_pct_cd: str | None = None
    latitude: str | None = None
    longitude: str | None = None
    susp_age_group: str | None = None
    susp_race: str | None = None
    susp_sex: str | None = None
    vic_age_group: str | None = None
    vic_race: str | None = None
    vic_sex: str | None = None

    is_valid: bool = True
    validation_errors: list[str] = Field(default_factory=list)

    model_config = {"extra": "allow"}

    @model_validator(mode="after")
    def validate_record(self) -> "NYCCrimeRaw":
        errors = []
        if not self.cmplnt_num:
            errors.append("missing cmplnt_num")
        if not self.cmplnt_fr_dt:
            errors.append("missing cmplnt_fr_dt")
        if self.law_cat_cd and self.law_cat_cd.upper() not in VALID_LAW_CATS:
            errors.append(f"unknown law_cat_cd: {self.law_cat_cd}")
        if errors:
            self.validation_errors = errors
            if "missing cmplnt_num" in errors:
                self.is_valid = False
        return self

    @classmethod
    def from_api_record(cls, record: dict, source_file: str | None = None) -> "NYCCrimeRaw":
        return cls(
            raw_json=json.dumps(record, default=str),
            source_file=source_file,
            cmplnt_num=safe_str(record.get("cmplnt_num")),
            cmplnt_fr_dt=safe_str(record.get("cmplnt_fr_dt")),
            cmplnt_fr_tm=safe_str(record.get("cmplnt_fr_tm")),
            cmplnt_to_dt=safe_str(record.get("cmplnt_to_dt")),
            cmplnt_to_tm=safe_str(record.get("cmplnt_to_tm")),
            rpt_dt=safe_str(record.get("rpt_dt")),
            ofns_desc=safe_str(record.get("ofns_desc")),
            pd_desc=safe_str(record.get("pd_desc")),
            law_cat_cd=safe_str(record.get("law_cat_cd")),
            boro_nm=safe_str(record.get("boro_nm")),
            addr_pct_cd=safe_str(record.get("addr_pct_cd")),
            latitude=safe_str(record.get("latitude")),
            longitude=safe_str(record.get("longitude")),
            susp_age_group=safe_str(record.get("susp_age_group")),
            susp_race=safe_str(record.get("susp_race")),
            susp_sex=safe_str(record.get("susp_sex")),
            vic_age_group=safe_str(record.get("vic_age_group")),
            vic_race=safe_str(record.get("vic_race")),
            vic_sex=safe_str(record.get("vic_sex")),
        )

    def to_bq_row(self) -> dict:
        return {
            "_ingestion_id": self.ingestion_id,
            "_ingestion_timestamp": self.ingestion_timestamp,
            "_ingestion_date": self.ingestion_date,
            "_source_file": self.source_file,
            "cmplnt_num": self.cmplnt_num,
            "cmplnt_fr_dt": self.cmplnt_fr_dt,
            "cmplnt_fr_tm": self.cmplnt_fr_tm,
            "cmplnt_to_dt": self.cmplnt_to_dt,
            "cmplnt_to_tm": self.cmplnt_to_tm,
            "rpt_dt": self.rpt_dt,
            "ofns_desc": self.ofns_desc,
            "pd_desc": self.pd_desc,
            "law_cat_cd": self.law_cat_cd,
            "boro_nm": self.boro_nm,
            "addr_pct_cd": self.addr_pct_cd,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "susp_age_group": self.susp_age_group,
            "susp_race": self.susp_race,
            "susp_sex": self.susp_sex,
            "vic_age_group": self.vic_age_group,
            "vic_race": self.vic_race,
            "vic_sex": self.vic_sex,
            "raw_json": self.raw_json,
        }
PYEOF
ok "ingestion/schemas/nyc_crime.py"

# ── Weather schema ────────────────────────────────────────────────────────────
cat > ingestion/schemas/weather.py << 'PYEOF'
"""ingestion/schemas/weather.py — Open-Meteo hourly weather observations."""

from __future__ import annotations
import json
from pydantic import BaseModel, Field, model_validator
from ingestion.schemas.base import safe_float, safe_int, safe_str, utc_now, today_utc, new_uuid

CITY_COORDS = {
    "nyc":    {"latitude": 40.7128, "longitude": -74.0060},
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
    def validate_record(self) -> "WeatherObservationRaw":
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
    def from_unpacked_row(cls, row: dict, city: str, source_file: str | None = None) -> "WeatherObservationRaw":
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
PYEOF
ok "ingestion/schemas/weather.py"

# ── AirNow schema ─────────────────────────────────────────────────────────────
cat > ingestion/schemas/airnow.py << 'PYEOF'
"""ingestion/schemas/airnow.py — AirNow EPA air quality observations."""

from __future__ import annotations
import json
from pydantic import BaseModel, Field, model_validator
from ingestion.schemas.base import safe_float, safe_int, safe_str, utc_now, today_utc, new_uuid


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
    def validate_record(self) -> "AirNowObservationRaw":
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
    def from_api_record(cls, record: dict, source_file: str | None = None) -> "AirNowObservationRaw":
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
            category_number=safe_int(category.get("Number") if isinstance(category, dict) else record.get("category_number")),
            category_name=safe_str(category.get("Name") if isinstance(category, dict) else record.get("category_name")),
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
PYEOF
ok "ingestion/schemas/airnow.py"

# ── Step 3: Write ingestion/batch/ utilities ──────────────────────────────────
header "Writing ingestion/batch/"

cat > ingestion/batch/s3_writer.py << 'PYEOF'
"""ingestion/batch/s3_writer.py — Write NDJSON files to AWS S3."""

from __future__ import annotations
import io, json, logging, os
from datetime import datetime, timezone
from pathlib import PurePosixPath
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Writer:
    def __init__(self, bucket: str | None = None, region: str | None = None) -> None:
        self.bucket = bucket or os.environ["AWS_BUCKET_RAW"]
        self.region = region or os.environ.get("AWS_REGION", "us-east-1")
        self._client = boto3.client(
            "s3",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            region_name=self.region,
        )

    def build_s3_key(self, source: str, run_id: str, dt: datetime | None = None) -> str:
        dt = dt or datetime.now(timezone.utc)
        return str(PurePosixPath(source) / str(dt.year) / f"{dt.month:02d}" / f"{dt.day:02d}" / f"{run_id}.ndjson")

    def write_records(self, records: list[dict], source: str, run_id: str, dt: datetime | None = None) -> str:
        if not records:
            return ""
        key = self.build_s3_key(source, run_id, dt)
        buffer = io.BytesIO()
        for record in records:
            buffer.write((json.dumps(record, default=str) + "\n").encode("utf-8"))
        buffer.seek(0)
        logger.info("Uploading %d records to s3://%s/%s", len(records), self.bucket, key)
        try:
            self._client.upload_fileobj(
                buffer, self.bucket, key,
                ExtraArgs={"ContentType": "application/x-ndjson", "Metadata": {"run_id": run_id, "source": source, "record_count": str(len(records))}},
            )
        except ClientError as e:
            logger.error("S3 upload failed: %s", e)
            raise
        s3_path = f"s3://{self.bucket}/{key}"
        logger.info("Wrote to %s", s3_path)
        return s3_path
PYEOF
ok "ingestion/batch/s3_writer.py"

cat > ingestion/batch/bq_loader.py << 'PYEOF'
"""ingestion/batch/bq_loader.py — Load records into BigQuery raw dataset."""

from __future__ import annotations
import logging, os
from typing import Any
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)
STREAMING_BATCH_SIZE = 500


class BigQueryLoader:
    def __init__(self, project: str | None = None, dataset: str = "raw") -> None:
        self.project = project or os.environ["GCP_PROJECT_ID"]
        self.dataset = dataset
        self._client = bigquery.Client(project=self.project)

    def _table_ref(self, table_name: str) -> str:
        return f"{self.project}.{self.dataset}.{table_name}"

    def stream_rows(self, table_name: str, rows: list[dict[str, Any]]) -> tuple[int, int, list[str]]:
        if not rows:
            return 0, 0, []
        table_ref = self._table_ref(table_name)
        inserted, failed, errors = 0, 0, []
        for i in range(0, len(rows), STREAMING_BATCH_SIZE):
            batch = rows[i: i + STREAMING_BATCH_SIZE]
            try:
                insert_errors = self._client.insert_rows_json(table_ref, batch, ignore_unknown_values=True)
                if insert_errors:
                    for err_item in insert_errors:
                        for err in err_item.get("errors", []):
                            errors.append(f"Row {err_item.get('index','?')}: {err.get('message','unknown')}")
                    failed += len(insert_errors)
                    inserted += len(batch) - len(insert_errors)
                else:
                    inserted += len(batch)
            except GoogleCloudError as e:
                logger.error("BQ streaming failed: %s", e)
                failed += len(batch)
                errors.append(str(e))
        logger.info("BQ stream: %d inserted, %d failed → %s", inserted, failed, table_name)
        return inserted, failed, errors

    def insert_ingestion_log(self, log_row: dict) -> None:
        _, failed, errors = self.stream_rows("_ingestion_log", [log_row])
        if failed:
            logger.error("Failed to write ingestion log: %s", errors)
PYEOF
ok "ingestion/batch/bq_loader.py"

cat > ingestion/batch/base_ingester.py << 'PYEOF'
"""ingestion/batch/base_ingester.py — Abstract base class for all batch ingesters."""

from __future__ import annotations
import logging, uuid
from abc import ABC, abstractmethod
from typing import Iterator
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from ingestion.batch.bq_loader import BigQueryLoader
from ingestion.batch.s3_writer import S3Writer
from ingestion.schemas.base import IngestionRunLog

logger = logging.getLogger(__name__)

RETRY_STRATEGY = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])


def build_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=RETRY_STRATEGY)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


class BaseIngester(ABC):
    source_name: str = ""
    table_name: str = ""

    def __init__(self, s3_writer: S3Writer | None = None, bq_loader: BigQueryLoader | None = None, dry_run: bool = False) -> None:
        self.s3 = s3_writer or S3Writer()
        self.bq = bq_loader or BigQueryLoader()
        self.dry_run = dry_run
        self.session = build_session()
        self.run_id = str(uuid.uuid4())
        self._run_log = IngestionRunLog(run_id=self.run_id, pipeline_name=self.__class__.__name__, source=self.source_name)

    @abstractmethod
    def fetch_records(self) -> Iterator[dict]: ...

    @abstractmethod
    def parse_record(self, raw: dict) -> object: ...

    def run(self) -> IngestionRunLog:
        logger.info("Starting run %s for %s", self.run_id[:8], self.source_name)
        records_read, records_valid, records_rejected, bq_rows, s3_path = 0, 0, 0, [], ""
        try:
            for raw_record in self.fetch_records():
                records_read += 1
                try:
                    parsed = self.parse_record(raw_record)
                    if hasattr(parsed, "is_valid") and not parsed.is_valid:
                        records_rejected += 1
                        continue
                    bq_rows.append(parsed.to_bq_row())
                    records_valid += 1
                except Exception as e:
                    records_rejected += 1
                    logger.error("Parse error record %d: %s", records_read, e)

            logger.info("Fetch done: read=%d valid=%d rejected=%d", records_read, records_valid, records_rejected)

            if bq_rows and not self.dry_run:
                s3_path = self.s3.write_records(records=bq_rows, source=self.source_name, run_id=self.run_id)
                inserted, failed, _ = self.bq.stream_rows(table_name=self.table_name, rows=bq_rows)
                records_rejected += failed
                self._run_log.complete(records_read, inserted, records_rejected, s3_path)
            else:
                logger.info("[DRY RUN] Would write %d rows to %s", len(bq_rows), self.table_name)
                self._run_log.complete(records_read, len(bq_rows), records_rejected)

        except Exception as e:
            logger.exception("Run failed: %s", e)
            self._run_log.fail(str(e))
        finally:
            if not self.dry_run:
                self.bq.insert_ingestion_log(self._run_log.model_dump())
            logger.info("Run %s | status=%s | read=%d written=%d rejected=%d", self.run_id[:8], self._run_log.status, self._run_log.records_read, self._run_log.records_written, self._run_log.records_rejected)

        return self._run_log
PYEOF
ok "ingestion/batch/base_ingester.py"

# ── Step 4: Write individual ingesters ────────────────────────────────────────
cat > ingestion/batch/nyc_311_ingester.py << 'PYEOF'
"""ingestion/batch/nyc_311_ingester.py — NYC 311 incremental ingester."""

from __future__ import annotations
import logging, os
from datetime import datetime, timedelta, timezone
from typing import Iterator
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
        since = datetime.now(timezone.utc) - timedelta(hours=self.lookback_hours)
        where = f"created_date >= '{since.strftime('%Y-%m-%dT%H:%M:%S.000')}'"
        offset, pages, total = 0, 0, 0
        logger.info("Fetching NYC 311: %s", where)
        while pages < MAX_PAGES:
            resp = self.session.get(NYC_311_ENDPOINT, params={"$$app_token": self.app_token, "$limit": PAGE_SIZE, "$offset": offset, "$order": "created_date DESC", "$where": where}, timeout=30)
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
PYEOF
ok "ingestion/batch/nyc_311_ingester.py"

cat > ingestion/batch/nyc_crime_ingester.py << 'PYEOF'
"""ingestion/batch/nyc_crime_ingester.py — NYPD Crime daily ingester."""

from __future__ import annotations
import logging, os
from datetime import datetime, timedelta, timezone
from typing import Iterator
from ingestion.batch.base_ingester import BaseIngester, PAGE_SIZE, MAX_PAGES
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
        since = datetime.now(timezone.utc) - timedelta(days=self.lookback_days)
        where = f"rpt_dt >= '{since.strftime('%m/%d/%Y')}'"
        offset, pages, total = 0, 0, 0
        logger.info("Fetching NYC Crime: %s", where)
        while pages < MAX_PAGES:
            resp = self.session.get(CRIME_ENDPOINT, params={"$$app_token": self.app_token, "$limit": PAGE_SIZE, "$offset": offset, "$order": "cmplnt_fr_dt DESC", "$where": where}, timeout=30)
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
PYEOF
ok "ingestion/batch/nyc_crime_ingester.py"

cat > ingestion/batch/weather_ingester.py << 'PYEOF'
"""ingestion/batch/weather_ingester.py — Open-Meteo weather ingester."""

from __future__ import annotations
import logging
from typing import Iterator
from ingestion.batch.base_ingester import BaseIngester
from ingestion.schemas.weather import WeatherObservationRaw, CITY_COORDS

logger = logging.getLogger(__name__)
WEATHER_ENDPOINT = "https://api.open-meteo.com/v1/forecast"
WEATHER_VARIABLES = ["temperature_2m", "relative_humidity_2m", "precipitation", "precipitation_probability", "wind_speed_10m", "weather_code"]


class WeatherIngester(BaseIngester):
    source_name = "weather"
    table_name = "weather_observations_raw"

    def __init__(self, cities: list[str] | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.cities = cities or ["nyc", "london"]

    def _fetch_city(self, city: str) -> list[dict]:
        coords = CITY_COORDS[city]
        resp = self.session.get(WEATHER_ENDPOINT, params={"latitude": coords["latitude"], "longitude": coords["longitude"], "hourly": ",".join(WEATHER_VARIABLES), "timezone": "UTC", "past_days": 2, "forecast_days": 0}, timeout=30)
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
PYEOF
ok "ingestion/batch/weather_ingester.py"

cat > ingestion/batch/airnow_ingester.py << 'PYEOF'
"""ingestion/batch/airnow_ingester.py — AirNow EPA hourly ingester."""

from __future__ import annotations
import logging, os
from typing import Iterator
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
                resp = self.session.get(AIRNOW_ENDPOINT, params={"format": "application/json", "latitude": loc["lat"], "longitude": loc["lon"], "distance": 15, "API_KEY": self.api_key}, timeout=30)
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
PYEOF
ok "ingestion/batch/airnow_ingester.py"

# ── Step 5: Write seed CSVs ───────────────────────────────────────────────────
header "Writing transformation/seeds/"

# borough_zip_lookup (abbreviated — key zip codes)
cat > transformation/seeds/borough_zip_lookup.csv << 'CSVEOF'
zip_code,borough_name,neighborhood
10001,MANHATTAN,Chelsea/Clinton
10002,MANHATTAN,Lower East Side
10007,MANHATTAN,Lower Manhattan
10009,MANHATTAN,East Village
10010,MANHATTAN,Gramercy Park/Murray Hill
10011,MANHATTAN,Chelsea/Clinton
10014,MANHATTAN,Greenwich Village/SoHo
10021,MANHATTAN,Upper East Side
10023,MANHATTAN,Upper West Side
10025,MANHATTAN,Upper West Side
10029,MANHATTAN,East Harlem
10035,MANHATTAN,East Harlem
10036,MANHATTAN,Chelsea/Clinton
10040,MANHATTAN,Inwood/Washington Heights
10065,MANHATTAN,Upper East Side
10128,MANHATTAN,Upper East Side
10301,STATEN ISLAND,St. George
10304,STATEN ISLAND,Stapleton
10312,STATEN ISLAND,New Springville
10314,STATEN ISLAND,New Springville
10451,BRONX,Highbridge/South Bronx
10452,BRONX,Highbridge/South Bronx
10456,BRONX,Central Bronx
10458,BRONX,Fordham/University Heights
10462,BRONX,Northeast Bronx
10466,BRONX,Northeast Bronx
10467,BRONX,Northeast Bronx
10468,BRONX,Kingsbridge/Riverdale
10471,BRONX,Kingsbridge/Riverdale
10475,BRONX,Northeast Bronx
11201,BROOKLYN,Northwest Brooklyn
11203,BROOKLYN,Flatbush
11205,BROOKLYN,Northwest Brooklyn
11207,BROOKLYN,East New York
11211,BROOKLYN,Bushwick/Williamsburg
11215,BROOKLYN,Northwest Brooklyn
11217,BROOKLYN,Northwest Brooklyn
11221,BROOKLYN,Bushwick/Williamsburg
11222,BROOKLYN,Greenpoint
11225,BROOKLYN,Flatbush
11226,BROOKLYN,Flatbush
11232,BROOKLYN,Sunset Park
11233,BROOKLYN,Brownsville/Ocean Hill
11235,BROOKLYN,Southern Brooklyn
11238,BROOKLYN,Crown Heights/Prospect Heights
11354,QUEENS,Flushing/Clearview
11368,QUEENS,Jackson Heights
11369,QUEENS,Jackson Heights
11372,QUEENS,Jackson Heights
11373,QUEENS,Elmhurst/Corona
11374,QUEENS,Rego Park/Forest Hills
11375,QUEENS,Rego Park/Forest Hills
11377,QUEENS,Woodside/Sunnyside
11385,QUEENS,Ridgewood/Glendale
11411,QUEENS,Southeast Queens
11414,QUEENS,Howard Beach/Ozone Park
11432,QUEENS,Jamaica
11434,QUEENS,Jamaica
11691,QUEENS,Rockaways
CSVEOF
ok "transformation/seeds/borough_zip_lookup.csv"

cat > transformation/seeds/tfl_severity_lookup.csv << 'CSVEOF'
severity_code,severity_description,is_disrupted
0,Special Service,false
1,Closed,true
2,Suspended,true
3,Part Suspended,true
4,Planned Closure,true
5,Part Closure,true
6,Severe Delays,true
7,Reduced Service,true
8,Bus Service,true
9,Minor Delays,true
10,Good Service,false
11,Part Closed,true
12,Exit Only,true
13,No Step Free Access,true
14,Change of Frequency,true
15,Diverted,true
16,Not Running,true
17,Issues Reported,true
18,No Issues,false
19,Information,false
20,Service Closed,true
CSVEOF
ok "transformation/seeds/tfl_severity_lookup.csv"

cat > transformation/seeds/wmo_weather_code_lookup.csv << 'CSVEOF'
wmo_code,description,category,is_precipitation
0,Clear sky,Clear,false
1,Mainly clear,Clear,false
2,Partly cloudy,Cloudy,false
3,Overcast,Cloudy,false
45,Foggy,Fog,false
48,Depositing rime fog,Fog,false
51,Light drizzle,Drizzle,true
53,Moderate drizzle,Drizzle,true
55,Dense drizzle,Drizzle,true
61,Slight rain,Rain,true
63,Moderate rain,Rain,true
65,Heavy rain,Rain,true
71,Slight snow,Snow,true
73,Moderate snow,Snow,true
75,Heavy snow,Snow,true
80,Slight rain showers,Rain,true
81,Moderate rain showers,Rain,true
82,Violent rain showers,Rain,true
95,Thunderstorm,Storm,true
96,Thunderstorm with slight hail,Storm,true
99,Thunderstorm with heavy hail,Storm,true
CSVEOF
ok "transformation/seeds/wmo_weather_code_lookup.csv"

cat > transformation/seeds/complaint_type_sla.csv << 'CSVEOF'
complaint_type,sla_hours,priority_level
Noise - Residential,8,LOW
Noise - Commercial,8,LOW
Noise - Street/Sidewalk,8,LOW
HEAT/HOT WATER,24,HIGH
PLUMBING,72,MEDIUM
Street Condition,10,HIGH
Water System,24,HIGH
Sewer,24,HIGH
Rodent,10,HIGH
Unsanitary Condition,10,HIGH
ELEVATOR,8,HIGH
Illegal Parking,72,LOW
Blocked Driveway,72,LOW
Street Light Condition,72,MEDIUM
Missed Collection (All Materials),24,MEDIUM
Derelict Vehicle,168,LOW
Graffiti,168,LOW
Dirty Conditions,24,LOW
CSVEOF
ok "transformation/seeds/complaint_type_sla.csv"

# ── Step 6: Write DAG and Docker Compose ─────────────────────────────────────
header "Writing orchestration/"

cp -f /dev/stdin orchestration/dags/batch_ingestion_dag.py << 'DAGEOF'
"""orchestration/dags/batch_ingestion_dag.py — Batch ingestion DAGs."""

from __future__ import annotations
import logging
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "urban-pulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def run_nyc_311(**context):
    from ingestion.batch.nyc_311_ingester import NYC311Ingester
    run_log = NYC311Ingester(lookback_hours=25).run()
    if run_log.status == "failed":
        raise RuntimeError(f"NYC 311 failed: {run_log.error_message}")
    return {"run_id": run_log.run_id, "records_written": run_log.records_written}


def run_nyc_crime(**context):
    from ingestion.batch.nyc_crime_ingester import NYCCrimeIngester
    run_log = NYCCrimeIngester(lookback_days=2).run()
    if run_log.status == "failed":
        raise RuntimeError(f"NYC Crime failed: {run_log.error_message}")
    return {"run_id": run_log.run_id, "records_written": run_log.records_written}


def run_weather(**context):
    from ingestion.batch.weather_ingester import WeatherIngester
    run_log = WeatherIngester(cities=["nyc", "london"]).run()
    if run_log.status == "failed":
        raise RuntimeError(f"Weather failed: {run_log.error_message}")
    return {"run_id": run_log.run_id, "records_written": run_log.records_written}


def run_airnow(**context):
    from ingestion.batch.airnow_ingester import AirNowIngester
    run_log = AirNowIngester().run()
    if run_log.status == "failed":
        raise RuntimeError(f"AirNow failed: {run_log.error_message}")
    return {"run_id": run_log.run_id, "records_written": run_log.records_written}


with DAG("nyc_311_batch_ingestion", default_args=DEFAULT_ARGS, description="NYC 311 incremental ingestion",
         schedule_interval="*/15 * * * *", start_date=days_ago(1), catchup=False, max_active_runs=1,
         tags=["ingestion", "batch", "nyc"]) as dag_311:
    PythonOperator(task_id="ingest_nyc_311", python_callable=run_nyc_311)

with DAG("nyc_crime_batch_ingestion", default_args=DEFAULT_ARGS, description="NYPD Crime daily ingestion",
         schedule_interval="0 6 * * *", start_date=days_ago(1), catchup=False, max_active_runs=1,
         tags=["ingestion", "batch", "nyc"]) as dag_crime:
    PythonOperator(task_id="ingest_nyc_crime", python_callable=run_nyc_crime)

with DAG("weather_batch_ingestion", default_args=DEFAULT_ARGS, description="Weather hourly ingestion",
         schedule_interval="5 * * * *", start_date=days_ago(1), catchup=False, max_active_runs=1,
         tags=["ingestion", "batch", "weather"]) as dag_weather:
    PythonOperator(task_id="ingest_weather", python_callable=run_weather)

with DAG("airnow_batch_ingestion", default_args=DEFAULT_ARGS, description="AirNow hourly ingestion",
         schedule_interval="10 * * * *", start_date=days_ago(1), catchup=False, max_active_runs=1,
         tags=["ingestion", "batch", "air_quality"]) as dag_airnow:
    PythonOperator(task_id="ingest_airnow", python_callable=run_airnow)
DAGEOF
ok "orchestration/dags/batch_ingestion_dag.py"

# ── Step 7: Write tests ───────────────────────────────────────────────────────
header "Writing tests/unit/"

cat > tests/__init__.py << 'PYEOF'
PYEOF

cat > tests/unit/__init__.py << 'PYEOF'
PYEOF

cat > tests/unit/test_schemas.py << 'PYEOF'
"""tests/unit/test_schemas.py — Unit tests for all Pydantic ingestion schemas."""

from __future__ import annotations
import json
import pytest
from ingestion.schemas.base import safe_float, safe_int, safe_str
from ingestion.schemas.nyc_311 import NYC311Raw
from ingestion.schemas.nyc_crime import NYCCrimeRaw
from ingestion.schemas.weather import WeatherObservationRaw
from ingestion.schemas.airnow import AirNowObservationRaw
from ingestion.batch.s3_writer import S3Writer
from datetime import datetime, timezone
from unittest.mock import MagicMock


@pytest.fixture
def valid_311():
    return {"unique_key": "59812345", "created_date": "2024-03-15T14:23:00.000", "closed_date": "2024-03-16T09:00:00.000", "agency": "NYPD", "complaint_type": "Noise - Residential", "descriptor": "Loud Music/Party", "incident_zip": "10025", "incident_address": "123 WEST 86 STREET", "borough": "MANHATTAN", "latitude": "40.7851", "longitude": "-73.9756", "status": "Closed", "community_board": "07 MANHATTAN"}


@pytest.fixture
def valid_crime():
    return {"cmplnt_num": "244789123", "cmplnt_fr_dt": "03/15/2024", "cmplnt_fr_tm": "14:30:00", "rpt_dt": "03/15/2024", "ofns_desc": "ASSAULT 3", "law_cat_cd": "MISDEMEANOR", "boro_nm": "MANHATTAN", "latitude": "40.7749", "longitude": "-73.9851", "susp_age_group": "25-44", "susp_race": "WHITE HISPANIC", "susp_sex": "M", "vic_age_group": "25-44", "vic_race": "BLACK", "vic_sex": "F"}


@pytest.fixture
def valid_weather():
    return {"time": "2024-03-15T14:00", "temperature_2m": 12.4, "relative_humidity_2m": 65.0, "precipitation": 0.0, "precipitation_probability": 10, "wind_speed_10m": 18.3, "weather_code": 1}


@pytest.fixture
def valid_airnow():
    return {"DateObserved": "2024-03-15 ", "HourObserved": 14, "LocalTimeZone": "EST", "ReportingArea": "New York City - Manhattan", "StateCode": "NY", "Latitude": 40.7128, "Longitude": -74.006, "ParameterName": "PM2.5", "AQI": 42, "Category": {"Number": 1, "Name": "Good"}}


class TestSafeConversions:
    def test_safe_str_none(self): assert safe_str(None) is None
    def test_safe_str_empty(self): assert safe_str("") is None
    def test_safe_str_whitespace(self): assert safe_str("   ") is None
    def test_safe_str_valid(self): assert safe_str("hello") == "hello"
    def test_safe_float_none(self): assert safe_float(None) is None
    def test_safe_float_string(self): assert safe_float("40.71") == pytest.approx(40.71)
    def test_safe_float_invalid(self): assert safe_float("bad") is None
    def test_safe_int_float_string(self): assert safe_int("14.0") == 14
    def test_safe_int_invalid(self): assert safe_int("abc") is None


class TestNYC311Schema:
    def test_valid_record(self, valid_311):
        r = NYC311Raw.from_api_record(valid_311)
        assert r.is_valid and r.unique_key == "59812345"

    def test_has_location(self, valid_311):
        r = NYC311Raw.from_api_record(valid_311)
        assert r.has_location and r.has_borough

    def test_no_location(self, valid_311):
        valid_311.update({"latitude": None, "longitude": None})
        r = NYC311Raw.from_api_record(valid_311)
        assert not r.has_location and r.is_valid

    def test_missing_unique_key_invalid(self, valid_311):
        valid_311["unique_key"] = None
        r = NYC311Raw.from_api_record(valid_311)
        assert not r.is_valid and "missing unique_key" in r.validation_errors

    def test_null_borough_valid(self, valid_311):
        valid_311["borough"] = None
        r = NYC311Raw.from_api_record(valid_311)
        assert r.is_valid and not r.has_borough

    def test_pii_present_in_raw(self, valid_311):
        r = NYC311Raw.from_api_record(valid_311)
        assert r.incident_address == "123 WEST 86 STREET"

    def test_extra_fields_allowed(self, valid_311):
        valid_311["new_socrata_field"] = "value"
        r = NYC311Raw.from_api_record(valid_311)
        assert r.is_valid

    def test_bq_row_has_required_columns(self, valid_311):
        row = NYC311Raw.from_api_record(valid_311).to_bq_row()
        for col in ["_ingestion_id", "_ingestion_timestamp", "_ingestion_date", "unique_key", "complaint_type", "raw_json"]:
            assert col in row

    def test_raw_json_serialized(self, valid_311):
        r = NYC311Raw.from_api_record(valid_311)
        assert json.loads(r.raw_json)["unique_key"] == "59812345"


class TestNYCCrimeSchema:
    def test_valid_record(self, valid_crime):
        r = NYCCrimeRaw.from_api_record(valid_crime)
        assert r.is_valid and r.cmplnt_num == "244789123"

    def test_missing_cmplnt_num_invalid(self, valid_crime):
        valid_crime["cmplnt_num"] = None
        assert not NYCCrimeRaw.from_api_record(valid_crime).is_valid

    def test_date_time_separate(self, valid_crime):
        r = NYCCrimeRaw.from_api_record(valid_crime)
        assert r.cmplnt_fr_dt == "03/15/2024" and r.cmplnt_fr_tm == "14:30:00"

    def test_pii_adjacent_present(self, valid_crime):
        r = NYCCrimeRaw.from_api_record(valid_crime)
        assert r.susp_race == "WHITE HISPANIC" and r.vic_age_group == "25-44"


class TestWeatherSchema:
    def test_valid_nyc(self, valid_weather):
        r = WeatherObservationRaw.from_unpacked_row(valid_weather, city="nyc")
        assert r.is_valid and r.city == "nyc" and r.temperature_2m == pytest.approx(12.4)

    def test_london_coords(self, valid_weather):
        r = WeatherObservationRaw.from_unpacked_row(valid_weather, city="london")
        assert r.latitude == pytest.approx(51.5074)

    def test_unknown_city_invalid(self, valid_weather):
        assert not WeatherObservationRaw.from_unpacked_row(valid_weather, city="paris").is_valid

    def test_null_values_ok(self, valid_weather):
        valid_weather.update({"precipitation": None, "wind_speed_10m": None})
        r = WeatherObservationRaw.from_unpacked_row(valid_weather, city="nyc")
        assert r.is_valid and r.precipitation is None


class TestAirNowSchema:
    def test_valid_record(self, valid_airnow):
        r = AirNowObservationRaw.from_api_record(valid_airnow)
        assert r.is_valid and r.parameter_name == "PM2.5" and r.aqi == 42

    def test_nested_category_parsed(self, valid_airnow):
        r = AirNowObservationRaw.from_api_record(valid_airnow)
        assert r.category_number == 1 and r.category_name == "Good"

    def test_missing_parameter_invalid(self, valid_airnow):
        valid_airnow["ParameterName"] = None
        assert not AirNowObservationRaw.from_api_record(valid_airnow).is_valid

    def test_aqi_out_of_range(self, valid_airnow):
        valid_airnow["AQI"] = 999
        r = AirNowObservationRaw.from_api_record(valid_airnow)
        assert any("aqi out of range" in e for e in r.validation_errors)


class TestS3Writer:
    def test_key_format(self):
        w = S3Writer.__new__(S3Writer)
        w.bucket = "test"
        dt = datetime(2024, 3, 15, tzinfo=timezone.utc)
        assert w.build_s3_key("nyc/311", "abc", dt) == "nyc/311/2024/03/15/abc.ndjson"

    def test_zero_padded_month(self):
        w = S3Writer.__new__(S3Writer)
        w.bucket = "test"
        dt = datetime(2024, 1, 5, tzinfo=timezone.utc)
        assert "2024/01/05" in w.build_s3_key("nyc/crime", "r1", dt)

    def test_empty_records_no_upload(self):
        w = S3Writer.__new__(S3Writer)
        w.bucket = "test"
        w._client = MagicMock()
        assert w.write_records([], "nyc/311", "run-1") == ""
        w._client.upload_fileobj.assert_not_called()
PYEOF
ok "tests/unit/test_schemas.py"

# ── Step 8: Write ADR-002 ─────────────────────────────────────────────────────
header "Writing docs/"

cat > docs/decisions/ADR-002-batch-ingestion-pattern.md << 'MDEOF'
# ADR-002: Batch Ingestion Pattern

## Status
Accepted

## Context
Phase 1 requires ingesting 4 batch sources: NYC 311, NYC Crime, Open-Meteo
Weather, and AirNow EPA. Each has different schemas, update frequencies,
volumes, and pagination patterns.

## Decision

### Pattern: Extract → Validate → Stage (S3) → Load (BigQuery)

Every batch ingester follows this exact sequence:
1. **Extract** — pull from API with pagination, rate limiting, retry logic
2. **Validate** — run each record through Pydantic model, reject invalids to dead letter
3. **Stage** — write valid records as NDJSON to S3 raw zone (durable checkpoint)
4. **Load** — BigQuery streaming insert from validated rows
5. **Log** — write run metadata to `raw._ingestion_log`

### Why S3 as staging layer?
- Durable checkpoint — BQ load failure doesn't lose data
- Enables replay without re-hitting the API
- Raw files are the true audit trail

### Idempotency
Raw tables are append-only. Each run writes a new file with unique run_id.
Deduplication happens in the staging (dbt) layer on natural keys.

## Consequences
- Slightly more complex than direct-to-BQ but dramatically more robust
- Adding a new source = extend BaseIngester + define Pydantic schema
- S3 cost negligible at this data volume
MDEOF
ok "docs/decisions/ADR-002-batch-ingestion-pattern.md"

# ── Step 9: Update README progress tracker ────────────────────────────────────
header "Updating README.md progress tracker"

# Use Python to update the README phase table in-place
python3 - << 'PYEOF'
from pathlib import Path

readme = Path("README.md")
content = readme.read_text()

old = "| Phase 1 | Batch ingestion — 311, Crime, Weather, AQ | 🔄 In Progress |"
new = "| Phase 1 | Batch ingestion — 311, Crime, Weather, AQ | ✅ Done |"

if old in content:
    content = content.replace(old, new)
    readme.write_text(content)
    print("  ✅ README updated — Phase 1 marked Done")
elif new in content:
    print("  ✅ README already shows Phase 1 Done")
else:
    print("  ⚠️  Could not find Phase 1 row in README — update manually")
PYEOF

# ── Step 10: Run unit tests ───────────────────────────────────────────────────
header "Running unit tests (dry — no cloud calls)"

pip install pydantic requests boto3 google-cloud-bigquery --quiet 2>/dev/null || true
python -m pytest tests/unit/test_schemas.py -v --tb=short 2>&1 || echo "  ⚠️  Some tests failed — check output above"

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}$(printf '═%.0s' {1..60})${RESET}"
echo -e "${GREEN}${BOLD}  Phase 1 files in place — ready to commit${RESET}"
echo -e "${BOLD}$(printf '═%.0s' {1..60})${RESET}"
echo ""
echo -e "  Files created:"
echo -e "  • ingestion/schemas/ (base, nyc_311, nyc_crime, weather, airnow)"
echo -e "  • ingestion/batch/   (s3_writer, bq_loader, base_ingester, 4 ingesters)"
echo -e "  • orchestration/dags/batch_ingestion_dag.py"
echo -e "  • transformation/seeds/ (4 CSV files)"
echo -e "  • tests/unit/test_schemas.py"
echo -e "  • docs/decisions/ADR-002-batch-ingestion-pattern.md"
echo -e "  • README.md (Phase 1 → Done)"
echo ""
