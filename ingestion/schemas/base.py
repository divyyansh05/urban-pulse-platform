"""
ingestion/schemas/base.py
Base Pydantic models and shared types used across all ingestion schemas.
"""

import uuid
from datetime import UTC, date, datetime

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(UTC)


def today_utc() -> date:
    return datetime.now(UTC).date()


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
