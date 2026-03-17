"""ingestion/batch/s3_writer.py — Write NDJSON files to AWS S3."""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import UTC, datetime
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
        dt = dt or datetime.now(UTC)
        return str(
            PurePosixPath(source)
            / str(dt.year)
            / f"{dt.month:02d}"
            / f"{dt.day:02d}"
            / f"{run_id}.ndjson"
        )

    def write_records(
        self, records: list[dict], source: str, run_id: str, dt: datetime | None = None
    ) -> str:
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
                buffer,
                self.bucket,
                key,
                ExtraArgs={
                    "ContentType": "application/x-ndjson",
                    "Metadata": {
                        "run_id": run_id,
                        "source": source,
                        "record_count": str(len(records)),
                    },
                },
            )
        except ClientError as e:
            logger.error("S3 upload failed: %s", e)
            raise
        s3_path = f"s3://{self.bucket}/{key}"
        logger.info("Wrote to %s", s3_path)
        return s3_path
