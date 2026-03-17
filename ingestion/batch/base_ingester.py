"""ingestion/batch/base_ingester.py — Abstract base class for all batch ingesters."""

from __future__ import annotations

import logging
import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.batch.bq_loader import BigQueryLoader
from ingestion.batch.s3_writer import S3Writer
from ingestion.schemas.base import IngestionRunLog

logger = logging.getLogger(__name__)

RETRY_STRATEGY = Retry(
    total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"]
)


def build_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=RETRY_STRATEGY)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


class BaseIngester(ABC):
    source_name: str = ""
    table_name: str = ""

    def __init__(
        self,
        s3_writer: S3Writer | None = None,
        bq_loader: BigQueryLoader | None = None,
        dry_run: bool = False,
    ) -> None:
        self.s3 = s3_writer or S3Writer()
        self.bq = bq_loader or BigQueryLoader()
        self.dry_run = dry_run
        self.session = build_session()
        self.run_id = str(uuid.uuid4())
        self._run_log = IngestionRunLog(
            run_id=self.run_id, pipeline_name=self.__class__.__name__, source=self.source_name
        )

    @abstractmethod
    def fetch_records(self) -> Iterator[dict]:
        ...

    @abstractmethod
    def parse_record(self, raw: dict) -> object:
        ...

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

            logger.info(
                "Fetch done: read=%d valid=%d rejected=%d",
                records_read,
                records_valid,
                records_rejected,
            )

            if bq_rows and not self.dry_run:
                s3_path = self.s3.write_records(
                    records=bq_rows, source=self.source_name, run_id=self.run_id
                )
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
            logger.info(
                "Run %s | status=%s | read=%d written=%d rejected=%d",
                self.run_id[:8],
                self._run_log.status,
                self._run_log.records_read,
                self._run_log.records_written,
                self._run_log.records_rejected,
            )

        return self._run_log
