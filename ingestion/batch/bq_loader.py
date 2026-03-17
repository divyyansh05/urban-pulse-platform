"""ingestion/batch/bq_loader.py — Load records into BigQuery raw dataset."""

from __future__ import annotations

import logging
import os
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

    def stream_rows(
        self, table_name: str, rows: list[dict[str, Any]]
    ) -> tuple[int, int, list[str]]:
        if not rows:
            return 0, 0, []
        table_ref = self._table_ref(table_name)
        inserted, failed, errors = 0, 0, []
        for i in range(0, len(rows), STREAMING_BATCH_SIZE):
            batch = rows[i : i + STREAMING_BATCH_SIZE]
            try:
                insert_errors = self._client.insert_rows_json(
                    table_ref, batch, ignore_unknown_values=True
                )
                if insert_errors:
                    for err_item in insert_errors:
                        for err in err_item.get("errors", []):
                            errors.append(
                                f"Row {err_item.get('index','?')}: {err.get('message','unknown')}"
                            )
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
