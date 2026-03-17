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


with DAG(
    "nyc_311_batch_ingestion",
    default_args=DEFAULT_ARGS,
    description="NYC 311 incremental ingestion",
    schedule_interval="*/15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "batch", "nyc"],
) as dag_311:
    PythonOperator(task_id="ingest_nyc_311", python_callable=run_nyc_311)

with DAG(
    "nyc_crime_batch_ingestion",
    default_args=DEFAULT_ARGS,
    description="NYPD Crime daily ingestion",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "batch", "nyc"],
) as dag_crime:
    PythonOperator(task_id="ingest_nyc_crime", python_callable=run_nyc_crime)

with DAG(
    "weather_batch_ingestion",
    default_args=DEFAULT_ARGS,
    description="Weather hourly ingestion",
    schedule_interval="5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "batch", "weather"],
) as dag_weather:
    PythonOperator(task_id="ingest_weather", python_callable=run_weather)

with DAG(
    "airnow_batch_ingestion",
    default_args=DEFAULT_ARGS,
    description="AirNow hourly ingestion",
    schedule_interval="10 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "batch", "air_quality"],
) as dag_airnow:
    PythonOperator(task_id="ingest_airnow", python_callable=run_airnow)
