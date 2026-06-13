"""Airflow DAG: daily NYC yellow-taxi hourly aggregation.

Idempotency is driven by the **logical date** (Airflow's ``ds``). Each DAG run
processes exactly one calendar-day partition, and every layer keys off that date:

    extract_task    BQ pull            -> raw parquet in GCS      (gs://.../raw/dt=ds/)
    transform_task  read raw, aggregate -> staged parquet in GCS  (gs://.../staging/dt=ds/)
    load_task       read staged         -> MERGE into BQ target

The three tasks hand off **GCS URIs** through XCom — small strings, the correct
use of XCom — while the actual data lands in object storage. This is a layered
raw -> staging -> curated pipeline rather than a single monolithic task.

Two layers of idempotency:
    * Storage: each GCS path is date-keyed and overwritten, so re-running a day
      replaces its files.
    * Warehouse: the load MERGEs on (pickup_date, pickup_hour), so the target
      converges to the same rows with no duplicates.

Because the date comes from the run's logical date — never ``datetime.now()`` —
re-running or backfilling a day is safe: clearing and re-running any task in the
Airflow UI reprocesses the same partition to the same final state.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from etl.alerts import check_partition_quality, task_failure_alert
from etl.extract import extract
from etl.load import load
from etl.storage import raw_uri, read_parquet, staging_uri, write_parquet
from etl.transform import transform

logger = logging.getLogger(__name__)

# The public dataset only covers 2022, so the DAG's window is bounded to it.
START_DATE = pendulum.datetime(2022, 1, 1, tz="UTC")
END_DATE = pendulum.datetime(2022, 12, 31, tz="UTC")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "depends_on_past": False,
    # Email on failure once retries are exhausted. We use a custom callback
    # (richer message + log link) instead of email_on_failure's default mail.
    "email_on_failure": False,
    "on_failure_callback": task_failure_alert,
}


@dag(
    dag_id="nyc_taxi_hourly_etl",
    description="Daily hourly aggregation of NYC yellow-taxi trips (GCS staging + idempotent MERGE).",
    schedule="@daily",
    start_date=START_DATE,
    end_date=END_DATE,
    catchup=False,  # don't auto-backfill all of 2022 on first unpause; see README
    max_active_runs=3,  # cap parallel partitions during a manual backfill
    default_args=default_args,
    tags=["etl", "bigquery", "gcs", "nyc-taxi"],
)
def nyc_taxi_hourly_etl():
    @task
    def extract_task() -> str:
        """Pull the logical date's trips from BQ and land them as raw parquet."""
        ds = get_current_context()["ds"]
        logger.info("Extracting partition %s", ds)
        df = extract(ds)
        return write_parquet(df, raw_uri(ds))

    @task
    def transform_task(raw_path: str) -> str:
        """Read raw parquet, aggregate to hourly buckets, stage the result."""
        ds = get_current_context()["ds"]
        df = read_parquet(raw_path)
        agg = transform(df)
        return write_parquet(agg, staging_uri(ds))

    @task
    def quality_check_task(staging_path: str) -> str:
        """Inspect the staged aggregate and warn (by email) if it looks thin.

        Passes the URI straight through to load — this is a non-blocking gate:
        it emails on a suspicious partition but never fails the run.
        """
        ds = get_current_context()["ds"]
        agg = read_parquet(staging_path)
        check_partition_quality(agg, ds)
        return staging_path

    @task
    def load_task(staging_path: str) -> None:
        """Read the staged aggregate and MERGE it into the target table."""
        ds = get_current_context()["ds"]
        agg = read_parquet(staging_path)
        load(agg, ds)
        logger.info("Loaded partition %s", ds)

    # Dependencies are expressed by passing each task's return (a GCS URI) to the
    # next: extract -> transform -> quality_check -> load.
    load_task(quality_check_task(transform_task(extract_task())))


nyc_taxi_hourly_etl()
