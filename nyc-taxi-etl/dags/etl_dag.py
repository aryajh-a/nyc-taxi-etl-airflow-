"""Airflow DAG: daily NYC yellow-taxi hourly aggregation.

Idempotency is driven entirely by the **logical date** (Airflow's ``ds``). Each
DAG run processes exactly one calendar-day partition, and every step downstream
keys off that date:

    extract(ds)  ->  one day of trips
    transform()  ->  that day's hourly aggregate
    load(ds)     ->  MERGE that day into the target table

Because the date comes from the run's logical date — never ``datetime.now()`` —
re-running or backfilling a given day reprocesses the *same* partition and the
MERGE overwrites it in place. Clearing and re-running a task in the Airflow UI is
therefore safe: the table converges to the same state, with no duplicates.

Design note — why one task, not three:
    Splitting extract/transform/load into separate Airflow tasks would mean
    passing the (~100k-row) extracted DataFrame between them via XCom, which
    serializes through the metadata DB — an anti-pattern at that size. Keeping
    the per-partition pipeline in a single task avoids that. To get true task
    separation you would stage the intermediate to GCS/BigQuery and hand off a
    URI instead; that is the right move at scale but overkill here.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from etl.extract import extract
from etl.load import load
from etl.transform import transform

logger = logging.getLogger(__name__)

# The public dataset only covers 2022, so the DAG's window is bounded to it.
# start_date is the first logical date; with @daily, a run for date D fires
# shortly after D ends.
START_DATE = pendulum.datetime(2022, 1, 1, tz="UTC")
END_DATE = pendulum.datetime(2022, 12, 31, tz="UTC")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "depends_on_past": False,
}


@dag(
    dag_id="nyc_taxi_hourly_etl",
    description="Daily hourly aggregation of NYC yellow-taxi trips (idempotent MERGE).",
    schedule="@daily",
    start_date=START_DATE,
    end_date=END_DATE,
    catchup=False,  # don't auto-backfill all of 2022 on first unpause; see README
    max_active_runs=3,  # cap parallel partitions during a manual backfill
    default_args=default_args,
    tags=["etl", "bigquery", "nyc-taxi"],
)
def nyc_taxi_hourly_etl():
    @task
    def run_partition() -> None:
        """Run extract -> transform -> load for this run's logical date."""
        # ``ds`` is the logical date as 'YYYY-MM-DD'. Pulling it from the run
        # context (not the wall clock) is what makes the run idempotent.
        ctx = get_current_context()
        ds = ctx["ds"]

        logger.info("Starting ETL for partition %s", ds)
        raw = extract(ds)
        agg = transform(raw)
        load(agg, ds)
        logger.info("Finished ETL for partition %s", ds)

    run_partition()


nyc_taxi_hourly_etl()
