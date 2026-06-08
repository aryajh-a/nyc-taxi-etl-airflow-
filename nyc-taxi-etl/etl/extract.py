"""Extract: pull a single date partition from the BQ public taxi dataset.

Source: bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022

The extract is deterministic: for a given `partition_date` it always returns the
same set of rows. That determinism is the first link in the idempotency chain —
re-running for the same date re-reads exactly the same source rows, so the
downstream MERGE can safely overwrite the previous run's output.
"""

from __future__ import annotations

import datetime as dt
import logging

import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)

SOURCE_TABLE = "bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022"

# Only the columns the transform step actually needs. Selecting an explicit list
# (instead of SELECT *) keeps the scanned bytes — and therefore the BQ bill —
# predictable.
_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
]


def extract(
    partition_date: dt.date | str,
    *,
    project_id: str | None = None,
    client: bigquery.Client | None = None,
    source_table: str = SOURCE_TABLE,
) -> pd.DataFrame:
    """Return one day of yellow-taxi trips as a DataFrame.

    Args:
        partition_date: The day to pull, as a ``date`` or ``YYYY-MM-DD`` string.
            Filtered against ``DATE(pickup_datetime)``.
        project_id: GCP project used to *run* the query (billing project). Not
            required if it is already set in the ambient credentials/env.
        client: An existing BigQuery client to reuse. If omitted, one is created.
        source_table: Fully-qualified source table; overridable for tests.

    Returns:
        A pandas DataFrame with the columns in ``_COLUMNS``. Empty if the date
        has no trips.
    """
    partition_date = _coerce_date(partition_date)
    client = client or bigquery.Client(project=project_id)

    query = f"""
        SELECT {", ".join(_COLUMNS)}
        FROM `{source_table}`
        WHERE DATE(pickup_datetime) = @partition_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("partition_date", "DATE", partition_date),
        ]
    )

    logger.info("Extracting %s for partition_date=%s", source_table, partition_date)
    df = client.query(query, job_config=job_config).to_dataframe()
    logger.info("Extracted %d rows for %s", len(df), partition_date)

    return df


def _coerce_date(value: dt.date | str) -> dt.date:
    """Accept a date or an ISO ``YYYY-MM-DD`` string and return a ``date``."""
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(value)


if __name__ == "__main__":
    # Smoke test: python -m etl.extract 2022-01-15
    import sys

    logging.basicConfig(level=logging.INFO)
    date_arg = sys.argv[1] if len(sys.argv) > 1 else "2022-01-15"
    frame = extract(date_arg)
    print(frame.head())
    print(f"\n{len(frame)} rows")
