"""Load: MERGE the hourly aggregate for one partition into the target table.

This is where idempotency is enforced. Extract and transform are deterministic,
so for a given date they always hand us the same aggregate rows. The load step
must take those rows and reach the *same final table state* no matter how many
times it runs — never appending duplicates.

We do that with a BigQuery MERGE instead of an INSERT:

    1. Write this run's aggregate to a per-date staging table (WRITE_TRUNCATE,
       so re-running overwrites the staging cleanly).
    2. MERGE staging -> target, keyed on (pickup_date, pickup_hour):
         - matched rows are UPDATEd in place (no duplicate inserted)
         - new rows are INSERTed
         - target rows for THIS date that are no longer in the source are
           DELETEd, so the date's slice of the table exactly mirrors the source.

An INSERT would grow the table by ~24 rows every re-run; the MERGE leaves it
identical. That is the whole point of "safely re-runnable."
"""

from __future__ import annotations

import datetime as dt
import logging

import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)

DEFAULT_DATASET = "taxi"
DEFAULT_TABLE = "trips_hourly_agg"

# Target schema. Declared explicitly (rather than letting BQ autodetect from the
# DataFrame) so pickup_date lands as DATE and pickup_hour as INTEGER — autodetect
# would guess TIMESTAMP/FLOAT and break the MERGE key comparison.
TARGET_SCHEMA = [
    bigquery.SchemaField("pickup_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("pickup_hour", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("trip_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("avg_trip_duration_minutes", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("avg_trip_distance", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("avg_fare_amount", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("total_revenue", "FLOAT", mode="NULLABLE"),
]

# Non-key columns, used to build the UPDATE/INSERT clauses without repeating the
# column list by hand.
_KEY_COLUMNS = ["pickup_date", "pickup_hour"]
_VALUE_COLUMNS = [f.name for f in TARGET_SCHEMA if f.name not in _KEY_COLUMNS]


def load(
    df: pd.DataFrame,
    partition_date: dt.date | str,
    *,
    project_id: str | None = None,
    client: bigquery.Client | None = None,
    dataset: str = DEFAULT_DATASET,
    table: str = DEFAULT_TABLE,
    location: str = "US",
) -> None:
    """Idempotently MERGE one date's hourly aggregate into the target table.

    Args:
        df: The aggregated DataFrame from ``transform.transform``.
        partition_date: The date this aggregate belongs to. Used both to name the
            staging table and to scope the MERGE's delete clause.
        project_id: GCP billing/target project. Defaults to the ambient project.
        client: Existing BigQuery client to reuse; created if omitted.
        dataset / table: Target location, ``<project>.<dataset>.<table>``.
        location: BQ location for the dataset (must match where it lives).
    """
    partition_date = _coerce_date(partition_date)
    client = client or bigquery.Client(project=project_id)
    project = client.project

    target_ref = f"{project}.{dataset}.{table}"
    staging_ref = f"{project}.{dataset}.{table}__stg_{partition_date:%Y%m%d}"

    _ensure_dataset(client, dataset, location)
    _ensure_target_table(client, target_ref)

    if df.empty:
        # Nothing to load. We deliberately do NOT run the MERGE here: an empty
        # source would delete any existing rows for this date. Skipping is the
        # safer default for a transient empty extract; flip this if you want a
        # truly empty day to wipe the date's slice.
        logger.warning("Empty aggregate for %s; nothing to merge", partition_date)
        return

    _load_staging(client, staging_ref, df, location)
    _merge(client, target_ref, staging_ref, partition_date)
    _drop_staging(client, staging_ref)

    logger.info("Load complete for %s -> %s", partition_date, target_ref)


def _ensure_dataset(client: bigquery.Client, dataset: str, location: str) -> None:
    """Create the dataset if it does not already exist."""
    ds = bigquery.Dataset(f"{client.project}.{dataset}")
    ds.location = location
    client.create_dataset(ds, exists_ok=True)


def _ensure_target_table(client: bigquery.Client, target_ref: str) -> None:
    """Create the target table with the canonical schema if missing.

    The MERGE statement requires the target to already exist, so on the very
    first run we create it. ``exists_ok=True`` makes this a no-op afterwards.
    """
    table = bigquery.Table(target_ref, schema=TARGET_SCHEMA)
    client.create_table(table, exists_ok=True)


def _load_staging(
    client: bigquery.Client, staging_ref: str, df: pd.DataFrame, location: str
) -> None:
    """Overwrite the per-date staging table with this run's aggregate."""
    job_config = bigquery.LoadJobConfig(
        schema=TARGET_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(
        df, staging_ref, job_config=job_config, location=location
    )
    job.result()  # wait for completion
    logger.info("Loaded %d rows into staging %s", len(df), staging_ref)


def _merge(
    client: bigquery.Client,
    target_ref: str,
    staging_ref: str,
    partition_date: dt.date,
) -> None:
    """Run the idempotent MERGE from staging into the target table."""
    update_clause = ",\n            ".join(f"T.{c} = S.{c}" for c in _VALUE_COLUMNS)
    all_columns = _KEY_COLUMNS + _VALUE_COLUMNS
    insert_cols = ", ".join(all_columns)
    insert_vals = ", ".join(f"S.{c}" for c in all_columns)

    merge_sql = f"""
        MERGE `{target_ref}` T
        USING `{staging_ref}` S
          ON T.pickup_date = S.pickup_date
         AND T.pickup_hour = S.pickup_hour
        WHEN MATCHED THEN UPDATE SET
            {update_clause}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
        WHEN NOT MATCHED BY SOURCE AND T.pickup_date = @partition_date THEN
            DELETE
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("partition_date", "DATE", partition_date),
        ]
    )
    job = client.query(merge_sql, job_config=job_config)
    job.result()
    logger.info("MERGE affected %s rows in %s", job.num_dml_affected_rows, target_ref)


def _drop_staging(client: bigquery.Client, staging_ref: str) -> None:
    """Remove the staging table once the MERGE has succeeded."""
    client.delete_table(staging_ref, not_found_ok=True)


def _coerce_date(value: dt.date | str) -> dt.date:
    """Accept a date or an ISO ``YYYY-MM-DD`` string and return a ``date``."""
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(value)


if __name__ == "__main__":
    # End-to-end smoke test: python -m etl.load 2022-01-15
    import sys

    from etl.extract import extract
    from etl.transform import transform

    logging.basicConfig(level=logging.INFO)
    date_arg = sys.argv[1] if len(sys.argv) > 1 else "2022-01-15"
    load(transform(extract(date_arg)), date_arg)
    print(f"Loaded {date_arg}. Run again — the row count should not change.")
