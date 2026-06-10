"""GCS staging IO: parquet read/write and date-partitioned URI helpers.

The DAG lands data in object storage *between* tasks instead of pushing it
through XCom. Each partition writes to a date-keyed path and OVERWRITES it, so
re-running a date replaces that date's file rather than appending a new one.

This gives idempotency at the **storage layer** (deterministic, overwritten
paths) that mirrors the MERGE at the **warehouse layer** — re-running any day
converges to the same raw file, the same staged file, and the same target rows.

Layout:
    gs://<bucket>/raw/yellow_trips/dt=YYYY-MM-DD/data.parquet      (extract output)
    gs://<bucket>/staging/hourly_agg/dt=YYYY-MM-DD/data.parquet    (transform output)
"""

from __future__ import annotations

import datetime as dt
import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)

RAW_PREFIX = "raw/yellow_trips"
STAGING_PREFIX = "staging/hourly_agg"


def raw_uri(partition_date: dt.date | str, *, bucket: str | None = None) -> str:
    """GCS path for a date's raw extracted trips."""
    return f"gs://{_bucket(bucket)}/{RAW_PREFIX}/dt={_ds(partition_date)}/data.parquet"


def staging_uri(partition_date: dt.date | str, *, bucket: str | None = None) -> str:
    """GCS path for a date's transformed hourly aggregate."""
    return f"gs://{_bucket(bucket)}/{STAGING_PREFIX}/dt={_ds(partition_date)}/data.parquet"


def write_parquet(df: pd.DataFrame, uri: str) -> str:
    """Write a DataFrame to ``uri`` as parquet, overwriting any existing file.

    Returns the URI so it can be passed straight to the next task via XCom.
    """
    logger.info("Writing %d rows -> %s", len(df), uri)
    df.to_parquet(uri, index=False)
    return uri


def read_parquet(uri: str) -> pd.DataFrame:
    """Read a parquet file from ``uri`` (local or ``gs://``) into a DataFrame."""
    df = pd.read_parquet(uri)
    logger.info("Read %d rows from %s", len(df), uri)
    return df


def _bucket(bucket: str | None) -> str:
    """Resolve the target bucket from the argument or the GCS_BUCKET env var."""
    bucket = bucket or os.environ.get("GCS_BUCKET")
    if not bucket:
        raise ValueError(
            "GCS bucket not configured. Pass bucket=... or set the GCS_BUCKET env var."
        )
    return bucket


def _ds(partition_date: dt.date | str) -> str:
    """Normalize a date or ISO string to a 'YYYY-MM-DD' partition label."""
    if isinstance(partition_date, dt.date):
        return partition_date.isoformat()
    return partition_date
