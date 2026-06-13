"""Transform: clean, enrich, and aggregate one partition of taxi trips.

Pipeline:
    1. Drop rows with nulls in the columns we depend on.
    2. Compute ``trip_duration_minutes`` from pickup/dropoff timestamps.
    3. (Optional) drop physically-impossible durations that would skew averages.
    4. Aggregate to one row per (pickup_date, pickup_hour).

Like extract, the transform is a pure function: same input DataFrame -> same
output DataFrame, with no I/O and no hidden state. That determinism is the
second link in the idempotency chain — re-running produces byte-identical
aggregates, so the downstream MERGE overwrites with the same values.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Columns required for the transform. A null in any of these makes the row
# unusable (can't compute duration, can't aggregate fares), so such rows are
# dropped up front.
_REQUIRED_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
]

# Schema of the aggregated output — also used to build an empty frame when a
# partition has no usable rows, so downstream code always sees the same columns.
_OUTPUT_COLUMNS = [
    "pickup_date",
    "pickup_hour",
    "trip_count",
    "avg_trip_duration_minutes",
    "avg_trip_distance",
    "avg_fare_amount",
    "total_revenue",
]


def transform(df: pd.DataFrame, *, drop_invalid_duration: bool = True) -> pd.DataFrame:
    """Clean and aggregate a day of trips into hourly buckets.

    Args:
        df: Raw trips for one date, as returned by ``extract.extract``.
        drop_invalid_duration: If True, drop trips whose dropoff is at or before
            pickup (duration <= 0). These are data-entry errors that would
            otherwise corrupt the average-duration metric.

    Returns:
        One row per (pickup_date, pickup_hour) with count, averages, and revenue.
        Columns are always ``_OUTPUT_COLUMNS``, even when the result is empty.
    """
    raw_rows = len(df)

    # 1. Drop nulls in the columns we rely on.
    df = df.dropna(subset=_REQUIRED_COLUMNS).copy()
    logger.info("Dropped %d rows with nulls (%d -> %d)", raw_rows - len(df), raw_rows, len(df))

    if df.empty:
        logger.warning("No rows left after cleaning; returning empty aggregate")
        return _empty_output()

    # 2. Enrich: duration in minutes, plus the grouping keys.
    df["trip_duration_minutes"] = (
        df["dropoff_datetime"] - df["pickup_datetime"]
    ).dt.total_seconds() / 60.0
    df["pickup_date"] = df["pickup_datetime"].dt.date
    df["pickup_hour"] = df["pickup_datetime"].dt.hour

    # 3. Drop physically-impossible trips (dropoff <= pickup).
    if drop_invalid_duration:
        before = len(df)
        df = df[df["trip_duration_minutes"] > 0]
        logger.info("Dropped %d rows with non-positive duration", before - len(df))
        if df.empty:
            return _empty_output()

    # 4. Aggregate to hourly buckets.
    agg = (
        df.groupby(["pickup_date", "pickup_hour"], as_index=False)
        .agg(
            trip_count=("trip_duration_minutes", "size"),
            avg_trip_duration_minutes=("trip_duration_minutes", "mean"),
            avg_trip_distance=("trip_distance", "mean"),
            avg_fare_amount=("fare_amount", "mean"),
            total_revenue=("total_amount", "sum"),
        )
    )

    # Round the floats so the stored values are stable and human-readable.
    for col in ["avg_trip_duration_minutes", "avg_trip_distance", "avg_fare_amount", "total_revenue"]:
        agg[col] = agg[col].round(2)

    logger.info("Aggregated into %d hourly buckets", len(agg))
    return agg[_OUTPUT_COLUMNS]


def _empty_output() -> pd.DataFrame:
    """Return an empty DataFrame with the canonical output schema."""
    return pd.DataFrame(columns=_OUTPUT_COLUMNS)


if __name__ == "__main__":
    # Smoke test against a live extract: python -m etl.transform 2022-01-15
    import sys

    from etl.extract import extract

    logging.basicConfig(level=logging.INFO)
    date_arg = sys.argv[1] if len(sys.argv) > 1 else "2022-01-15"
    result = transform(extract(date_arg))
    print(result.to_string(index=False))
