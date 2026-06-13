# NYC Taxi ETL — Idempotent Hourly Aggregation

A daily, **safely re-runnable** ETL pipeline that aggregates NYC yellow-taxi
trips into hourly metrics. Built with Apache Airflow, Google Cloud Storage, and
BigQuery.

The design goal is idempotency: **running the pipeline twice for the same date
produces the same result — never duplicate data.** This is enforced at two
layers (date-partitioned GCS paths and a BigQuery `MERGE`), explained below.

---

## Architecture

```
                          ┌─────────────────────────────────────────────┐
                          │            Airflow DAG (daily)               │
                          │         nyc_taxi_hourly_etl, per ds          │
                          └─────────────────────────────────────────────┘
                                            │
        ┌───────────────────────┬───────────┴───────────┬───────────────────────┐
        ▼                       ▼                        ▼                       
  ┌───────────┐           ┌───────────┐            ┌───────────┐
  │ extract   │           │ transform │            │  load     │
  └─────┬─────┘           └─────┬─────┘            └─────┬─────┘
        │                       │                        │
  reads │ BQ public       reads │ raw parquet      reads │ staged parquet
  dataset                 writes│ staged parquet   MERGE │ into target
        ▼                       ▼                        ▼
  gs://…/raw/             gs://…/staging/           BigQuery
  yellow_trips/           hourly_agg/               <project>.taxi.
  dt=YYYY-MM-DD/          dt=YYYY-MM-DD/            trips_hourly_agg
  data.parquet           data.parquet
```

A layered **raw → staging → curated** flow. Each Airflow task does one stage and
hands the next task a **GCS URI** (a small string via XCom); the actual data
lives in parquet on GCS, never in Airflow's metadata DB.

| Stage | Reads | Writes | Module |
|-------|-------|--------|--------|
| **extract** | `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022` | raw parquet (GCS) | [etl/extract.py](etl/extract.py) |
| **transform** | raw parquet | staged parquet (GCS) | [etl/transform.py](etl/transform.py) |
| **load** | staged parquet | `MERGE` into BQ target | [etl/load.py](etl/load.py) |

### Output schema (`trips_hourly_agg`)

One row per `(pickup_date, pickup_hour)`:

| Column | Type | Meaning |
|--------|------|---------|
| `pickup_date` | DATE | partition day (MERGE key) |
| `pickup_hour` | INTEGER | hour of day 0–23 (MERGE key) |
| `trip_count` | INTEGER | trips in that hour |
| `avg_trip_duration_minutes` | FLOAT | mean trip duration |
| `avg_trip_distance` | FLOAT | mean distance |
| `avg_fare_amount` | FLOAT | mean fare |
| `total_revenue` | FLOAT | sum of `total_amount` |

---

## How idempotency works

"Idempotent" means a re-run reaches the **same final state** as the first run —
no duplicates, no drift. Three properties combine to guarantee it:

**1. The date comes from the logical date, never the wall clock.**
The DAG processes Airflow's `ds` (the run's logical date), not `datetime.now()`.
So a re-run, a cleared task, or a backfill of `2022-03-09` always reprocesses
*that same partition*. Every module filters/keys on this date.

**2. Storage layer — date-partitioned paths that overwrite.**
Extract and transform write to deterministic, date-keyed GCS paths
(`.../dt=2022-03-09/data.parquet`) and **overwrite** them. Re-running a day
replaces its raw and staged files rather than appending new ones.

**3. Warehouse layer — `MERGE`, not `INSERT`.**
The load step uses a BigQuery `MERGE` keyed on `(pickup_date, pickup_hour)`:

```sql
MERGE `target` T
USING `staging` S
  ON T.pickup_date = S.pickup_date
 AND T.pickup_hour = S.pickup_hour
WHEN MATCHED THEN UPDATE SET ...                 -- row exists → overwrite in place
WHEN NOT MATCHED BY TARGET THEN INSERT ...       -- new hour → add it
WHEN NOT MATCHED BY SOURCE
     AND T.pickup_date = @partition_date THEN
     DELETE                                       -- stale row for this date → remove
```

- **MATCHED → UPDATE**: a re-run finds the existing `(date, hour)` row and
  overwrites it with identical values — no duplicate row is appended. An
  `INSERT` would instead grow the table by ~24 rows on every re-run.
- **NOT MATCHED BY TARGET → INSERT**: first run, or a genuinely new hour.
- **NOT MATCHED BY SOURCE … DELETE**: if a recompute yields *fewer* hours than
  before, stale rows are removed so the date's slice exactly mirrors the source.
  The `AND T.pickup_date = @partition_date` scopes the delete to **this date
  only** — without it, the clause would delete every other day's rows.

**Net effect:** clear and re-run any task in the Airflow UI and the table
converges to the same state.

---

## Project structure

```
nyc-taxi-etl/
├── dags/
│   └── etl_dag.py        # Airflow DAG: extract → transform → load (per ds)
├── etl/
│   ├── extract.py        # BQ public dataset → DataFrame
│   ├── transform.py      # clean, compute duration, aggregate hourly
│   ├── load.py           # idempotent MERGE into the target table
│   └── storage.py        # GCS parquet IO + date-partitioned URI helpers
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Prerequisites

- A GCP project with **billing enabled** (queries against the public dataset are
  billed to *your* project; the free tier covers 1 TB scanned/month).
- A **GCS bucket** for the raw/staging layers.
- The **`gcloud` CLI** for authentication.
- **Python 3.10+**. Note: `apache-airflow` does not install cleanly on native
  Windows — use WSL, macOS, or Linux for the full Airflow run.

---

## Setup

```bash
# 1. Install dependencies (use a virtualenv)
python -m venv .venv
source .venv/bin/activate             # Linux/macOS/WSL  (Windows: .venv\Scripts\Activate.ps1)
pip install -r requirements.txt

# 2. Authenticate to GCP
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID

# 3. Create a bucket for the raw/staging layers
gcloud storage buckets create gs://YOUR_BUCKET_NAME --location=US

# 4. Point the pipeline at your project and bucket
export GOOGLE_CLOUD_PROJECT=YOUR_PROJECT_ID
export GCS_BUCKET=YOUR_BUCKET_NAME
```

> The target dataset/table (`taxi.trips_hourly_agg`) is created automatically on
> the first load run. To change names, pass `dataset=`/`table=` to `load()` (see
> [etl/load.py](etl/load.py)).

---

## Running locally

### Quick smoke tests (no Airflow needed)

Each module is runnable on its own for a fast sanity check against live data:

```bash
# Extract one day → prints head + row count
python -m etl.extract 2022-01-15

# Extract + transform → prints the ~24-row hourly aggregate
python -m etl.transform 2022-01-15

# Full extract + transform + load → MERGEs into BigQuery
python -m etl.load 2022-01-15
```

Run `python -m etl.load 2022-01-15` **twice** and query the target table — the
row count does not change the second time. That is idempotency in action.

> Run these from the project root with the `-m` module form so the `etl.*`
> imports resolve.

### Running via Airflow

```bash
# Point Airflow at this project's dags/ folder
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export PYTHONPATH=$(pwd)            # so tasks can import the etl package

airflow standalone                 # starts scheduler + webserver (dev mode)
```

Open the UI (http://localhost:8080), find **`nyc_taxi_hourly_etl`**, and trigger
a run or unpause it.

### Backfilling a date range

`catchup=False`, so unpausing does **not** auto-run all of 2022. To process a
deliberate range:

```bash
airflow dags backfill nyc_taxi_hourly_etl \
  --start-date 2022-01-01 --end-date 2022-01-07
```

`max_active_runs=3` caps how many partitions run in parallel, so a backfill
won't flood BigQuery. Because the pipeline is idempotent, you can re-run any
range safely.

---

## Alerting

The DAG emails on two conditions ([etl/alerts.py](etl/alerts.py)):

| Alert | Trigger | Effect |
|-------|---------|--------|
| **Failure** | any task errors out *after retries are exhausted* | `on_failure_callback` emails a notice with the task, partition, exception, and a log link |
| **Data quality** | a partition produces **0** or **< 20** hourly rows | `quality_check_task` emails a warning — the run still succeeds (re-run later; it's idempotent) |

The data-quality check is a **non-blocking gate** between transform and load: it
flags a silently-thin day without halting the pipeline. Failures retry twice
(5-min delay) before alerting, so transient blips don't spam you.

### Setup

Alerts use Airflow's `send_email`, configured via the `[smtp]` settings (here as
env vars) plus an `alert_email` **Airflow Variable** for the recipient list:

```bash
# Recipient list — set as an Airflow Variable (comma-separated for multiple)
airflow variables set alert_email "you@example.com"

# SMTP config (env vars)
export AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
export AIRFLOW__SMTP__SMTP_PORT=587
export AIRFLOW__SMTP__SMTP_STARTTLS=True
export AIRFLOW__SMTP__SMTP_USER=you@example.com
export AIRFLOW__SMTP__SMTP_PASSWORD=your_app_password   # Gmail: use an App Password
export AIRFLOW__SMTP__SMTP_MAIL_FROM=you@example.com
```

You can also set the Variable in the UI under **Admin → Variables** (key
`alert_email`).

> If the `alert_email` Variable is unset, alerts are logged and skipped rather
> than raising — a missing mail config never breaks the DAG.

## Cost notes

- The extract filters on `DATE(pickup_datetime)`, which **cannot prune** by a
  native partition column, so each run scans the year's `pickup_datetime` column
  (a few hundred MB). Comfortably within BQ's 1 TB/month free tier, but it means
  repeated full-year backfills are not free forever.
- GCS staging stores small parquet files (~tens of KB/day) — effectively free.

---

## Tech stack

Python · Apache Airflow · Google Cloud Storage · BigQuery · pandas · pyarrow
