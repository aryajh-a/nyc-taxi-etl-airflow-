"""Create BigQuery sandbox datasets for the project.

Reads GCP_PROJECT_ID and BQ_DATASET_* from environment. Idempotent.
Invoked by `make seed-bq` via the `bq-init` docker-compose service.
"""

from __future__ import annotations

import os
import sys

from google.cloud import bigquery


def main() -> int:
    project = os.environ.get("GCP_PROJECT_ID")
    if not project:
        print("ERROR: GCP_PROJECT_ID env var not set", file=sys.stderr)
        return 1

    datasets = [
        os.environ.get("BQ_DATASET_RAW", "raw"),
        os.environ.get("BQ_DATASET_STAGING", "staging"),
        os.environ.get("BQ_DATASET_MARTS", "marts"),
        os.environ.get("BQ_DATASET_DQ", "dq"),
    ]

    client = bigquery.Client(project=project)

    print(f"Creating datasets in project: {project}")
    for ds_name in datasets:
        ds_id = f"{project}.{ds_name}"
        dataset = bigquery.Dataset(ds_id)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        print(f"  ok  {ds_id}")

    print(f"\nDone. {len(datasets)} datasets ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
