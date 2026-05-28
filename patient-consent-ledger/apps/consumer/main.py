"""Phase 0 placeholder. Real Kafka -> BigQuery consumer arrives in Phase 2."""

from __future__ import annotations

import os
import time


def main() -> None:
    print(f"[consumer] phase-0 placeholder running. Will consume from "
          f"{os.environ.get('KAFKA_TOPIC_RAW', '?')} on {os.environ.get('KAFKA_BOOTSTRAP', '?')}.")
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
