"""Email alerts: task-failure notifications and data-quality warnings.

Two distinct alert paths:

  * **Failure** — wired as an ``on_failure_callback`` in the DAG. Fires when a
    task errors out *after its retries are exhausted*, so transient blips that
    self-heal don't spam you.

  * **Data quality** — a "succeeded but suspicious" warning. A task can finish
    green while the data is wrong: e.g. a partition with zero or unexpectedly
    few hourly rows. The pipeline still completes (the load is idempotent, so a
    later re-run fixes it); we just email so a silently-empty day isn't missed.
    This is the re-runnability theme applied to monitoring — a day that suddenly
    produces fewer rows than usual is exactly what you want flagged.

Both use Airflow's ``send_email``, which reads SMTP settings from the ``[smtp]``
config section (see the README's Alerting setup). Recipients come from the
``alert_email`` Airflow Variable (comma-separated). If it's unset, alerts are
logged and skipped rather than raising — so a missing mail config never breaks
the DAG.
"""

from __future__ import annotations

import datetime as dt
import logging

import pandas as pd
from airflow.models import Variable
from airflow.utils.email import send_email

logger = logging.getLogger(__name__)

# A full day has up to 24 hourly rows. Fewer than this looks suspicious enough
# to warrant a heads-up (but not to fail the run).
DEFAULT_MIN_ROWS = 20


def task_failure_alert(context: dict) -> None:
    """``on_failure_callback``: email a formatted failure notice.

    Airflow passes the run context; we pull the task instance, partition date,
    and exception out of it to build the message.
    """
    to = _recipients()
    if not to:
        logger.warning("alert_email Variable not set; skipping failure email")
        return

    ti = context["task_instance"]
    subject = f"[Airflow] FAILED: {ti.dag_id}.{ti.task_id} ({context['ds']})"
    html = f"""
        <h3>Task failed</h3>
        <ul>
          <li><b>DAG:</b> {ti.dag_id}</li>
          <li><b>Task:</b> {ti.task_id}</li>
          <li><b>Partition (ds):</b> {context['ds']}</li>
          <li><b>Exception:</b> {context.get('exception')}</li>
          <li><a href="{ti.log_url}">View task logs</a></li>
        </ul>
    """
    send_email(to=to, subject=subject, html_content=html)
    logger.info("Sent failure alert to %s", to)


def check_partition_quality(
    df: pd.DataFrame,
    partition_date: dt.date | str,
    *,
    min_rows: int = DEFAULT_MIN_ROWS,
) -> None:
    """Email a warning if the aggregate for a partition looks anomalous.

    Deliberately does **not** raise — the task still succeeds. The goal is
    visibility into a thin/empty day, not to halt the pipeline.
    """
    issues: list[str] = []
    if df.empty:
        issues.append("aggregate is EMPTY (0 hourly rows)")
    elif len(df) < min_rows:
        issues.append(
            f"only {len(df)} hourly rows (a full day has up to 24; threshold is {min_rows})"
        )

    if not issues:
        logger.info("Quality check passed for %s (%d rows)", partition_date, len(df))
        return

    logger.warning("Data-quality issues for %s: %s", partition_date, issues)

    to = _recipients()
    if not to:
        logger.warning("alert_email Variable not set; skipping data-quality email")
        return

    items = "".join(f"<li>{issue}</li>" for issue in issues)
    html = f"""
        <h3>Data quality warning</h3>
        <p>Partition <b>{partition_date}</b> completed but looks suspicious:</p>
        <ul>{items}</ul>
        <p>The pipeline still finished; review the source data for this date.
           Re-running the partition is safe (idempotent).</p>
    """
    send_email(
        to=to,
        subject=f"[Airflow] DQ warning: nyc_taxi_hourly_etl ({partition_date})",
        html_content=html,
    )
    logger.info("Sent data-quality alert to %s", to)


def _recipients() -> list[str]:
    """Parse the comma-separated ``alert_email`` Airflow Variable into addresses."""
    raw = Variable.get("alert_email", default_var="")
    return [addr.strip() for addr in raw.split(",") if addr.strip()]
