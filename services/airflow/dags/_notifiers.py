"""Shared email-alert notifiers for DAGs (Mailpit / smtp_default).

Opt a DAG in by importing these and passing them to the DAG (or task)
callbacks::

    from _notifiers import alert_on_failure, alert_on_success

    with DAG(..., on_failure_callback=alert_on_failure(),
                  on_success_callback=alert_on_success()) as dag:
        ...

Both build an SmtpNotifier that sends through the pre-wired ``smtp_default``
connection (points at the in-network Mailpit sink — see
services/airflow/docker-compose.yml). Recipients and the From address come
from the worker env (ALERT_EMAIL_*, AIRFLOW_SMTP_FROM), set in the same compose
file. The leading underscore keeps this module out of DAG-discovery noise.
"""
from __future__ import annotations

import os

from airflow.providers.smtp.notifications.smtp import SmtpNotifier

_FROM = os.getenv("AIRFLOW_SMTP_FROM", "airflow@lakehouse.local")
_RECIPIENTS = [
    e for e in (os.getenv("ALERT_EMAIL_ADMIN"), os.getenv("ALERT_EMAIL_DATATEAM")) if e
]


def alert_on_failure(to: list[str] | None = None) -> SmtpNotifier:
    """on_failure_callback that emails admin + datateam via Mailpit."""
    return SmtpNotifier(
        from_email=_FROM,
        to=to or _RECIPIENTS,
        subject="[Airflow] {{ dag.dag_id }}.{{ ti.task_id }} failed",
        html_content=(
            "DAG <b>{{ dag.dag_id }}</b> task <b>{{ ti.task_id }}</b> failed.<br>"
            "run_id: {{ run_id }}<br>logical_date: {{ ds }}<br>"
            '<a href="{{ ti.log_url }}">View logs</a>'
        ),
    )


def alert_on_success(to: list[str] | None = None) -> SmtpNotifier:
    """on_success_callback that emails admin + datateam on success.

    Set this at the DAG level so it fires once per successful run rather than
    once per task.
    """
    return SmtpNotifier(
        from_email=_FROM,
        to=to or _RECIPIENTS,
        subject="[Airflow] {{ dag.dag_id }} succeeded",
        html_content=(
            "DAG <b>{{ dag.dag_id }}</b> completed successfully.<br>"
            "run_id: {{ run_id }}<br>logical_date: {{ ds }}"
        ),
    )
