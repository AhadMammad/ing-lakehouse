"""Shared Telegram-alert callbacks for DAGs (sibling of _notifiers.py).

Opt a DAG in alongside the email notifiers::

    from _notifiers import alert_on_failure, alert_on_success
    from _telegram_notifiers import telegram_alert_on_failure, telegram_alert_on_success

    with DAG(...,
             on_failure_callback=[alert_on_failure(), telegram_alert_on_failure()],
             on_success_callback=[alert_on_success(), telegram_alert_on_success()]):
        ...

Implementation note: the ``apache-airflow-providers-telegram`` version pinned for
Airflow 3.2.1 (4.9.4) ships only ``hooks`` + ``operators`` — there is **no**
``TelegramNotifier`` class (unlike the smtp provider's ``SmtpNotifier``). So these
helpers post via ``TelegramHook`` directly inside a plain callback. The factory
returns a ``callback(context)`` function, which slots into a DAG ``*_callback``
list right next to the email ``SmtpNotifier``.

Token + chat id come from the worker env (``TELEGRAM_BOT_TOKEN`` /
``TELEGRAM_CHAT_ID``), set in services/airflow/docker-compose.yml and sourced from
.env.<instance> — mirroring how _notifiers.py reads ALERT_EMAIL_*. They are passed
straight to the hook (no AIRFLOW_CONN_* — the bot token's ':' breaks the
connection-URI form). If either is blank the callback logs and returns, so a
fresh clone parses fine and the email alert still fires. The leading underscore
keeps this module out of DAG-discovery noise.

Channel chat ids look like ``-1001234567890`` (note the leading ``-100``); set the
full value including the minus sign in TELEGRAM_CHAT_ID.
"""
from __future__ import annotations

import logging

import os

from airflow.providers.telegram.hooks.telegram import TelegramHook

log = logging.getLogger(__name__)

_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def _send(text: str, chat_id: str | None) -> None:
    """Post one message; no-op (with a warning) when creds are missing."""
    cid = chat_id or _CHAT_ID
    if not _TOKEN or not cid:
        log.warning(
            "Telegram alert skipped: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set "
            "in the worker env (.env.<instance>)."
        )
        return
    TelegramHook(token=_TOKEN, chat_id=str(cid)).send_message({"text": text})


def _dag_id(context) -> str:
    dag = context.get("dag")
    if dag is not None and getattr(dag, "dag_id", None):
        return dag.dag_id
    ti = context.get("task_instance") or context.get("ti")
    return getattr(ti, "dag_id", "<unknown dag>")


def _run_id(context) -> str:
    if context.get("run_id"):
        return context["run_id"]
    return getattr(context.get("dag_run"), "run_id", "")


def telegram_alert_on_failure(chat_id: str | None = None):
    """on_failure_callback that posts to the Telegram channel via the bot API."""

    def _callback(context) -> None:
        ti = context.get("task_instance") or context.get("ti")
        header = f"❌ {_dag_id(context)}"
        if ti is not None and getattr(ti, "task_id", None):
            header += f".{ti.task_id}"
        lines = [f"{header} failed", f"run_id: {_run_id(context)}"]
        if context.get("ds"):
            lines.append(f"logical_date: {context['ds']}")
        log_url = getattr(ti, "log_url", None)
        if log_url:
            lines.append(log_url)
        _send("\n".join(lines), chat_id)

    return _callback


def telegram_alert_on_success(chat_id: str | None = None):
    """on_success_callback that posts to the Telegram channel on a successful run.

    Set this at the DAG level so it fires once per successful run rather than
    once per task.
    """

    def _callback(context) -> None:
        lines = [f"✅ {_dag_id(context)} succeeded", f"run_id: {_run_id(context)}"]
        if context.get("ds"):
            lines.append(f"logical_date: {context['ds']}")
        _send("\n".join(lines), chat_id)

    return _callback
