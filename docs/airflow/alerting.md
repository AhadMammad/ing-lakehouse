# Alerting

DAGs send failure / success alerts over two channels that fire side by side:
**email** (captured by Mailpit) and **Telegram** (posted to a channel via a bot).
Both are opt-in per DAG and driven by callbacks in `dags/`. The four scheduled ETL
DAGs — `crypto_etl`, `weather_etl`, `payments_etl`, `rideon_etl` — are wired with
both. The [Telegram section](#telegram) is at the bottom.

## Email

DAGs send failure / success alerts by email. Mail is captured by **Mailpit**, a
lightweight open-source SMTP sink that ships with the stack — it does **not**
deliver to real external mailboxes, it shows everything in a web inbox. Perfect
for dev: no DNS, no TLS, no real credentials.

## Topology

```
DAG callback ──▶ SmtpNotifier ──▶ smtp_default conn ──▶ mailpit:1025 ──▶ Mailpit web inbox
                 (dags/_notifiers.py)                                    http://localhost:8025
```

- **Mailpit** runs as the `mailpit` service ([services/mailserver/docker-compose.yml](../../services/mailserver/docker-compose.yml)).
  SMTP on `:1025`, web inbox on `:8025` (host port auto-allocated per instance by
  `make init-instance`).
- **`smtp_default`** connection is pre-wired via `AIRFLOW_CONN_SMTP_DEFAULT` in
  [services/airflow/docker-compose.yml](../../services/airflow/docker-compose.yml),
  the same way `spark_default` / `trino_default` / `aws_default` are. TLS/SSL are
  disabled since Mailpit speaks plain SMTP in-network.
- The **smtp provider** (`apache-airflow-providers-smtp`) is installed in
  [services/airflow/Dockerfile](../../services/airflow/Dockerfile); it supplies
  `SmtpNotifier`.

## The notifier helper

[services/airflow/dags/_notifiers.py](../../services/airflow/dags/_notifiers.py)
exposes two callbacks. Recipients and the From address come from the worker env
(`ALERT_EMAIL_ADMIN`, `ALERT_EMAIL_DATATEAM`, `AIRFLOW_SMTP_FROM`), set in the
Airflow compose file and sourced from `.env`:

- `alert_on_failure()` → email admin + datateam when a task fails.
- `alert_on_success()` → email admin + datateam when a DAG run succeeds.

## Opting a DAG in

Alerting is **opt-in per DAG**. Import the helper and pass the callbacks to the
`DAG` — set `on_success_callback` at the DAG level so it fires once per run, not
once per task:

```python
from _notifiers import alert_on_failure, alert_on_success

with DAG(
    dag_id="my_etl",
    ...
    on_failure_callback=alert_on_failure(),
    on_success_callback=alert_on_success(),
) as dag:
    ...
```

The four ETL DAGs (`crypto_etl`, `weather_etl`, `payments_etl`, `rideon_etl`) are
wired as the reference examples — each passes a **list** of callbacks so the email
and Telegram notifiers both fire. To alert only a specific task, pass the same
callback to that operator instead of the DAG.

To override recipients for one DAG: `alert_on_failure(to=["oncall@lakehouse.local"])`.

## Viewing alerts

Open the Mailpit inbox (printed by `make up-batch` / `make console`):
`http://localhost:8025`. Every alert lands there addressed to admin@ + datateam@.

## Switching to real delivery

Mailpit only captures mail. To deliver to real inboxes, repoint the
`smtp_default` connection at a real relay (SendGrid / SES / Gmail app password)
in `.env` / the Airflow compose file — **no DAG changes needed**, since alerting
flows through `smtp_default`.

## Telegram

Alongside email, the ETL DAGs post failure / success alerts to a **Telegram
channel** via a bot. Unlike email there is no in-network sink — messages go
straight to Telegram's Bot API and land in your real channel.

### Telegram topology

```text
DAG callback ──▶ TelegramHook ──▶ Telegram Bot API ──▶ your channel / group
                 (dags/_telegram_notifiers.py)
```

- The **telegram provider** (`apache-airflow-providers-telegram`) is installed in
  [services/airflow/Dockerfile](../../services/airflow/Dockerfile); it supplies
  `TelegramHook`. (The version pinned for Airflow 3.2.1 ships no `Notifier`
  class, so the helper posts through the hook inside a plain callback.)
- Credentials come from the worker env: `TELEGRAM_BOT_TOKEN` and
  `TELEGRAM_CHAT_ID`, set in
  [services/airflow/docker-compose.yml](../../services/airflow/docker-compose.yml)
  and sourced from `.env.<instance>`. They are passed **straight to the notifier**
  — no `AIRFLOW_CONN_*` connection, because the bot token's `:` breaks the
  URI-connection form (this mirrors how `_notifiers.py` reads `ALERT_EMAIL_*`).

### Getting the bot credentials

1. Message **@BotFather** on Telegram → `/newbot` → copy the **bot token**.
2. Add the bot to the target channel/group (as admin for a channel), then get the
   **chat id** (e.g. via `@userinfobot`, or the `getUpdates` API). Channel ids look
   like `-100…`.
3. Fill both into `.env.<username>` after `make init-instance`:

   ```bash
   TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
   TELEGRAM_CHAT_ID=-1001234567890
   ```

   Left blank, the Telegram callback simply no-ops (the send errors and Airflow
   catches it) — the email alert and the task's own state are unaffected.

### The Telegram notifier helper

[services/airflow/dags/_telegram_notifiers.py](../../services/airflow/dags/_telegram_notifiers.py)
mirrors `_notifiers.py` and exposes two callbacks:

- `telegram_alert_on_failure()` → ❌ message when a task fails.
- `telegram_alert_on_success()` → ✅ message when a DAG run succeeds.

### Opting a DAG into Telegram

Import both helpers and pass a **list** of callbacks so email and Telegram fire
together:

```python
from _notifiers import alert_on_failure, alert_on_success
from _telegram_notifiers import telegram_alert_on_failure, telegram_alert_on_success

with DAG(
    dag_id="my_etl",
    ...
    on_failure_callback=[alert_on_failure(), telegram_alert_on_failure()],
    on_success_callback=[alert_on_success(), telegram_alert_on_success()],
) as dag:
    ...
```

To send to a different channel for one DAG:
`telegram_alert_on_failure(chat_id="-100...")`.
