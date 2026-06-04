# Email Alerting

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

`crypto_etl.py` and `weather_etl.py` are wired as the reference examples. To
alert only a specific task, pass the same callback to that operator instead of
the DAG.

To override recipients for one DAG: `alert_on_failure(to=["oncall@lakehouse.local"])`.

## Viewing alerts

Open the Mailpit inbox (printed by `make up-batch` / `make console`):
`http://localhost:8025`. Every alert lands there addressed to admin@ + datateam@.

## Switching to real delivery

Mailpit only captures mail. To deliver to real inboxes, repoint the
`smtp_default` connection at a real relay (SendGrid / SES / Gmail app password)
in `.env` / the Airflow compose file — **no DAG changes needed**, since alerting
flows through `smtp_default`.
