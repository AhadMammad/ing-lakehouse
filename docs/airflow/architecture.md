# Apache Airflow 3.2.1 — Architecture

## Overview

Airflow is a platform for authoring, scheduling, and monitoring workflows expressed as directed acyclic graphs (DAGs). Airflow 3.x introduced significant architectural changes that decompose the monolithic 2.x process model into clearly separated, independently scalable components.

---

## Component Topology

```mermaid
graph TB
    subgraph infra["Infrastructure"]
        PG[(PostgreSQL 16\nMetadata DB)]
        RD[(Redis 7\nCelery Broker)]
    end

    subgraph core["Core Components"]
        INIT[airflow-init\none-shot: db migrate\n+ admin user]
        API[airflow-apiserver\nWeb UI + REST API\n:8082]
        SCH[airflow-scheduler\nDAG scheduling]
        DPR[airflow-dag-processor\nDAG file parsing]
        TRG[airflow-triggerer\nDeferrable operators]
    end

    subgraph workers["Celery Workers"]
        W1[airflow-worker-1]
        W2[airflow-worker-2]
    end

    subgraph lakehouse["Lakehouse Services"]
        SPK[spark-master\n:7077]
        TRN[trino\n:8080]
        RFS[rustfs\n:9000\nS3-compatible]
    end

    INIT --> PG
    API --> PG
    SCH --> PG
    SCH --> DPR
    SCH --> RD
    DPR --> PG
    TRG --> PG
    W1 --> RD
    W1 --> PG
    W2 --> RD
    W2 --> PG
    W1 -.->|spark_default| SPK
    W2 -.->|spark_default| SPK
    W1 -.->|trino_default| TRN
    W2 -.->|trino_default| TRN
    W1 -.->|aws_default| RFS
    W2 -.->|aws_default| RFS
```

---

## Component Reference

### airflow-postgres (PostgreSQL 16)

The **metadata database** is the single source of truth for all Airflow state:

- DAG definitions, DAG run records, task instance records
- XCom values (default backend)
- Connection and variable configs (when not using env vars)
- Job heartbeats (scheduler, triggerer, workers)
- Audit logs

Connection string used by all components:
```
postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow
```

### airflow-redis (Redis 7)

Acts exclusively as the **Celery message broker**. The scheduler writes task messages here; workers consume them. Redis does not persist DAG state — that remains in Postgres.

Broker URL: `redis://airflow-redis:6379/0`

The result backend (where task return values are stored) is configured back to Postgres to avoid a separate persistence layer:
```
db+postgresql://airflow:airflow@airflow-postgres/airflow
```

### airflow-init

A one-shot container that runs `airflow db migrate` to apply schema migrations, then creates the admin user. It exits cleanly and all other components gate their startup on `service_completed_successfully`.

**3.x init workaround:** the `airflow users create` CLI is broken in 3.x (`AirflowSecurityManagerV2` is missing `find_role`), so this project drives init through the official image's entrypoint using the `_AIRFLOW_DB_MIGRATE` and `_AIRFLOW_WWW_USER_CREATE` env vars instead. See the comments in [services/airflow/docker-compose.yml](../../services/airflow/docker-compose.yml). If a future image release fixes the bug, the override can be dropped — this paragraph exists so that won't be archaeology.

### airflow-apiserver

Replaces the `webserver` process from Airflow 2.x. In Airflow 3.x this process serves:
- The React-based Web UI
- The REST API (`/api/v2/`)
- Authentication and RBAC enforcement, dispatched through the configured `AuthManager`

It is **stateless from an orchestration perspective** — it reads/writes Postgres but makes no scheduling decisions. The health endpoint is `/api/v2/version`.

Exposed on host port `${AIRFLOW_PORT:-8082}`.

**Auth manager.** Airflow 3.x defaults to `SimpleAuthManager`, which exposes JWT-only auth on `/api/v2/` (every request needs an `Authorization: Bearer <token>`). This project overrides that via `AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager`, which restores the Flask AppBuilder identity stack and lets HTTP basic auth work (`curl -u admin:airflow ...`). All `curl` examples in these docs assume FabAuthManager. See [connections.md](connections.md) for details.

### airflow-scheduler

The heart of Airflow. The scheduler:

1. Reads the DagBag from the metadata DB (populated by the dag-processor)
2. Evaluates which DAG runs should be created based on schedule intervals
3. Determines which task instances are eligible to run (all upstream tasks satisfied)
4. Submits eligible tasks to the executor (writes them to the Celery broker)
5. Monitors running task instances via heartbeat records in Postgres

The scheduler does **not** parse DAG files directly in Airflow 3.x — that is delegated to the dag-processor.

When a worker stops sending heartbeats for longer than the configured zombie threshold, the scheduler flips the worker's in-flight tasks to `up_for_retry` and re-queues them. The current config-key name has shifted across the 3.x line — check it on the running version with `airflow config list --section scheduler | grep -i zombie`.

The scheduler is HA-capable since Airflow 2.0 (concurrent schedulers coordinate through a row lock in the metadata DB), though this project runs a single instance.

Health check endpoint: `:8974/health`

### airflow-dag-processor

**Standalone-required in Airflow 3.x.** Previously the scheduler could parse DAG files inline; now this is a dedicated process. Responsibilities:

- Watches `./dags/` for file changes (polling interval: `[scheduler] min_file_process_interval`, default 30 s — lowering it shortens DAG-edit-to-runnable latency but costs CPU on every cycle)
- Imports each Python file and extracts DAG objects
- Serializes DAGs into the metadata DB (`serialized_dag` table)
- Detects syntax / import errors and records them in the DB
- Each parse is subject to `[core] dagbag_import_timeout` (default 30 s); slow imports get killed

This separation means DAG parse failures cannot destabilize the scheduler, and the dag-processor can be scaled/restarted independently. In production, DAGs are typically distributed via **DAG bundles** (`[dag_processor] dag_bundle_config_list` — git, local folder, s3) instead of the host-mount used here.

DAGs are mounted read-only: `./dags:/opt/airflow/dags:ro`

### airflow-triggerer

Handles **deferrable operators**. When an operator defers (yields a trigger), the task instance enters `deferred` state and the triggerer:

- Runs lightweight async triggers using Python `asyncio`
- Monitors external conditions (file arrival, HTTP polling, Kafka offset, etc.)
- Resumes the task instance when the trigger fires — re-submits it to the executor

A single triggerer process can manage thousands of concurrent triggers efficiently. This makes long-running sensors cost-effective at scale.

Health check: `airflow jobs check --job-type TriggererJob`

### Task Execution API — required cross-container config

Airflow 3.x split task execution out of the metadata DB and into a dedicated REST surface on the apiserver (`/execution/`, AIP-72). Workers fork a subprocess that talks to this endpoint to fetch task details and post state. Two configs are non-negotiable on any multi-container deployment:

- `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` — the URL workers use to reach the apiserver. Default is `http://localhost:8080` which fails inside docker. This project sets it to `http://airflow-apiserver:8080/execution/`.
- `AIRFLOW__API_AUTH__JWT_SECRET` — the symmetric secret used to sign Task Execution API JWTs. If left blank, each container generates its own and scheduler-signed tokens fail signature verification at the apiserver. Task subprocesses then die with `Invalid auth token: Signature verification failed` **before writing any log file**. The 0-byte log is the diagnostic signature.

Both are wired up in [services/airflow/docker-compose.yml](../../services/airflow/docker-compose.yml). If you split components onto separate hosts, propagate both.

### airflow-worker-1 / airflow-worker-2

Celery workers that actually **execute tasks**. Each worker:

- Subscribes to the Celery task queue in Redis
- Picks up a task message, deserializes it, runs the operator's `execute()` method
- Writes task state (running → success/failed) back to Postgres
- Pushes XCom return values to Postgres

Workers communicate with lakehouse services directly — Spark, Trino, and RustFS — using the pre-wired connections.

```
DUMB_INIT_SETSID: "0"
```
This flag is required so that Celery's warm-shutdown signal propagates correctly through dumb-init to the worker process group.

---

## Airflow 3.x vs 2.x: Breaking Changes

| Concern | Airflow 2.x | Airflow 3.x |
|---|---|---|
| Web UI + REST API | `webserver` process | `api-server` process |
| DAG file parsing | Optional standalone or inline | Standalone `dag-processor` required |
| REST API version | `/api/v1/` | `/api/v2/` |
| Default auth | FAB / basic | `SimpleAuthManager` / JWT (this project overrides back to FAB) |
| Task execution | Workers hit metadata DB | Task SDK + Task Execution API; workers talk to apiserver (AIP-72) |
| SequentialExecutor | Available (dev only) | Removed from default installs |
| DAG versioning | Each parse overwrites | AIP-66 explicit versioning; historical runs reference their version |
| `schedule_interval` param | Supported | **Removed**; use `schedule` |
| `execution_date` context key | Available (deprecated 2.2+) | **Removed**; use `logical_date` |
| `Dataset` model | Introduced 2.4 | Renamed to `Asset` (AIP-74/75); `Dataset` alias not present in `airflow.sdk` |
| XCom serialization | JSON or pickle (toggle) | JSON only; pickle removed |
| `SubDagOperator` | Available | Removed; use `TaskGroup` |
| `S3KeySensorAsync` etc | Standalone async classes | Removed; use `deferrable=True` on the regular sensor |
| Connections UI | Classic Flask | React-based UI |

---

## Critical Data Flows

### DAG Parse Path

```mermaid
sequenceDiagram
    participant FS as DAG File (./dags/)
    participant DP as dag-processor
    participant PG as Postgres (serialized_dag)
    participant SCH as scheduler

    DP->>FS: polls for new/changed .py files
    DP->>DP: imports module, extracts DAG objects
    DP->>PG: writes serialized DAG JSON
    SCH->>PG: reads serialized DAGs
    SCH->>SCH: evaluates schedule, creates DagRun records
```

### Task Submission Path (CeleryExecutor)

```mermaid
sequenceDiagram
    participant SCH as scheduler
    participant PG as Postgres (task_instance)
    participant RD as Redis (broker)
    participant W as celery worker

    SCH->>PG: sets task state = queued
    SCH->>RD: publishes task message
    W->>RD: consumes task message
    W->>PG: sets task state = running
    W->>W: executes operator.execute()
    W->>PG: sets task state = success/failed
    W->>PG: pushes XCom (if any)
```

### Deferrable Task Path

```mermaid
sequenceDiagram
    participant W as celery worker
    participant PG as Postgres
    participant TRG as triggerer
    participant EXT as External System

    W->>W: operator calls self.defer(trigger=..., method_name=...)
    W->>PG: sets task state = deferred, stores trigger classpath
    TRG->>PG: reads deferred trigger records
    TRG->>EXT: polls/awaits condition (asyncio)
    EXT-->>TRG: condition met
    TRG->>PG: sets task state = scheduled (resumes at method_name)
    Note over PG,W: Scheduler picks it back up, re-queues to worker
```

---

## This Project's Configuration

From `services/airflow/docker-compose.yml`:

| Setting | Value |
|---|---|
| Image | Derived from `apache/airflow:3.2.1` (see [services/airflow/Dockerfile](../../services/airflow/Dockerfile) — adds `apache-spark` + `trino` providers) |
| Executor | `CeleryExecutor` |
| Workers | 2 (`airflow-worker-1`, `airflow-worker-2`), explicit `WORKER_CONCURRENCY=4` |
| Auth manager | `FabAuthManager` (basic auth on REST; overrides the default `SimpleAuthManager` JWT-only) |
| Metadata DB | PostgreSQL 16 (`airflow-postgres`) |
| Celery Broker | Redis 7 (`airflow-redis:6379/0`) |
| Result Backend | `db+postgresql` (same Postgres) |
| DAGs path (host) | `services/airflow/dags/` (includes `lakehouse_smoke.py` reference DAG) |
| DAGs path (container) | `/opt/airflow/dags` (read-only mount) |
| Host port | `${AIRFLOW_PORT:-8082}` |
| Encryption | Fernet key via `${AIRFLOW_FERNET_KEY}` (blank in dev `.env`) |
| Examples | Disabled (`LOAD_EXAMPLES=false`) |
| DAGs paused on creation | Yes (`DAGS_ARE_PAUSED_AT_CREATION=true`) |
| Network | `lakehouse-net` (shared with all services) |
