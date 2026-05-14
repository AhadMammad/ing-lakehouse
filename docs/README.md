# ing-lakehouse

A modular, service-per-directory data lakehouse platform built for data engineering workflows. Each component is independently deployable and shares a single Docker network (`lakehouse-net`) for seamless cross-service communication.

---

## Service Catalog

| Service | Status | Description | Docs |
| --- | --- | --- | --- |
| **RustFS** | ✅ Active | S3-compatible distributed object storage (4-node) | [architecture](rustfs/architecture.md) · [erasure coding](rustfs/erasure-coding.md) |
| **Nessie** | ✅ Active | Iceberg REST Catalog with Git-like data versioning | [architecture](nessie/architecture.md) |
| **Jupyter** | ✅ Active | JupyterLab notebook environment (PyIceberg + Polars) | — |
| **Apache Iceberg** | ✅ Active | Open table format — ACID, time travel, schema evolution | [architecture](iceberg/architecture.md) |
| Spark | 🔜 Planned | Distributed query engine | — |
| Trino | 🔜 Planned | Interactive SQL on the lakehouse | — |
| Kafka | 🔜 Planned | Streaming ingest layer | — |
| Airflow | 🔜 Planned | Workflow orchestration | — |

---

## Quick Start

```bash
# 1. Clone and enter the repo
cd ing-lakehouse

# 2. Start everything
make up

# 3. Check status
make status

# 4. Open the RustFS web console
make console          # prints URL + credentials
# → http://localhost:9001

# 5. Tail logs
make logs

# 6. Shut down
make down
```

---

## Port Reference

| Host Port | Service | Protocol |
| --- | --- | --- |
| `9000` | RustFS S3 API — nginx LB, round-robins across all 4 nodes | HTTP / S3 |
| `9001` | RustFS Web Console — nginx LB, round-robins across all 4 nodes | HTTP |
| `19120` | Nessie Iceberg REST Catalog + Versioning API | HTTP |
| `8888` | JupyterLab | HTTP |

RustFS ports are served by `rustfs-nginx`. No node is a single point of failure for either the data plane or the UI.

---

## Environment Variables

Defined in the root [`.env`](../.env). Override locally by editing that file.

| Variable | Default | Description |
| --- | --- | --- |
| `COMPOSE_PROJECT_NAME` | `ing-lakehouse` | Docker project prefix |
| `RUSTFS_IMAGE` | `rustfs/rustfs:latest-glibc` | RustFS image tag |
| `RUSTFS_ACCESS_KEY` | `rustfsadmin` | S3 access key |
| `RUSTFS_SECRET_KEY` | `rustfsadmin123` | S3 secret key |
| `RUSTFS_S3_PORT` | `9000` | Host port for S3 API |
| `RUSTFS_CONSOLE_PORT` | `9001` | Host port for web console |
| `RUSTFS_LOG_LEVEL` | `info` | Logging verbosity |

> **Security note:** Change `RUSTFS_ACCESS_KEY` and `RUSTFS_SECRET_KEY` before exposing any port outside localhost.

---

## Adding a New Service

1. Create `services/<name>/docker-compose.yml` with your service definition.
2. Reference the shared network in each service (do **not** redefine it — it is owned by the root compose file):

   ```yaml
   networks:
     - lakehouse-net
   ```

3. Add one line to the root `docker-compose.yml`:

   ```yaml
   include:
     - services/<name>/docker-compose.yml
   ```

4. Add Makefile targets and a `docs/<name>/` directory following the same pattern.

---

## Makefile Reference

Run `make help` to see the full colored target list. Key targets:

| Target | Description |
| --- | --- |
| `make up` | Start all services |
| `make down` | Stop all services |
| `make health` | Check health of all RustFS nodes |
| `make logs-node NODE=rustfs2` | Tail a specific node's logs |
| `make console` | Print the console URL and credentials |
| `make clean` | **Destructive** — stops services and wipes all volumes |
