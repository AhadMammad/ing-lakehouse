# Project Nessie — Architecture

## What Is Nessie?

Project Nessie is a **versioned catalog** for data lakes. It implements the Apache Iceberg REST Catalog specification and adds a Git-like branching model on top — every table operation (create, drop, append, overwrite) is a commit on a branch.

Nessie is not a storage engine. It stores only metadata pointers (which metadata file a table currently points to). All actual data and Iceberg metadata files live in object storage (RustFS).

### Why Nessie instead of Hive Metastore?

| Concern | Hive Metastore | Nessie |
|---|---|---|
| External database | Requires PostgreSQL / MySQL | Embedded RocksDB — zero sidecar |
| Container startup | ~60s (Thrift server + DB migrations) | ~10s |
| Iceberg REST Catalog spec | Third-party adapters needed | First-class, built-in |
| Data branching | Not supported | Native (Git-style) |
| Spark / Trino / PyIceberg support | Yes | Yes |
| Time travel across tables | Not supported | Cross-table consistent snapshots |

---

## High-Level Architecture

```mermaid
graph TD
    subgraph Clients
        PY["PyIceberg<br/>(Jupyter notebooks)"]
        SP["Spark<br/>(planned)"]
        TR["Trino<br/>(planned)"]
    end

    subgraph Nessie["Nessie Container (ing-lakehouse-nessie)"]
        REST["Iceberg REST Catalog API<br/>:19120/iceberg"]
        NAPI["Nessie Versioning API<br/>:19120/api/v2"]
        QRK["Quarkus runtime"]
        RDB["RocksDB<br/>/nessie/data<br/>(version store)"]
        S3C["Internal S3 client<br/>(validates table locations)"]
    end

    subgraph Storage["RustFS (S3-compatible)"]
        BKT["s3://iceberg-warehouse"]
        META["metadata/<br/>table-metadata.json<br/>manifest-list.avro<br/>manifest-file.avro"]
        DATA["data/<br/>part-*.parquet"]
    end

    PY  -->|"HTTP REST"| REST
    SP  -->|"HTTP REST"| REST
    TR  -->|"HTTP REST"| REST
    REST --> QRK
    NAPI --> QRK
    QRK --> RDB
    QRK --> S3C
    S3C -->|"head-object / validate"| BKT
    PY  -->|"S3FileIO (direct)"| DATA
    SP  -->|"S3FileIO (direct)"| DATA
    BKT --> META
    BKT --> DATA
```

> Clients talk to Nessie only for **catalog operations** (create table, commit, load table metadata path). All actual file I/O (reading/writing Parquet) goes directly to RustFS — Nessie is never in the data path.

---

## Internal Components

### Quarkus Runtime

Nessie is built on [Quarkus](https://quarkus.io/) (the "Supersonic Subatomic Java" framework). Configuration is read from `/deployments/config/application.properties` at startup — this is the standard Quarkus SmallRye Config location.

```mermaid
graph LR
    subgraph Config sources (priority high → low)
        ENV["Environment variables<br/>(QUARKUS_HTTP_PORT etc.)"]
        FILE["application.properties<br/>(/deployments/config/)"]
        JAR["Built-in defaults<br/>(inside nessie-quarkus.jar)"]
    end
    ENV --> FILE --> JAR
```

### Version Store (RocksDB)

The version store is Nessie's database. It stores:
- Branch / tag refs and their HEAD commit hashes
- The full commit history (each commit = a content change on one or more tables)
- Per-table content IDs and their current metadata file paths

RocksDB is embedded — no network connection, no separate process. Data persists in the Docker volume at `/nessie/data`.

### Iceberg REST Catalog Endpoint

Nessie exposes the [Iceberg REST Catalog specification](https://iceberg.apache.org/docs/latest/rest-catalog/) at `/iceberg`. PyIceberg's `RestCatalog` speaks this protocol natively.

---

## Nessie Commit Model

Every table change is a **Nessie commit** on a branch. This is the key difference from a plain Iceberg REST catalog:

```mermaid
gitGraph
   commit id: "init main"
   commit id: "create demo.events"
   commit id: "append 5 rows"
   branch feature/new-schema
   commit id: "add 'country' column"
   commit id: "backfill country data"
   checkout main
   commit id: "append 2 more rows"
   merge feature/new-schema id: "merge schema change"
   commit id: "overwrite purchase rows"
```

- `main` is the default branch.
- You can create a feature branch, make table changes on it (schema evolution, data writes), and merge to `main` atomically — all without affecting readers on `main`.
- Tags are immutable snapshots of branch state — useful for marking a release or a reporting cutoff.

---

## Credential Configuration

Nessie's internal S3 client (used for validating table locations and writing catalog-side metadata) uses **Quarkus-native URN secrets**, not standard `AWS_*` environment variables.

```mermaid
flowchart TD
    PROP["application.properties"]
    PROP --> URN["nessie.catalog.service.s3.default-options.access-key<br/>= urn:nessie-secret:quarkus:rustfs-creds"]
    URN --> NAME["rustfs-creds.name = rustfsadmin"]
    URN --> SEC["rustfs-creds.secret = rustfsadmin123"]
```

The `urn:nessie-secret:quarkus:<prefix>` syntax tells Nessie's secret resolver to look for `<prefix>.name` and `<prefix>.secret` keys in the same config file. This is the format documented in Nessie's own built-in `application.properties` (inside the JAR).

Standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars are **not** picked up by Nessie's catalog S3 client.

---

## Request Flow — Table Creation

```mermaid
sequenceDiagram
    participant Client as PyIceberg Client
    participant NR as Nessie REST (:19120/iceberg)
    participant RDB as RocksDB
    participant RFS as RustFS S3

    Client->>NR: POST /iceberg/v1/namespaces/demo/tables<br/>{schema, partition-spec, location}
    NR->>RFS: HEAD s3://iceberg-warehouse/warehouse/demo/events/<br/>(validate location accessible)
    RFS-->>NR: 200 OK
    NR->>RDB: commit — insert content-id → metadata path
    NR-->>Client: {metadata-location, table-uuid, ...}
    Client->>RFS: Write Parquet data files
    Client->>RFS: Write manifest + metadata JSON
    Client->>NR: POST /iceberg/v1/namespaces/demo/tables/events<br/>(commitTableTransaction — update metadata pointer)
    NR->>RDB: commit — update content-id → new metadata path
    NR-->>Client: 200 OK
```

---

## Request Flow — Table Append (from Notebook 01)

```mermaid
sequenceDiagram
    participant PY as PyIceberg (Polars → Arrow → Iceberg)
    participant NES as Nessie
    participant RFS as RustFS

    PY->>NES: load_table("demo.events") → get current metadata path
    NES-->>PY: s3://iceberg-warehouse/warehouse/demo/events/metadata/v1.metadata.json
    PY->>RFS: write part-00000.parquet (the Polars data)
    PY->>RFS: write manifest-file.avro  (lists the parquet file)
    PY->>RFS: write manifest-list.avro  (lists the manifest)
    PY->>RFS: write v2.metadata.json    (new snapshot pointing to manifest list)
    PY->>NES: commitTableTransaction — swap pointer to v2.metadata.json
    NES-->>PY: commit OK, new snapshot ID
```

---

## Ports and Endpoints

| Port | Interface | Key paths |
|---|---|---|
| `19120` | Main HTTP (Quarkus) | `/iceberg` — Iceberg REST Catalog · `/api/v2` — Nessie versioning API |
| `9001` | Management (Quarkus) | `/q/health/live` · `/q/health/ready` · `/q/metrics` |

The management port is separate so that health probes never compete with catalog traffic.

---

## In This Lakehouse

```mermaid
graph LR
    subgraph "docker-compose.local.yml"
        JUP["ing-lakehouse-jupyter<br/>:8888"]
        NES["ing-lakehouse-nessie<br/>:19120"]
        RFS["ing-lakehouse-rustfs-{1..4}<br/>:9000 (internal)"]
        NGX["ing-lakehouse-rustfs-nginx<br/>:9000 / :9001 (host)"]
    end

    JUP -->|"catalog ops (REST)"| NES
    JUP -->|"file I/O (S3, direct)"| RFS
    NES -->|"location validation (S3)"| RFS
    RFS --- NGX
```

> Jupyter connects to `http://rustfs:9000` (direct container, no TLS) rather than `http://ing-lakehouse-rustfs-nginx:9000`. The nginx proxy uses a self-signed certificate for external HTTPS — bypassing it for intra-cluster traffic avoids SSL trust issues.

---

## First-Run Checklist

```bash
make up                  # starts nessie + jupyter (jupyter waits for nessie healthy)
make nessie-init-bucket  # creates iceberg-warehouse bucket in RustFS (one-time)
make health              # verify ing-lakehouse-nessie shows (healthy)
# open http://localhost:8888?token=lakehouse
# run notebooks in order: 00 → 01 → 02
```
