# RustFS — Architecture

RustFS is a high-performance, S3-compatible distributed object storage system written in Rust. It is Apache 2.0-licensed, has no telemetry, and benchmarks at ~2.3× the throughput of MinIO on small objects.

---

## How It Fits in the Lakehouse

RustFS acts as the **storage layer**: every other service (Spark, Trino, Hive, Airflow) reads and writes data through the S3 API. Think of it as the lakehouse's hard drive — everything else is compute on top of it.

```mermaid
graph TD
    subgraph Compute Layer
        Spark["Apache Spark"]
        Trino["Trino"]
        Airflow["Airflow"]
    end

    subgraph Storage Layer
        RustFS["RustFS<br/>(S3-compatible)"]
    end

    Spark  -->|s3a://| RustFS
    Trino  -->|s3a://| RustFS
    Airflow -->|boto3 / s3fs| RustFS
```

---

## High-Level Architecture

Our deployment runs **4 RustFS nodes** behind an **NGINX load balancer**. NGINX is the single entry point for both traffic types:

- **Port 9000** — S3 API, round-robined across all 4 nodes
- **Port 9001** — Web console, round-robined across all 4 nodes (each node runs its own console process)

This means the console remains reachable even if any individual node goes down — NGINX simply skips the unhealthy backend and picks the next one.

```mermaid
graph TD
    Client(["Client<br/>(SDK / CLI / Browser)"])

    Client -->|"S3 API  :9000"| NGINX["rustfs-nginx<br/>NGINX load balancer"]
    Client -->|"Console :9001"| NGINX

    NGINX -->|":9000 S3"| N1["rustfs1"]
    NGINX -->|":9000 S3"| N2["rustfs2"]
    NGINX -->|":9000 S3"| N3["rustfs3"]
    NGINX -->|":9000 S3"| N4["rustfs4"]

    NGINX -->|":9001 console"| N1
    NGINX -->|":9001 console"| N2
    NGINX -->|":9001 console"| N3
    NGINX -->|":9001 console"| N4

    N1 <-->|"erasure shard sync"| N2
    N1 <-->|"erasure shard sync"| N3
    N1 <-->|"erasure shard sync"| N4
    N2 <-->|"erasure shard sync"| N3
    N2 <-->|"erasure shard sync"| N4
    N3 <-->|"erasure shard sync"| N4
```

---

## Docker Compose Service Map

Each service container is attached to the shared `lakehouse-net` Docker network. Named volumes ensure data persists across restarts.

```mermaid
graph LR
    subgraph ing-lakehouse Docker Compose

    
        nginx["rustfs-nginx<br/>host :9000 S3<br/>host :9001 console"]

        nginx --> n1["rustfs1<br/>:9000 + :9001"]
        nginx --> n2["rustfs2<br/>:9000 + :9001"]
        nginx --> n3["rustfs3<br/>:9000 + :9001"]
        nginx --> n4["rustfs4<br/>:9000 + :9001"]

        n1 --- v1[("rustfs-data-1")]
        n2 --- v2[("rustfs-data-2")]
        n3 --- v3[("rustfs-data-3")]
        n4 --- v4[("rustfs-data-4")]
    end
```

---

## Write Path (PUT object)

When a client uploads an object, the receiving node handles erasure coding and distributes shards to its peers before acknowledging the client.

```mermaid
sequenceDiagram
    participant C as Client
    participant LB as rustfs-nginx
    participant R1 as rustfs1 (coordinator)
    participant R2 as rustfs2
    participant R3 as rustfs3
    participant R4 as rustfs4

    C->>LB: PUT /bucket/object (data)
    LB->>R1: forwarded (round-robin pick)
    R1->>R1: split object into erasure shards
    R1->>R2: data shard A
    R1->>R3: data shard B
    R1->>R4: parity shard
    R1->>R1: store own shard locally
    R1-->>C: HTTP 200 OK
```

---

## Read Path (GET object)

Any node can serve a read. If one node is unavailable, the coordinator reconstructs the object from the surviving shards using the parity data.

```mermaid
sequenceDiagram
    participant C as Client
    participant LB as rustfs-nginx
    participant R1 as rustfs1 (coordinator)
    participant R2 as rustfs2
    participant R3 as rustfs3

    C->>LB: GET /bucket/object
    LB->>R1: forwarded
    R1->>R1: read own shard
    R1->>R2: fetch shard A
    R1->>R3: fetch shard B
    R1->>R1: reconstruct full object
    R1-->>C: HTTP 200 + object body
```

---

## Fault Tolerance

With 4 nodes using RS(2,2) erasure coding:

| Nodes failed | Cluster status |
| --- | --- |
| 0 | Fully operational |
| 1 | Fully operational (degraded) |
| 2 | Fully operational (degraded) |
| 3 | **Cluster unavailable** |
| 4 | **Cluster unavailable** |

The cluster can survive the simultaneous loss of any **2 nodes** while continuing to serve reads and writes.

---

## Component Responsibilities

| Component | Role |
| --- | --- |
| `rustfs-nginx` | Single entry point — load-balances both S3 API (:9000) and web console (:9001) across all nodes; logs each request with the upstream node address via `$upstream_addr` |
| `rustfs1–4` | Data nodes — each runs the S3 API and a console process; NGINX picks any healthy node for either traffic type |
| `lakehouse-net` | Shared Docker network for cross-service communication |
| Named volumes | Persistent data storage, one volume per node |

---

## Configuration Reference

Key environment variables (set in root `.env`):

| Variable | Description |
| --- | --- |
| `RUSTFS_ACCESS_KEY` | S3 root access key |
| `RUSTFS_SECRET_KEY` | S3 root secret key |
| `RUSTFS_VOLUMES` | Space-separated list of all node data endpoints |
| `RUSTFS_ADDRESS` | Bind address for S3 API (`0.0.0.0:9000`) |
| `RUSTFS_CONSOLE_ADDRESS` | Bind address for web console (`0.0.0.0:9001`) |
| `RUSTFS_OBS_LOGGER_LEVEL` | Log level: `debug`, `info`, `warn`, `error` |

---

## Further Reading

- [Erasure Coding in RustFS](erasure-coding.md)
- [RustFS GitHub](https://github.com/rustfs/rustfs)
- [RustFS Documentation](https://docs.rustfs.com)
