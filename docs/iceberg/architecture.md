# Apache Iceberg — Architecture

## What Is Iceberg?

Apache Iceberg is an open **table format** for large analytic datasets. It is not a storage engine and not a query engine — it is a specification that sits between your object store (RustFS / S3) and your query tools (Spark, Trino, PyIceberg, DuckDB).

Iceberg solves problems that Hive-partitioned tables cannot:

| Problem | Hive tables | Iceberg |
|---|---|---|
| Concurrent writes | No isolation | Full ACID via optimistic concurrency |
| Schema changes | Break existing queries | Safe evolution (add, rename, reorder, widen) |
| Partition changes | Requires full rewrite | Partition evolution — old and new files coexist |
| Time travel | Not possible | Every snapshot is retained and queryable |
| Large directories | `LIST` calls are slow | Manifest files skip entire partitions |

---

## Three-Layer Architecture

Every Iceberg table is made of three distinct layers. The catalog only holds a single pointer — all structure lives in files on object storage.

```mermaid
graph TD
    subgraph Catalog["Catalog (Nessie / Hive / REST)"]
        PTR["table pointer<br/>latest metadata file path"]
    end

    subgraph Metadata["Metadata Layer (S3 — JSON / Avro)"]
        TM["table-metadata.json<br/>schema · partition spec · snapshots list"]
        ML["manifest-list.avro<br/>one entry per manifest"]
        MF1["manifest-file-1.avro<br/>data file paths + partition stats"]
        MF2["manifest-file-2.avro<br/>data file paths + partition stats"]
    end

    subgraph Data["Data Layer (S3 — Parquet)"]
        P1["part-00001.parquet"]
        P2["part-00002.parquet"]
        P3["part-00003.parquet"]
    end

    PTR --> TM
    TM  --> ML
    ML  --> MF1
    ML  --> MF2
    MF1 --> P1
    MF1 --> P2
    MF2 --> P3
```

### Layer responsibilities

| Layer | Files | Purpose |
|---|---|---|
| **Catalog** | In-memory pointer | Maps `namespace.table` → current metadata file path |
| **Table metadata** | `metadata/*.json` | Schema, partition spec, list of all snapshots |
| **Manifest list** | `metadata/*.avro` | One row per manifest; created per snapshot |
| **Manifest file** | `metadata/*.avro` | One row per data file; stores min/max stats for partition pruning |
| **Data files** | `data/*.parquet` | Actual rows; immutable once written |

---

## Snapshot Model

Every write (append, overwrite, delete) creates a new **snapshot**. Previous snapshots are never mutated — this is what enables ACID, time travel, and concurrent reads without locks.

```mermaid
gitGraph
   commit id: "snapshot 1 — create table"
   commit id: "snapshot 2 — append 5 rows"
   commit id: "snapshot 3 — append 2 rows"
   commit id: "snapshot 4 — overwrite purchase rows"
```

```mermaid
graph LR
    subgraph "table-metadata.json"
        CURR["current-snapshot-id: S3"]
        S1["snapshot S1<br/>timestamp: T1"]
        S2["snapshot S2<br/>timestamp: T2<br/>parent: S1"]
        S3["snapshot S3<br/>timestamp: T3<br/>parent: S2"]
    end

    S1 -->|manifest-list| ML1["manifest-list-S1.avro"]
    S2 -->|manifest-list| ML2["manifest-list-S2.avro"]
    S3 -->|manifest-list| ML3["manifest-list-S3.avro"]

    ML2 -->|reuses| ML1
    ML3 -->|reuses| ML2

    CURR --> S3
```

Manifests are **reused across snapshots** — only the delta (new files) gets a new manifest. This makes snapshot creation O(new files), not O(total files).

---

## Write Flow

```mermaid
sequenceDiagram
    participant App as Application<br/>(PyIceberg / Spark)
    participant Cat as Catalog<br/>(Nessie)
    participant S3 as Object Store<br/>(RustFS)

    App->>S3: Write Parquet data files
    App->>S3: Write manifest file (lists new data files)
    App->>S3: Write manifest list (references manifest)
    App->>S3: Write new table-metadata.json (new snapshot)
    App->>Cat: Atomic swap — update table pointer to new metadata path
    Cat-->>App: Commit confirmed (or conflict → retry)
```

The catalog's **atomic swap** is the only coordination point. Writers never lock data files — they race only on the metadata pointer. If two writers commit concurrently, the second one detects the conflict and retries from the new base snapshot.

---

## Read Flow — Predicate Pushdown

Iceberg skips data files before opening them using statistics stored in manifest files.

```mermaid
flowchart TD
    Q["Query: event_type = 'click'"]
    Q --> CAT["1. Catalog: resolve table → metadata file"]
    CAT --> SNAP["2. Load current snapshot → manifest list"]
    SNAP --> MAN["3. Scan manifests<br/>check column-level min/max stats"]
    MAN -->|stats exclude this file| SKIP["skip file<br/>(zero I/O)"]
    MAN -->|stats may match| OPEN["4. Open Parquet file<br/>apply row-group filter"]
    OPEN --> ROWS["5. Return matching rows"]
```

Two levels of pruning:
1. **Manifest-level**: skip entire manifests if partition range excludes the predicate.
2. **Row-group-level**: Parquet column statistics skip row groups within a file.

---

## Schema Evolution

Iceberg tracks fields by **ID**, not by name or position. Renaming or reordering a column never breaks readers that were compiled against the old schema.

```mermaid
graph LR
    subgraph "v1 schema"
        F1["id=1  event_id   string"]
        F2["id=2  user_id    long"]
        F3["id=3  event_type string"]
    end

    subgraph "v2 schema (evolved)"
        F1E["id=1  event_id   string"]
        F2E["id=2  user_id    long"]
        F3E["id=3  event_type string"]
        F4E["id=4  country    string  ← added"]
    end

    F1 --> F1E
    F2 --> F2E
    F3 --> F3E
```

Old Parquet files (written without `country`) are read as `null` for that field — no migration needed.

---

## Partition Evolution

In Hive, changing the partition scheme means rewriting every file. In Iceberg, old and new partition schemes coexist — each manifest records which scheme its files use.

```mermaid
gantt
    title Partition scheme timeline
    dateFormat YYYY-MM-DD
    section Files written with month partitioning
    Jan files   :a1, 2024-01-01, 31d
    Feb files   :a2, 2024-02-01, 29d
    section Partition changed to day (no rewrite needed)
    Mar files   :a3, 2024-03-01, 31d
    Apr files   :a4, 2024-04-01, 30d
```

---

## In This Lakehouse

```mermaid
graph LR
    NB["Jupyter Notebook<br/>Polars + PyIceberg"]
    NB -->|"RestCatalog<br/>http://nessie:19120/iceberg"| NES["Nessie<br/>(Iceberg REST Catalog)"]
    NES -->|"create/commit table metadata"| RFS["RustFS<br/>s3://iceberg-warehouse"]
    NB  -->|"S3FileIO<br/>http://rustfs:9000"| RFS
```

- **PyIceberg** handles the Iceberg protocol client-side.
- **Nessie** is the catalog — tracks the table pointer and enforces atomic commits.
- **RustFS** stores all files (Parquet data files + JSON/Avro metadata files) in the `iceberg-warehouse` bucket.
