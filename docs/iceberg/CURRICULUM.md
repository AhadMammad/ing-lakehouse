# Iceberg Notebook Curriculum

Eleven notebooks under [`notebooks/`](../../notebooks/) walk through every Iceberg feature available in this stack — table writes, evolution, mutations, branching, and Spark-driven maintenance.

## Quick start

```bash
make up                   # start the stack
make nessie-init-bucket   # one-time bucket create
make jupyter-rebuild      # only needed after Dockerfile changes
```

Open `http://localhost:8888?token=lakehouse` and run the notebooks **in order**. Use **Kernel → Restart Kernel and Run All Cells** for each.

To replay from scratch: `make reset-events-table` (or `make reset-nessie` for a full catalog wipe).

## Notebooks

| # | Notebook | Focus |
|---|---|---|
| 00 | [`00_setup_catalog.ipynb`](../../notebooks/00_setup_catalog.ipynb) | Verify S3 + catalog reachable; create `demo` namespace |
| 01 | [`01_write_iceberg_polars.ipynb`](../../notebooks/01_write_iceberg_polars.ipynb) | First write — Polars → PyArrow → Iceberg `append`, partitioned by `days(ts)` |
| 02 | [`02_read_iceberg_polars.ipynb`](../../notebooks/02_read_iceberg_polars.ipynb) | Reads, predicate pushdown, column projection, `overwrite` |
| 03 | [`03_schema_evolution.ipynb`](../../notebooks/03_schema_evolution.ipynb) | `add_column`, `rename_column`, type promotion, reorder, `delete_column` |
| 04 | [`04_partition_evolution.ipynb`](../../notebooks/04_partition_evolution.ipynb) | Evolve `days(ts)` → `hours(ts)`; read across both specs |
| 05 | [`05_row_mutations.ipynb`](../../notebooks/05_row_mutations.ipynb) | `upsert`, `delete`, `overwrite` patterns |
| 06 | [`06_metadata_inspection.ipynb`](../../notebooks/06_metadata_inspection.ipynb) | `table.inspect.{snapshots, files, manifests, history, partitions, refs}` |
| 07 | [`07_iceberg_branches_tags.ipynb`](../../notebooks/07_iceberg_branches_tags.ipynb) | Demonstrates that Iceberg-native table-level refs are silent no-ops on Nessie — `create_tag`/`create_branch` return cleanly but aren't persisted |
| 08 | [`08_nessie_catalog_refs.ipynb`](../../notebooks/08_nessie_catalog_refs.ipynb) | Catalog-level Git-style versioning — Nessie commits, branches, tags, diff, past-hash reads via `{NESSIE_URI}/main@{hash}` |
| 09 | [`09_snapshot_management.ipynb`](../../notebooks/09_snapshot_management.ipynb) | Rollback via Nessie branch reassignment (`PUT /trees/main@{hash}`); explains why Iceberg-level `rollback`/`expire_snapshots` don't apply here |
| 10 | [`10_spark_maintenance.ipynb`](../../notebooks/10_spark_maintenance.ipynb) | Spark stored procedures: `rewrite_data_files`, `rewrite_manifests`, `remove_orphan_files` (dry run) — with Nessie-GC caveats |

## Dependency DAG

```
00 ── 01 ─┬─ 02
          ├─ 03 ── 04
          ├─ 05 ── 06
          ├─ 07
          ├─ 08
          ├─ 09
          └─ 10
```

- **00 → 01** is the only hard prerequisite chain. Always run 00 once, 01 before anything else.
- **03 → 04** is sequential — 04 writes data under the column name `event_kind` that 03 introduces via `rename_column`. Running 04 before 03 will fail.
- **02, 05, 07, 08, 09** each only require 01. They can run in any order.
  - 05 detects whether 03 has run (column is `event_kind` vs `event_type`) and adapts.
  - 06 works after any of them; richer history if 03/04/05 have run.
- **10** runs Spark maintenance. It demonstrates the procedures' APIs but the **counts will be near-zero** on this demo:
  - `rewrite_data_files` returns 0 because the table has only ~10 tiny files spread across ~9 partitions — most partitions don't reach the configured `min-input-files=2`.
  - `remove_orphan_files` (dry run) returns 0 because PyIceberg 0.9.1 doesn't expose `expire_snapshots`, so no snapshots have been orphaned at the Iceberg layer. Files referenced only by old Nessie commits also don't show as orphans — they're still reachable from a Nessie ref.

## Two ref systems — only one actually works here

Iceberg's spec defines **table-level refs** stored inside `metadata.json`. Nessie adds a separate **catalog-level** ref system that versions the whole catalog like Git. On a Nessie-backed catalog, the two interact badly:

| Layer | Stored in | API | Status on Nessie |
|---|---|---|---|
| Iceberg table refs | `metadata.json` | `table.manage_snapshots().create_tag/branch` | **Silent no-op** — see notebook 07 |
| Nessie catalog refs | Nessie commit graph | Nessie REST API v2 | Works — see notebook 08 |

Why: Nessie synthesizes a fresh `metadata.json` per commit containing only the current snapshot and `main`. PyIceberg's `AddSnapshotRefUpdate` doesn't survive the round-trip, so the tag/branch you created via `manage_snapshots` is never persisted.

**On this stack, use Nessie refs.** Notebook 07 exists to make that no-op explicit so you don't waste time debugging it. The same Iceberg-native API would work on a Hive Metastore or Glue catalog, where the server persists whatever metadata PyIceberg writes.

## Why time travel by `snapshot_id` is not in 02

Nessie's table `metadata.json` retains only the current snapshot. `table.scan(snapshot_id=...)` for past snapshots reliably fails on this stack. Catalog-ref-based time travel via Nessie commit hashes (notebook 08) is the correct pattern here.
