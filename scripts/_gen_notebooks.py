"""One-shot generator for the lakehouse notebook curriculum.

Produces notebooks/00..10 with consistent structure, no stale outputs,
and the curriculum DAG documented in the plan file.

Run once from the repo root:
    python3 scripts/_gen_notebooks.py

This file is not part of the runtime — delete after generation if desired.
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NB_DIR = ROOT / "notebooks"
NB_DIR.mkdir(parents=True, exist_ok=True)


def cid() -> str:
    return uuid.uuid4().hex[:8]


def md(*lines: str) -> dict:
    src = "\n".join(lines)
    return {
        "cell_type": "markdown",
        "id": cid(),
        "metadata": {},
        "source": [s + "\n" for s in src.split("\n")[:-1]] + [src.split("\n")[-1]]
        if src else [],
    }


def code(*lines: str) -> dict:
    src = "\n".join(lines)
    parts = src.split("\n")
    source = [p + "\n" for p in parts[:-1]] + [parts[-1]] if parts else []
    return {
        "cell_type": "code",
        "id": cid(),
        "metadata": {},
        "execution_count": None,
        "outputs": [],
        "source": source,
    }


def write_nb(name: str, cells: list[dict]) -> None:
    nb = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    (NB_DIR / name).write_text(json.dumps(nb, indent=1) + "\n")
    print(f"wrote {name}")


# Shared inline boilerplate (each notebook duplicates this — pedagogical choice).
BOILERPLATE_IMPORTS = '''import os

from pyiceberg.catalog.rest import RestCatalog

NESSIE_URI       = os.environ["NESSIE_URI"]
S3_ENDPOINT      = os.environ["AWS_S3_ENDPOINT"]
S3_ACCESS_KEY    = os.environ["AWS_ACCESS_KEY_ID"]
S3_SECRET_KEY    = os.environ["AWS_SECRET_ACCESS_KEY"]
WAREHOUSE_BUCKET = os.environ["ICEBERG_WAREHOUSE_BUCKET"]
WAREHOUSE_URI    = f"s3://{WAREHOUSE_BUCKET}/warehouse"

catalog = RestCatalog(
    name="nessie",
    **{
        "uri": NESSIE_URI,
        "warehouse": WAREHOUSE_URI,
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": S3_ACCESS_KEY,
        "s3.secret-access-key": S3_SECRET_KEY,
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    },
)

print("Connected to catalog:", catalog.name)'''

TABLE_ID_LINE = 'TABLE_ID = ("demo", "events")'


# ─────────────────────────────────────────────────────────────────────────────
# 00 — Setup Iceberg Catalog
# ─────────────────────────────────────────────────────────────────────────────
def nb_00():
    cells = [
        md(
            "# 00 — Setup Iceberg Catalog",
            "",
            "Run this notebook **once** after `make up` and `make nessie-init-bucket`.",
            "",
            "It verifies:",
            "1. The `iceberg-warehouse` S3 bucket is accessible in RustFS",
            "2. The Nessie REST catalog is reachable",
            "3. The `demo` namespace exists (creates it if not)",
            "",
            "All connection parameters come from environment variables injected by Docker Compose.",
        ),
        code(
            "import os",
            "",
            "import boto3",
            "from botocore.config import Config",
            "from pyiceberg.catalog.rest import RestCatalog",
            "from pyiceberg.exceptions import NamespaceAlreadyExistsError",
            "",
            'NESSIE_URI       = os.environ["NESSIE_URI"]',
            'S3_ENDPOINT      = os.environ["AWS_S3_ENDPOINT"]',
            'S3_ACCESS_KEY    = os.environ["AWS_ACCESS_KEY_ID"]',
            'S3_SECRET_KEY    = os.environ["AWS_SECRET_ACCESS_KEY"]',
            'WAREHOUSE_BUCKET = os.environ["ICEBERG_WAREHOUSE_BUCKET"]',
            'WAREHOUSE_URI    = f"s3://{WAREHOUSE_BUCKET}/warehouse"',
            "",
            'print(f"Nessie URI    : {NESSIE_URI}")',
            'print(f"S3 endpoint   : {S3_ENDPOINT}")',
            'print(f"Warehouse URI : {WAREHOUSE_URI}")',
        ),
        md("## 1. Verify S3 bucket"),
        code(
            "s3 = boto3.client(",
            '    "s3",',
            "    endpoint_url=S3_ENDPOINT,",
            "    aws_access_key_id=S3_ACCESS_KEY,",
            "    aws_secret_access_key=S3_SECRET_KEY,",
            '    config=Config(s3={"addressing_style": "path"}),',
            '    region_name="us-east-1",',
            ")",
            "",
            'buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]',
            'print("Buckets:", buckets)',
            "",
            "assert WAREHOUSE_BUCKET in buckets, (",
            '    f"Bucket {WAREHOUSE_BUCKET!r} not found. Run: make nessie-init-bucket"',
            ")",
            'print("✔ S3 bucket check passed.")',
        ),
        md("## 2. Connect to Nessie REST catalog"),
        code(
            "catalog = RestCatalog(",
            '    name="nessie",',
            "    **{",
            '        "uri": NESSIE_URI,',
            '        "warehouse": WAREHOUSE_URI,',
            '        "s3.endpoint": S3_ENDPOINT,',
            '        "s3.access-key-id": S3_ACCESS_KEY,',
            '        "s3.secret-access-key": S3_SECRET_KEY,',
            '        "s3.path-style-access": "true",',
            '        "s3.region": "us-east-1",',
            "    },",
            ")",
            "",
            'print("✔ Connected to catalog:", catalog.name)',
        ),
        md("## 3. Create `demo` namespace"),
        code(
            "try:",
            '    catalog.create_namespace("demo")',
            '    print("Namespace \'demo\' created.")',
            "except NamespaceAlreadyExistsError:",
            '    print("Namespace \'demo\' already exists — skipping.")',
            "",
            'print("Namespaces:", catalog.list_namespaces())',
        ),
        md(
            "## 4. List existing tables in `demo`",
            "",
            "On a fresh setup this is empty. After running notebook 01 it will contain `events`.",
        ),
        code(
            'tables = catalog.list_tables("demo")',
            'print(f"Tables in demo ({len(tables)}):")',
            "for t in tables:",
            '    print(" ", t)',
        ),
        md(
            "---",
            "**Next:** [01_write_iceberg_polars.ipynb](01_write_iceberg_polars.ipynb) — first write.",
        ),
    ]
    write_nb("00_setup_catalog.ipynb", cells)


# ─────────────────────────────────────────────────────────────────────────────
# 01 — Write Iceberg Tables with Polars
# ─────────────────────────────────────────────────────────────────────────────
def nb_01():
    cells = [
        md(
            "# 01 — Write Iceberg Tables with Polars",
            "",
            "Demonstrates writing a Polars DataFrame into an Apache Iceberg table stored in RustFS (S3),",
            "with the table registered in the Nessie REST catalog.",
            "",
            "**Data flow:** `Polars DataFrame` → `PyArrow Table` → `PyIceberg append()` → `RustFS (Parquet)` + `Nessie (metadata)`",
            "",
            "**Idempotent:** this notebook drops `demo.events` at the top and recreates it, so re-running gives the same final state.",
            "",
            "**Prerequisites:** Run `00_setup_catalog.ipynb` first.",
        ),
        code(
            "import datetime",
            "import os",
            "",
            "import polars as pl",
            "from pyiceberg.catalog.rest import RestCatalog",
            "from pyiceberg.exceptions import NoSuchTableError",
            "from pyiceberg.io.pyarrow import schema_to_pyarrow",
            "from pyiceberg.partitioning import PartitionField, PartitionSpec",
            "from pyiceberg.schema import Schema",
            "from pyiceberg.transforms import DayTransform",
            "from pyiceberg.types import (",
            "    DoubleType,",
            "    LongType,",
            "    NestedField,",
            "    StringType,",
            "    TimestamptzType,",
            ")",
            "",
            BOILERPLATE_IMPORTS.split("\n", 2)[2],  # env vars + RestCatalog
        ),
        md("## 1. Create a Polars DataFrame"),
        code(
            "UTC = datetime.timezone.utc",
            "",
            "df = pl.DataFrame(",
            "    {",
            '        "event_id":   ["e001", "e002", "e003", "e004", "e005"],',
            '        "user_id":    [101, 102, 103, 101, 104],',
            '        "event_type": ["click", "view", "click", "purchase", "view"],',
            '        "amount":     [0.0, 0.0, 0.0, 49.99, 0.0],',
            '        "ts": [',
            "            datetime.datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC),",
            "            datetime.datetime(2024, 1, 15, 11, 0, 0, tzinfo=UTC),",
            "            datetime.datetime(2024, 1, 16,  9, 0, 0, tzinfo=UTC),",
            "            datetime.datetime(2024, 1, 16, 14, 0, 0, tzinfo=UTC),",
            "            datetime.datetime(2024, 1, 17,  8, 0, 0, tzinfo=UTC),",
            "        ],",
            "    }",
            ")",
            "",
            "print(df)",
            'print("\\nPolars schema:", df.schema)',
        ),
        md(
            "## 2. Define Iceberg schema, partition spec, and (re)create the table",
            "",
            "- `event_id` and `ts` are `required=True` — they're the identifier and time spine.",
            "- `ts` uses `TimestamptzType()` (UTC); never use a naive timestamp for events.",
            "- Partitioned by `days(ts)` so time-range predicates can prune entire files.",
            "- Drop-and-recreate makes the notebook idempotent.",
        ),
        code(
            "iceberg_schema = Schema(",
            "    NestedField(1, \"event_id\",   StringType(),     required=True),",
            "    NestedField(2, \"user_id\",    LongType(),       required=False),",
            "    NestedField(3, \"event_type\", StringType(),     required=False),",
            "    NestedField(4, \"amount\",     DoubleType(),     required=False),",
            "    NestedField(5, \"ts\",         TimestamptzType(), required=True),",
            ")",
            "",
            "partition_spec = PartitionSpec(",
            "    PartitionField(source_id=5, field_id=1000, transform=DayTransform(), name=\"ts_day\"),",
            ")",
            "",
            TABLE_ID_LINE,
            "",
            "try:",
            "    catalog.drop_table(TABLE_ID)",
            "    print(\"Dropped existing table.\")",
            "except NoSuchTableError:",
            "    print(\"No prior table — fresh create.\")",
            "",
            "table = catalog.create_table(",
            "    identifier=TABLE_ID,",
            "    schema=iceberg_schema,",
            "    partition_spec=partition_spec,",
            ")",
            "print(\"Table created:\", table.name())",
            "print(\"Location:    \", table.location())",
        ),
        md(
            "## 3. Write Polars DataFrame to Iceberg",
            "",
            "Polars → PyArrow → PyIceberg `append()`. Two things to note:",
            "1. `polars.to_arrow()` marks every field nullable. PyIceberg 0.9 strictly requires the",
            "   Arrow schema's nullability to match the Iceberg schema's `required` flags, so we",
            "   `cast(schema_to_pyarrow(iceberg_schema))` before appending.",
            "2. After the write, `refresh()` so the local `Table` object sees the new snapshot.",
        ),
        code(
            "arrow_table = df.to_arrow().cast(schema_to_pyarrow(iceberg_schema))",
            "",
            "table.append(arrow_table)",
            "table.refresh()",
            "",
            "print(\"✔ Data written.\")",
            "print(\"Snapshot ID:\", table.current_snapshot().snapshot_id)",
            "print(\"Snapshots in metadata:\", len(table.history()))",
        ),
        md("## 4. Verify — scan back as Polars"),
        code(
            "result = pl.from_arrow(table.scan().to_arrow())",
            "",
            "print(f\"Row count: {len(result)}\")",
            "result",
        ),
        md(
            "## 5. Append more data",
            "",
            "Each `append()` creates a new immutable Iceberg snapshot AND a new Nessie commit.",
            "",
            "**Surprise on Nessie:** `len(table.history())` will still show **1**. Nessie's table",
            "`metadata.json` only carries the current snapshot — the prior snapshot lives as an",
            "earlier Nessie commit, not as a snapshot entry. Catalog-level history is the source",
            "of truth here. See [08_nessie_catalog_refs.ipynb](08_nessie_catalog_refs.ipynb).",
        ),
        code(
            "df2 = pl.DataFrame(",
            "    {",
            "        \"event_id\":   [\"e006\", \"e007\"],",
            "        \"user_id\":    [105, 101],",
            "        \"event_type\": [\"click\", \"purchase\"],",
            "        \"amount\":     [0.0, 99.00],",
            "        \"ts\": [",
            "            datetime.datetime(2024, 1, 18,  9,  0, 0, tzinfo=UTC),",
            "            datetime.datetime(2024, 1, 18, 10, 30, 0, tzinfo=UTC),",
            "        ],",
            "    }",
            ")",
            "",
            "table.append(df2.to_arrow().cast(schema_to_pyarrow(iceberg_schema)))",
            "table.refresh()",
            "",
            "print(\"✔ Second batch written.\")",
            "print(\"Snapshots in metadata:\", len(table.history()))",
            "print(\"Total rows:\", len(pl.from_arrow(table.scan().to_arrow())))",
        ),
        md(
            "---",
            "**Next:**",
            "- [02_read_iceberg_polars.ipynb](02_read_iceberg_polars.ipynb) — reads, projections, predicate pushdown.",
            "- [03_schema_evolution.ipynb](03_schema_evolution.ipynb) — evolve this table's schema.",
            "- [05_row_mutations.ipynb](05_row_mutations.ipynb) — upsert/delete/overwrite.",
        ),
    ]
    write_nb("01_write_iceberg_polars.ipynb", cells)


# ─────────────────────────────────────────────────────────────────────────────
# 02 — Read Iceberg Tables with Polars
# ─────────────────────────────────────────────────────────────────────────────
def nb_02():
    cells = [
        md(
            "# 02 — Read Iceberg Tables with Polars",
            "",
            "Demonstrates reading the `demo.events` Iceberg table with PyIceberg + Polars.",
            "",
            "Covers:",
            "- Full table scan",
            "- Predicate pushdown (filter at Iceberg file-scan layer)",
            "- Column projection",
            "- Overwrite as atomic delete + insert",
            "",
            "**Time travel** is NOT shown here — see `07_iceberg_branches_tags.ipynb` (table-level refs)",
            "and `08_nessie_catalog_refs.ipynb` (catalog-level commits).",
            "",
            "**Prerequisites:** Run `01_write_iceberg_polars.ipynb` first.",
        ),
        code(
            "import polars as pl",
            "from pyiceberg.expressions import EqualTo",
            "from pyiceberg.io.pyarrow import schema_to_pyarrow",
            "",
            BOILERPLATE_IMPORTS,
            "",
            "table = catalog.load_table((\"demo\", \"events\"))",
            "table.refresh()",
            "print(\"Table:\", table.name())",
            "print(\"Schema:\")",
            "print(table.schema())",
        ),
        md("## 1. Table metadata"),
        code(
            "history = table.history()",
            "print(f\"Snapshots in current metadata ({len(history)}):\")",
            "for snap in history:",
            "    print(f\"  id={snap.snapshot_id}  ts={snap.timestamp_ms}\")",
        ),
        md("## 2. Full table scan → Polars DataFrame"),
        code(
            "df = pl.from_arrow(table.scan().to_arrow())",
            "",
            "print(f\"Shape: {df.shape}\")",
            "df",
        ),
        md(
            "## 3. Predicate pushdown + column projection",
            "",
            "The filter is evaluated at the Iceberg file-scan layer. Combined with the",
            "`days(ts)` partition spec from notebook 01, time-bounded queries can prune",
            "whole partitions without reading their Parquet files.",
        ),
        code(
            "df_clicks = pl.from_arrow(",
            "    table.scan(",
            "        row_filter=EqualTo(\"event_type\", \"click\"),",
            "        selected_fields=(\"event_id\", \"user_id\", \"ts\"),",
            "    ).to_arrow()",
            ")",
            "",
            "print(f\"Click events: {len(df_clicks)}\")",
            "df_clicks",
        ),
        md(
            "## 4. Overwrite pattern (row-level update)",
            "",
            "PyIceberg has no in-place row mutation. The standard pattern is",
            "`overwrite()` with a filter — atomic delete-matching + insert.",
            "",
            "For upsert/delete patterns, see [05_row_mutations.ipynb](05_row_mutations.ipynb).",
        ),
        code(
            "import datetime",
            "",
            "UTC = datetime.timezone.utc",
            "",
            "updated_row = pl.DataFrame(",
            "    {",
            "        \"event_id\":   [\"e002\"],",
            "        \"user_id\":    [102],",
            "        \"event_type\": [\"view\"],",
            "        \"amount\":     [5.99],",
            "        \"ts\":         [datetime.datetime(2024, 1, 15, 11, 0, 0, tzinfo=UTC)],",
            "    }",
            ")",
            "",
            "table.overwrite(",
            "    updated_row.to_arrow().cast(schema_to_pyarrow(table.schema())),",
            "    overwrite_filter=EqualTo(\"event_id\", \"e002\"),",
            ")",
            "table.refresh()",
            "",
            "print(\"✔ Overwrite complete. Snapshots:\", len(table.history()))",
            "",
            "df_after = pl.from_arrow(table.scan(row_filter=EqualTo(\"event_id\", \"e002\")).to_arrow())",
            "print(\"Updated row:\")",
            "df_after",
        ),
        md(
            "---",
            "**Next:** [03_schema_evolution.ipynb](03_schema_evolution.ipynb) — add/drop/rename columns.",
        ),
    ]
    write_nb("02_read_iceberg_polars.ipynb", cells)


# ─────────────────────────────────────────────────────────────────────────────
# 03 — Schema Evolution
# ─────────────────────────────────────────────────────────────────────────────
def nb_03():
    cells = [
        md(
            "# 03 — Schema Evolution",
            "",
            "Iceberg supports schema changes as **metadata-only** operations:",
            "no data files are rewritten when you add, drop, rename, or reorder columns,",
            "and old data remains readable through the new schema by column ID.",
            "",
            "**Prerequisites:** Run `01_write_iceberg_polars.ipynb` first.",
        ),
        code(
            "import polars as pl",
            "from pyiceberg.types import LongType, StringType",
            "",
            BOILERPLATE_IMPORTS,
            "",
            "table = catalog.load_table((\"demo\", \"events\"))",
            "table.refresh()",
            "print(\"Starting schema:\")",
            "print(table.schema())",
        ),
        md("## 1. Add a column"),
        code(
            "with table.update_schema() as us:",
            "    us.add_column(\"country\", StringType(), doc=\"ISO country code\")",
            "",
            "table.refresh()",
            "print(\"After add_column:\")",
            "print(table.schema())",
        ),
        md(
            "## 2. Rename a column",
            "",
            "Iceberg tracks columns by stable numeric IDs, so renames don't rewrite data.",
        ),
        code(
            "with table.update_schema() as us:",
            "    us.rename_column(\"event_type\", \"event_kind\")",
            "",
            "table.refresh()",
            "print(table.schema())",
        ),
        md(
            "## 3. Promote a column type",
            "",
            "Iceberg allows safe widening: `int → long`, `float → double`, `decimal(p,s) → decimal(p',s)` where p' ≥ p.",
            "",
            "Here `user_id` is already `LongType`. We'll demonstrate the API on it (no-op promotion).",
        ),
        code(
            "with table.update_schema() as us:",
            "    us.update_column(\"user_id\", LongType())",
            "",
            "table.refresh()",
            "print(\"user_id type:\", table.schema().find_field(\"user_id\").field_type)",
        ),
        md("## 4. Reorder columns"),
        code(
            "with table.update_schema() as us:",
            "    us.move_first(\"ts\")",
            "    us.move_after(\"event_id\", \"ts\")",
            "",
            "table.refresh()",
            "print(\"Field order:\", [f.name for f in table.schema().fields])",
        ),
        md("## 5. Drop a column"),
        code(
            "with table.update_schema() as us:",
            "    us.delete_column(\"country\")",
            "",
            "table.refresh()",
            "print(\"After drop:\", [f.name for f in table.schema().fields])",
        ),
        md(
            "## 6. Read old data through the evolved schema",
            "",
            "Data written under the original schema is still readable — Iceberg resolves",
            "columns by ID, not by position or name.",
        ),
        code(
            "df = pl.from_arrow(table.scan().to_arrow())",
            "print(f\"Row count: {len(df)}\")",
            "df.head()",
        ),
        md(
            "---",
            "**Next:** [04_partition_evolution.ipynb](04_partition_evolution.ipynb) — evolve the partition spec.",
            "Also see [06_metadata_inspection.ipynb](06_metadata_inspection.ipynb) to inspect the metadata-only commits this notebook produced.",
        ),
    ]
    write_nb("03_schema_evolution.ipynb", cells)


# ─────────────────────────────────────────────────────────────────────────────
# 04 — Partition Evolution
# ─────────────────────────────────────────────────────────────────────────────
def nb_04():
    cells = [
        md(
            "# 04 — Partition Evolution",
            "",
            "Iceberg lets you change the partition layout without rewriting historical data.",
            "Old files keep their original spec; new writes use the new spec; reads transparently",
            "stitch them together.",
            "",
            "We'll go from `days(ts)` (set up in 01) to `hours(ts)`, write a new batch under the new",
            "spec, and verify both partitions coexist.",
            "",
            "**Prerequisites:** Run `03_schema_evolution.ipynb` first.",
        ),
        code(
            "import datetime",
            "",
            "import polars as pl",
            "from pyiceberg.io.pyarrow import schema_to_pyarrow",
            "from pyiceberg.transforms import HourTransform",
            "",
            BOILERPLATE_IMPORTS,
            "",
            "table = catalog.load_table((\"demo\", \"events\"))",
            "table.refresh()",
            "print(\"Current partition spec:\", table.spec())",
        ),
        md("## 1. Inspect existing partitions"),
        code(
            "partitions = pl.from_arrow(table.inspect.partitions())",
            "print(f\"Partition rows: {len(partitions)}\")",
            "partitions",
        ),
        md(
            "## 2. Evolve the partition spec — replace `days(ts)` with `hours(ts)`",
            "",
            "Note: in PyIceberg you can't delete and add a field on the same source",
            "column in a single transaction commit if the name collides. We drop the old",
            "field first, commit, then add the new one.",
        ),
        code(
            "with table.update_spec() as us:",
            "    us.remove_field(\"ts_day\")",
            "    us.add_field(\"ts\", HourTransform(), \"ts_hour\")",
            "",
            "table.refresh()",
            "print(\"New partition spec:\", table.spec())",
        ),
        md("## 3. Write a new batch under the new spec"),
        code(
            "UTC = datetime.timezone.utc",
            "",
            "df_new = pl.DataFrame(",
            "    {",
            "        \"event_id\":   [\"e100\", \"e101\", \"e102\"],",
            "        \"user_id\":    [201, 201, 202],",
            "        \"event_kind\": [\"click\", \"view\", \"click\"],",
            "        \"amount\":     [0.0, 0.0, 0.0],",
            "        \"ts\": [",
            "            datetime.datetime(2024, 2, 1, 9,  0, 0, tzinfo=UTC),",
            "            datetime.datetime(2024, 2, 1, 9, 30, 0, tzinfo=UTC),",
            "            datetime.datetime(2024, 2, 1, 10, 0, 0, tzinfo=UTC),",
            "        ],",
            "    }",
            ")",
            "",
            "# Column order may differ from the (possibly evolved) table schema —",
            "# reorder to match, then cast nullability/types to align with Iceberg.",
            "field_names = [f.name for f in table.schema().fields]",
            "table.append(",
            "    df_new.select(field_names).to_arrow().cast(schema_to_pyarrow(table.schema()))",
            ")",
            "table.refresh()",
            "print(\"Snapshots:\", len(table.history()))",
        ),
        md("## 4. Inspect partitions — both specs should appear"),
        code(
            "partitions = pl.from_arrow(table.inspect.partitions())",
            "print(f\"Partition rows after evolution: {len(partitions)}\")",
            "partitions",
        ),
        md(
            "## 5. Read across both specs",
            "",
            "Iceberg handles the spec mismatch transparently — one logical scan covers both layouts.",
        ),
        code(
            "df = pl.from_arrow(table.scan().to_arrow())",
            "print(f\"Total rows: {len(df)}\")",
            "df.sort(\"ts\")",
        ),
        md(
            "---",
            "**Next:** [05_row_mutations.ipynb](05_row_mutations.ipynb) — upserts, deletes, overwrites.",
        ),
    ]
    write_nb("04_partition_evolution.ipynb", cells)


# ─────────────────────────────────────────────────────────────────────────────
# 05 — Row Mutations
# ─────────────────────────────────────────────────────────────────────────────
def nb_05():
    cells = [
        md(
            "# 05 — Row Mutations: upsert, delete, overwrite",
            "",
            "PyIceberg 0.9+ supports three row-level mutation patterns:",
            "- `upsert(df, join_cols=...)` — merge by key (insert new, update existing).",
            "- `delete(delete_filter=...)` — drop rows matching a predicate.",
            "- `overwrite(df, overwrite_filter=...)` — atomic delete-and-insert under filter.",
            "",
            "Each call produces a new snapshot.",
            "",
            "**Prerequisites:** Run `01_write_iceberg_polars.ipynb` first.",
            "This notebook does NOT depend on 03/04 — it ignores any schema/partition changes.",
        ),
        code(
            "import datetime",
            "",
            "import polars as pl",
            "from pyiceberg.expressions import EqualTo, GreaterThan",
            "from pyiceberg.io.pyarrow import schema_to_pyarrow",
            "",
            BOILERPLATE_IMPORTS,
            "",
            "table = catalog.load_table((\"demo\", \"events\"))",
            "table.refresh()",
            "print(\"Start snapshot count:\", len(table.history()))",
            "print(\"Start row count:    \", len(pl.from_arrow(table.scan().to_arrow())))",
        ),
        md(
            "## 1. `upsert()` — merge by `event_id`",
            "",
            "Three rows: one update (`e001` → new amount), two inserts (`e200`, `e201`).",
        ),
        code(
            "UTC = datetime.timezone.utc",
            "",
            "upsert_df = pl.DataFrame(",
            "    {",
            "        \"event_id\":   [\"e001\", \"e200\", \"e201\"],",
            "        \"user_id\":    [101, 301, 302],",
            "        \"event_kind\": [\"click\", \"purchase\", \"view\"],",
            "        \"amount\":     [1.50, 19.99, 0.0],",
            "        \"ts\": [",
            "            datetime.datetime(2024, 3, 1,  9, 0, 0, tzinfo=UTC),",
            "            datetime.datetime(2024, 3, 1, 10, 0, 0, tzinfo=UTC),",
            "            datetime.datetime(2024, 3, 1, 11, 0, 0, tzinfo=UTC),",
            "        ],",
            "    }",
            ")",
            "",
            "# 03 may have renamed event_type → event_kind. Align the column name first,",
            "# then reorder + cast to the live table schema.",
            "schema_names = {f.name for f in table.schema().fields}",
            "if \"event_type\" in schema_names and \"event_kind\" not in schema_names:",
            "    upsert_df = upsert_df.rename({\"event_kind\": \"event_type\"})",
            "",
            "field_names = [f.name for f in table.schema().fields]",
            "result = table.upsert(",
            "    upsert_df.select(field_names).to_arrow().cast(schema_to_pyarrow(table.schema())),",
            "    join_cols=[\"event_id\"],",
            ")",
            "table.refresh()",
            "",
            "print(\"Rows updated:\", result.rows_updated)",
            "print(\"Rows inserted:\", result.rows_inserted)",
            "print(\"Snapshots:\", len(table.history()))",
        ),
        md("## 2. `delete()` with a row filter"),
        code(
            "table.delete(delete_filter=GreaterThan(\"amount\", 50.0))",
            "table.refresh()",
            "",
            "print(\"Snapshots:\", len(table.history()))",
            "print(\"Rows after delete:\", len(pl.from_arrow(table.scan().to_arrow())))",
        ),
        md(
            "## 3. `overwrite()` with a filter — atomic replace-by-key",
            "",
            "Useful when you want delete-and-insert semantics in one commit.",
        ),
        code(
            "replace_kind = \"event_kind\" if \"event_kind\" in {f.name for f in table.schema().fields} else \"event_type\"",
            "",
            "replacement = pl.DataFrame(",
            "    {",
            "        \"event_id\":   [\"e001\"],",
            "        \"user_id\":    [101],",
            "        replace_kind:  [\"click\"],",
            "        \"amount\":     [2.50],",
            "        \"ts\":         [datetime.datetime(2024, 3, 2, 12, 0, 0, tzinfo=UTC)],",
            "    }",
            ")",
            "",
            "field_names = [f.name for f in table.schema().fields]",
            "table.overwrite(",
            "    replacement.select(field_names).to_arrow().cast(schema_to_pyarrow(table.schema())),",
            "    overwrite_filter=EqualTo(\"event_id\", \"e001\"),",
            ")",
            "table.refresh()",
            "print(\"Snapshots:\", len(table.history()))",
        ),
        md("## 4. Final state"),
        code(
            "df = pl.from_arrow(table.scan().to_arrow()).sort(\"ts\")",
            "print(f\"Final row count: {len(df)}\")",
            "df",
        ),
        md(
            "---",
            "**Next:** [06_metadata_inspection.ipynb](06_metadata_inspection.ipynb) — query the snapshots, files, manifests this notebook created.",
            "Also: [09_snapshot_management.ipynb](09_snapshot_management.ipynb) for rollback to any of these snapshots.",
        ),
    ]
    write_nb("05_row_mutations.ipynb", cells)


# ─────────────────────────────────────────────────────────────────────────────
# 06 — Metadata Inspection
# ─────────────────────────────────────────────────────────────────────────────
def nb_06():
    cells = [
        md(
            "# 06 — Metadata Inspection",
            "",
            "Every Iceberg table exposes its internals as queryable metadata tables via",
            "`table.inspect.*`. Each call returns a PyArrow `Table` — convert to Polars for analysis.",
            "",
            "This is the reference notebook the rest of the curriculum links to.",
            "",
            "**Prerequisites:** Run 01–05 to populate snapshots, files, and refs.",
        ),
        code(
            "import polars as pl",
            "",
            BOILERPLATE_IMPORTS,
            "",
            "table = catalog.load_table((\"demo\", \"events\"))",
            "table.refresh()",
            "print(\"Table:\", table.name())",
        ),
        md("## 1. `inspect.snapshots()` — every snapshot the current metadata knows about"),
        code(
            "snapshots = pl.from_arrow(table.inspect.snapshots())",
            "print(f\"Snapshots: {len(snapshots)}\")",
            "snapshots",
        ),
        md("## 2. `inspect.files()` — every data file in the current snapshot"),
        code(
            "files = pl.from_arrow(table.inspect.files())",
            "print(f\"Files: {len(files)}\")",
            "files.select([\"file_path\", \"file_format\", \"record_count\", \"file_size_in_bytes\"])",
        ),
        md("## 3. `inspect.manifests()` — manifest files (one level above data files)"),
        code(
            "manifests = pl.from_arrow(table.inspect.manifests())",
            "print(f\"Manifests: {len(manifests)}\")",
            "manifests",
        ),
        md("## 4. `inspect.history()` — operation log"),
        code(
            "history = pl.from_arrow(table.inspect.history())",
            "history",
        ),
        md("## 5. `inspect.partitions()` — current partition layout"),
        code(
            "partitions = pl.from_arrow(table.inspect.partitions())",
            "partitions",
        ),
        md(
            "## 6. `inspect.refs()` — Iceberg-native branches and tags",
            "",
            "Not to be confused with Nessie catalog refs (see notebook 08). These are",
            "stored inside the table's own metadata.",
        ),
        code(
            "refs = pl.from_arrow(table.inspect.refs())",
            "refs",
        ),
        md(
            "---",
            "**Cross-references:**",
            "- [07_iceberg_branches_tags.ipynb](07_iceberg_branches_tags.ipynb) creates entries that show up in `inspect.refs()`.",
            "- [09_snapshot_management.ipynb](09_snapshot_management.ipynb) uses `inspect.snapshots()` to pick rollback targets.",
            "- [10_spark_maintenance.ipynb](10_spark_maintenance.ipynb) compares `inspect.files()` counts before and after compaction.",
        ),
    ]
    write_nb("06_metadata_inspection.ipynb", cells)


# ─────────────────────────────────────────────────────────────────────────────
# 07 — Iceberg-native branches & tags (table-level)
# ─────────────────────────────────────────────────────────────────────────────
def nb_07():
    cells = [
        md(
            "# 07 — Iceberg-native Branches & Tags (and why they're a no-op on Nessie)",
            "",
            "Iceberg's spec defines a table-level ref system stored **inside `metadata.json`**:",
            "- A **tag** pins a specific snapshot under a stable name.",
            "- A **branch** is an independent line of snapshots for a single table.",
            "- `manage_snapshots().create_tag()` / `create_branch()` are how you write them.",
            "- `scan().use_ref(name)` reads through a ref.",
            "",
            "**This notebook demonstrates that those APIs are effectively no-ops on a Nessie-backed",
            "catalog.** Nessie owns the metadata and only persists a single snapshot reference per",
            "commit, so Iceberg-native refs aren't round-tripped. You'll see `create_tag()` return",
            "without error, but `inspect.refs()` still shows only `main`.",
            "",
            "The right pattern on Nessie is **catalog-level refs** — see",
            "[08_nessie_catalog_refs.ipynb](08_nessie_catalog_refs.ipynb).",
            "",
            "| Layer | Scope | Where stored | Works on Nessie? |",
            "|---|---|---|---|",
            "| Iceberg table refs (this notebook) | One table | Inside `metadata.json` | **No** — silent no-op |",
            "| Nessie catalog refs (08) | Whole catalog | Nessie commit graph | Yes |",
            "",
            "**Prerequisites:** Run 01–05.",
        ),
        code(
            "import polars as pl",
            "",
            BOILERPLATE_IMPORTS,
            "",
            "table = catalog.load_table((\"demo\", \"events\"))",
            "table.refresh()",
            "current_sid = table.current_snapshot().snapshot_id",
            "print(\"Current snapshot:\", current_sid)",
            "print()",
            "print(\"Refs before any manage_snapshots calls:\")",
            "print(pl.from_arrow(table.inspect.refs()))",
        ),
        md(
            "## 1. Try to create an Iceberg tag",
            "",
            "The call returns cleanly — no exception, no warning.",
        ),
        code(
            "with table.manage_snapshots() as ms:",
            "    ms.create_tag(snapshot_id=current_sid, tag_name=\"v1\")",
            "",
            "table.refresh()",
            "print(\"Refs after create_tag('v1'):\")",
            "print(pl.from_arrow(table.inspect.refs()))",
            "print()",
            "print(\"^ Notice 'v1' is missing. Nessie didn't persist the tag.\")",
        ),
        md(
            "## 2. Try to create an Iceberg branch",
            "",
            "Same pattern: API returns OK, ref doesn't show up.",
        ),
        code(
            "with table.manage_snapshots() as ms:",
            "    ms.create_branch(snapshot_id=current_sid, branch_name=\"experiment\")",
            "",
            "table.refresh()",
            "print(\"Refs after create_branch('experiment'):\")",
            "print(pl.from_arrow(table.inspect.refs()))",
        ),
        md(
            "## 3. Confirm the consequence — `use_ref` cannot find them",
            "",
            "Because the refs weren't persisted, `scan().use_ref('v1')` fails with",
            "`ValueError: Cannot scan unknown ref=v1`.",
        ),
        code(
            "try:",
            "    df_v1 = pl.from_arrow(table.scan().use_ref(\"v1\").to_arrow())",
            "    print(\"Rows at tag v1:\", len(df_v1))",
            "except ValueError as e:",
            "    print(\"Expected failure:\", e)",
        ),
        md(
            "## 4. Why this happens",
            "",
            "Iceberg's REST spec allows a server to return a subset of metadata. Nessie's adapter",
            "synthesizes a fresh `metadata.json` containing **only** the current snapshot and the",
            "`main` ref pointing at it. The `update_table` commit Nessie accepts doesn't include",
            "ref additions outside of `main`, so PyIceberg's locally-built `AddSnapshotRefUpdate`",
            "doesn't survive the round-trip.",
            "",
            "**Takeaway:** if you're on Nessie, don't reach for `manage_snapshots().create_tag` or",
            "`create_branch`. Use Nessie catalog refs in [08](08_nessie_catalog_refs.ipynb).",
            "",
            "On a Hive Metastore or Glue catalog, the same code would work — those catalogs persist",
            "whatever metadata PyIceberg hands them.",
        ),
        md(
            "---",
            "**Next:** [08_nessie_catalog_refs.ipynb](08_nessie_catalog_refs.ipynb).",
        ),
    ]
    write_nb("07_iceberg_branches_tags.ipynb", cells)


# ─────────────────────────────────────────────────────────────────────────────
# 08 — Nessie Catalog Refs (catalog-level versioning)
# ─────────────────────────────────────────────────────────────────────────────
def nb_08():
    cells = [
        md(
            "# 08 — Nessie Catalog Refs (catalog-level)",
            "",
            "Nessie versions the **entire catalog** as a Git-like commit graph. Every catalog",
            "mutation (create_table, append, overwrite, drop, schema change) is a commit on a",
            "branch (default `main`). You can:",
            "",
            "- list the commit history,",
            "- read any past version by pointing a fresh `RestCatalog` at a commit hash,",
            "- create branches and tags via the Nessie API,",
            "- write on a branch without affecting `main`.",
            "",
            "This is the real \"time travel\" pattern for this stack. Iceberg's own `snapshot_id`-based",
            "time travel doesn't work on Nessie because Nessie's `metadata.json` retains only the",
            "current snapshot — history lives at the catalog layer.",
            "",
            "**Prerequisites:** Run `01_write_iceberg_polars.ipynb` (more runs of 02–05 give a richer commit log).",
        ),
        code(
            "import os",
            "",
            "import polars as pl",
            "import requests",
            "from pyiceberg.catalog.rest import RestCatalog",
            "from pyiceberg.io.pyarrow import schema_to_pyarrow",
            "",
            "NESSIE_URI       = os.environ[\"NESSIE_URI\"]              # http://nessie:19120/iceberg",
            "NESSIE_API       = NESSIE_URI.replace(\"/iceberg\", \"/api/v2\")",
            "S3_ENDPOINT      = os.environ[\"AWS_S3_ENDPOINT\"]",
            "S3_ACCESS_KEY    = os.environ[\"AWS_ACCESS_KEY_ID\"]",
            "S3_SECRET_KEY    = os.environ[\"AWS_SECRET_ACCESS_KEY\"]",
            "WAREHOUSE_BUCKET = os.environ[\"ICEBERG_WAREHOUSE_BUCKET\"]",
            "WAREHOUSE_URI    = f\"s3://{WAREHOUSE_BUCKET}/warehouse\"",
            "",
            "S3_PROPS = {",
            "    \"warehouse\": WAREHOUSE_URI,",
            "    \"s3.endpoint\": S3_ENDPOINT,",
            "    \"s3.access-key-id\": S3_ACCESS_KEY,",
            "    \"s3.secret-access-key\": S3_SECRET_KEY,",
            "    \"s3.path-style-access\": \"true\",",
            "    \"s3.region\": \"us-east-1\",",
            "}",
            "",
            "print(\"Nessie Iceberg REST :\", NESSIE_URI)",
            "print(\"Nessie native API   :\", NESSIE_API)",
        ),
        md(
            "## 1. List the commit history of `main`",
            "",
            "Each commit corresponds to one catalog mutation (create_table, append, etc).",
        ),
        code(
            "r = requests.get(f\"{NESSIE_API}/trees/main/history\", params={\"maxRecords\": 20})",
            "r.raise_for_status()",
            "entries = r.json()[\"logEntries\"]",
            "print(f\"Recent commits on main ({len(entries)}):\")",
            "for e in entries[:10]:",
            "    cm = e[\"commitMeta\"]",
            "    print(f\"  {cm['hash'][:12]}  {cm['commitTime']}  {cm.get('message','')[:60]}\")",
        ),
        md(
            "## 2. Read a past version by commit hash",
            "",
            "Point a fresh `RestCatalog` at `{NESSIE_URI}/main@{hash}` — Nessie's URL grammar",
            "for \"a named ref at a specific commit\". A bare hash without a ref name doesn't",
            "work because Nessie's parser requires ref names to start with a letter.",
        ),
        code(
            "# Pick a hash a few commits back so the table state differs from main HEAD.",
            "# Going too far back lands on a commit before the table existed.",
            "past_idx  = min(5, len(entries) - 1)",
            "past_hash = entries[past_idx][\"commitMeta\"][\"hash\"]",
            "print(f\"Reading at commit #{past_idx}: {past_hash[:12]}\")",
            "",
            "catalog_past = RestCatalog(",
            "    name=\"nessie-past\",",
            "    **{**S3_PROPS, \"uri\": f\"{NESSIE_URI}/main@{past_hash}\"},",
            ")",
            "",
            "try:",
            "    t_past = catalog_past.load_table((\"demo\", \"events\"))",
            "    df_past = pl.from_arrow(t_past.scan().to_arrow())",
            "    print(f\"Rows at past commit: {len(df_past)}\")",
            "    print(f\"Rows at main HEAD:   {len(pl.from_arrow(RestCatalog(name='m', **{**S3_PROPS, 'uri': NESSIE_URI}).load_table(('demo','events')).scan().to_arrow()))}\")",
            "except Exception as e:",
            "    print(\"Table didn't exist at that commit yet:\", e)",
        ),
        md(
            "## 3. Create a Nessie branch `dev` from `main`",
            "",
            "Nessie API v2 ref creation: `POST /trees?name=<new>&type=branch|tag` with the **source** reference",
            "(`{type, name, hash}`) in the JSON body. Delete uses `DELETE /trees/{name}@{hash}` —",
            "the expected hash is embedded in the URL with `@`.",
        ),
        code(
            "# Need current main hash for the source ref.",
            "main_ref = requests.get(f\"{NESSIE_API}/trees/main\").json()[\"reference\"]",
            "main_hash = main_ref[\"hash\"]",
            "",
            "# Idempotent: if `dev` already exists, delete it first.",
            "existing = requests.get(f\"{NESSIE_API}/trees/dev\")",
            "if existing.status_code == 200:",
            "    h = existing.json()[\"reference\"][\"hash\"]",
            "    requests.delete(f\"{NESSIE_API}/trees/dev@{h}\")",
            "",
            "create = requests.post(",
            "    f\"{NESSIE_API}/trees\",",
            "    params={\"name\": \"dev\", \"type\": \"branch\"},",
            "    json={\"type\": \"BRANCH\", \"name\": \"main\", \"hash\": main_hash},",
            ")",
            "create.raise_for_status()",
            "print(\"Created branch dev →\", create.json()[\"reference\"][\"hash\"][:12])",
        ),
        md("## 4. Write on `dev` — `main` is unaffected"),
        code(
            "import datetime",
            "",
            "catalog_dev = RestCatalog(",
            "    name=\"nessie-dev\",",
            "    **{**S3_PROPS, \"uri\": f\"{NESSIE_URI}/dev\"},",
            ")",
            "t_dev = catalog_dev.load_table((\"demo\", \"events\"))",
            "",
            "UTC = datetime.timezone.utc",
            "kind_col = \"event_kind\" if \"event_kind\" in {f.name for f in t_dev.schema().fields} else \"event_type\"",
            "",
            "dev_batch = pl.DataFrame(",
            "    {",
            "        \"event_id\":   [\"e_dev_1\"],",
            "        \"user_id\":    [999],",
            "        kind_col:      [\"click\"],",
            "        \"amount\":     [0.0],",
            "        \"ts\":         [datetime.datetime(2024, 4, 1, 0, 0, 0, tzinfo=UTC)],",
            "    }",
            ")",
            "",
            "field_names = [f.name for f in t_dev.schema().fields]",
            "t_dev.append(",
            "    dev_batch.select(field_names).to_arrow().cast(schema_to_pyarrow(t_dev.schema()))",
            ")",
            "t_dev.refresh()",
            "",
            "catalog_main = RestCatalog(name=\"nessie-main\", **{**S3_PROPS, \"uri\": NESSIE_URI})",
            "t_main = catalog_main.load_table((\"demo\", \"events\"))",
            "",
            "rows_dev  = len(pl.from_arrow(t_dev.scan().to_arrow()))",
            "rows_main = len(pl.from_arrow(t_main.scan().to_arrow()))",
            "print(f\"rows on dev:  {rows_dev}\")",
            "print(f\"rows on main: {rows_main}  (unchanged)\")",
        ),
        md("## 5. Create a Nessie tag pinning the current main"),
        code(
            "existing = requests.get(f\"{NESSIE_API}/trees/release-v1\")",
            "if existing.status_code == 200:",
            "    h = existing.json()[\"reference\"][\"hash\"]",
            "    requests.delete(f\"{NESSIE_API}/trees/release-v1@{h}\")",
            "",
            "tag = requests.post(",
            "    f\"{NESSIE_API}/trees\",",
            "    params={\"name\": \"release-v1\", \"type\": \"tag\"},",
            "    json={\"type\": \"BRANCH\", \"name\": \"main\", \"hash\": main_hash},",
            ")",
            "tag.raise_for_status()",
            "print(\"Tag release-v1 →\", tag.json()[\"reference\"][\"hash\"][:12])",
        ),
        md("## 6. Diff `main` vs `dev`"),
        code(
            "diff = requests.get(f\"{NESSIE_API}/trees/main/diff/dev\").json()",
            "print(\"Diffs:\", len(diff.get(\"diffs\", [])))",
            "for d in diff.get(\"diffs\", [])[:5]:",
            "    print(\" \", d[\"key\"], \"from\", d.get(\"from\"), \"→\", d.get(\"to\"))",
        ),
        md(
            "## 7. When to use which",
            "",
            "- **Iceberg table refs** (notebook 07) — version one table independently. Good for tagging a release of *one* dataset, or A/B testing a single table.",
            "- **Nessie catalog refs** (this notebook) — version many tables together. Good for cross-table atomic changes (ETL that touches 5 tables and either all succeed or all roll back), and for the Git-style PR workflow (commit on a branch, review, merge).",
            "",
            "---",
            "**Next:** [09_snapshot_management.ipynb](09_snapshot_management.ipynb) — rollback and expire snapshots.",
        ),
    ]
    write_nb("08_nessie_catalog_refs.ipynb", cells)


# ─────────────────────────────────────────────────────────────────────────────
# 09 — Snapshot Management
# ─────────────────────────────────────────────────────────────────────────────
def nb_09():
    cells = [
        md(
            "# 09 — Snapshot Management: the Nessie way",
            "",
            "Vanilla Iceberg has two snapshot-management primitives:",
            "- `rollback_to_snapshot_id(sid)` — point the table at an earlier snapshot.",
            "- `expire_snapshots(...)` — drop old snapshots from metadata to cap retention.",
            "",
            "Two reasons neither shows up in this notebook the way you'd expect:",
            "",
            "1. **PyIceberg 0.9.1 doesn't expose them** at the `Table.manage_snapshots()` level.",
            "   `manage_snapshots()` here is limited to `create_tag`, `create_branch`,",
            "   `remove_tag`, `remove_branch`. Both `expire_snapshots` and `rollback_to_snapshot_id`",
            "   live as Spark stored procedures — see [10_spark_maintenance.ipynb](10_spark_maintenance.ipynb).",
            "",
            "2. **Nessie collapses snapshot history into the catalog layer.** `metadata.json`",
            "   carries only the current snapshot. The richer version history is the Nessie",
            "   commit log, and rollback is done by reassigning the `main` branch.",
            "",
            "This notebook demonstrates the Nessie-native rollback pattern, which actually works.",
            "",
            "**Prerequisites:** Run 01–05.",
        ),
        code(
            "import polars as pl",
            "import requests",
            "",
            BOILERPLATE_IMPORTS,
            "",
            "NESSIE_API = NESSIE_URI.replace(\"/iceberg\", \"/api/v2\")",
            "table = catalog.load_table((\"demo\", \"events\"))",
            "table.refresh()",
            "print(\"Snapshots in Iceberg metadata:\", len(table.history()))",
        ),
        md(
            "## 1. Confirm: only one snapshot in Iceberg metadata",
            "",
            "Even after many appends/upserts/overwrites in 01–05, `inspect.snapshots()` returns 1.",
            "Nessie discards earlier snapshot entries when synthesizing `metadata.json` for the",
            "current commit.",
        ),
        code(
            "snaps = pl.from_arrow(table.inspect.snapshots())",
            "print(f\"Rows in inspect.snapshots(): {len(snaps)}\")",
            "snaps.select([\"snapshot_id\", \"committed_at\", \"operation\"])",
        ),
        md(
            "## 2. The real history lives in Nessie's commit log",
            "",
            "Every `append`, `overwrite`, `upsert`, `delete`, schema-evolve, partition-evolve from",
            "01–05 is a Nessie commit on `main`. That's the version history you can roll over.",
        ),
        code(
            "hist = requests.get(",
            "    f\"{NESSIE_API}/trees/main/history\",",
            "    params={\"maxRecords\": 50},",
            ").json()[\"logEntries\"]",
            "",
            "print(f\"Commits on main: {len(hist)}\")",
            "for e in hist[:8]:",
            "    cm = e[\"commitMeta\"]",
            "    print(f\"  {cm['hash'][:12]}  {cm.get('message','')[:60]}\")",
        ),
        md(
            "## 3. Roll `main` back to a prior commit",
            "",
            "Nessie API: `PUT /trees/{ref}@{expected_hash}` with the **target** Reference in the",
            "body. This is a Git-style branch reset for the entire catalog state.",
        ),
        code(
            "current_main = requests.get(f\"{NESSIE_API}/trees/main\").json()[\"reference\"]",
            "target_hash  = hist[min(5, len(hist) - 1)][\"commitMeta\"][\"hash\"]",
            "",
            "rows_before = len(pl.from_arrow(table.scan().to_arrow()))",
            "print(f\"Rows before rollback: {rows_before}\")",
            "print(f\"Reassigning main: {current_main['hash'][:12]} → {target_hash[:12]}\")",
            "",
            "r = requests.put(",
            "    f\"{NESSIE_API}/trees/main@{current_main['hash']}\",",
            "    json={\"type\": \"BRANCH\", \"name\": \"main\", \"hash\": target_hash},",
            ")",
            "r.raise_for_status()",
            "print(\"main now at:\", r.json()[\"reference\"][\"hash\"][:12])",
            "",
            "# Reload — metadata.json now comes from the rolled-back main HEAD.",
            "table = catalog.load_table((\"demo\", \"events\"))",
            "rows_after = len(pl.from_arrow(table.scan().to_arrow()))",
            "print(f\"Rows after rollback:  {rows_after}\")",
        ),
        md(
            "## 4. Roll back to a tag",
            "",
            "Same call, with a tag's hash. If you created `release-v1` in notebook 08, you can",
            "pin `main` to that release state and \"undo\" everything since.",
        ),
        code(
            "tag = requests.get(f\"{NESSIE_API}/trees/release-v1\")",
            "if tag.status_code == 200:",
            "    tag_hash = tag.json()[\"reference\"][\"hash\"]",
            "    main_now = requests.get(f\"{NESSIE_API}/trees/main\").json()[\"reference\"][\"hash\"]",
            "    r = requests.put(",
            "        f\"{NESSIE_API}/trees/main@{main_now}\",",
            "        json={\"type\": \"BRANCH\", \"name\": \"main\", \"hash\": tag_hash},",
            "    )",
            "    r.raise_for_status()",
            "    print(\"main now pinned to release-v1:\", r.json()[\"reference\"][\"hash\"][:12])",
            "else:",
            "    print(\"release-v1 tag not present — run notebook 08 first to create it.\")",
        ),
        md(
            "## 5. Orphan data files",
            "",
            "Rolling `main` backwards leaves data files written by now-orphaned commits on S3.",
            "Iceberg-level expire/rollback APIs (when available) can't see them — they're outside",
            "current metadata. The right cleanup is `system.remove_orphan_files` via Spark.",
            "",
            "---",
            "**Next:** [10_spark_maintenance.ipynb](10_spark_maintenance.ipynb) — compaction, manifest rewrite, orphan-file cleanup.",
        ),
    ]
    write_nb("09_snapshot_management.ipynb", cells)


# ─────────────────────────────────────────────────────────────────────────────
# 10 — Spark Maintenance
# ─────────────────────────────────────────────────────────────────────────────
def nb_10():
    cells = [
        md(
            "# 10 — Spark-driven Maintenance: Compaction, Manifest Rewrite, Orphan Cleanup",
            "",
            "Some Iceberg maintenance operations are exposed only as **Spark stored procedures**:",
            "- `rewrite_data_files` — compact small files into larger ones.",
            "- `rewrite_manifests` — consolidate fragmented manifests.",
            "- `remove_orphan_files` — delete data files no longer referenced by any snapshot.",
            "",
            "PyIceberg 0.9 doesn't implement these. We use PySpark inside the Jupyter container,",
            "running in `local[*]` mode and pulling Iceberg + Nessie runtimes via `--packages`.",
            "",
            "First `getOrCreate()` downloads ~200 MB of jars to `/home/jovyan/.ivy2` — this is",
            "persisted across kernel restarts via the notebook volume mount.",
            "",
            "**Prerequisites:** Run 01–09 for a fragmented file layout worth compacting.",
        ),
        code(
            "import os",
            "",
            "import polars as pl",
            "from pyiceberg.catalog.rest import RestCatalog",
            "",
            "S3_ACCESS_KEY    = os.environ[\"AWS_ACCESS_KEY_ID\"]",
            "S3_SECRET_KEY    = os.environ[\"AWS_SECRET_ACCESS_KEY\"]",
            "WAREHOUSE_BUCKET = os.environ[\"ICEBERG_WAREHOUSE_BUCKET\"]",
            "",
            "# PyIceberg-side: count files before compaction.",
            "py_catalog = RestCatalog(",
            "    name=\"nessie\",",
            "    **{",
            "        \"uri\": os.environ[\"NESSIE_URI\"],",
            "        \"warehouse\": f\"s3://{WAREHOUSE_BUCKET}/warehouse\",",
            "        \"s3.endpoint\": os.environ[\"AWS_S3_ENDPOINT\"],",
            "        \"s3.access-key-id\": S3_ACCESS_KEY,",
            "        \"s3.secret-access-key\": S3_SECRET_KEY,",
            "        \"s3.path-style-access\": \"true\",",
            "        \"s3.region\": \"us-east-1\",",
            "    },",
            ")",
            "py_table = py_catalog.load_table((\"demo\", \"events\"))",
            "py_table.refresh()",
            "",
            "files_before = pl.from_arrow(py_table.inspect.files())",
            "print(f\"Data files before maintenance: {len(files_before)}\")",
        ),
        md(
            "## 1. Build a SparkSession with Iceberg + Nessie runtimes",
            "",
            "Spark talks to Nessie's **native** API (`/api/v2`), not the Iceberg REST endpoint —",
            "the Nessie Spark extensions need it for branch/tag DDL.",
        ),
        code(
            "from pyspark.sql import SparkSession",
            "",
            "PACKAGES = \",\".join([",
            "    \"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1\",",
            "    \"org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.99.0\",",
            "    \"software.amazon.awssdk:bundle:2.24.0\",",
            "    # hadoop-aws + matching aws-sdk give Spark the 's3a' filesystem.",
            "    # Needed by remove_orphan_files (it scans S3 via Hadoop FS, not via Iceberg FileIO).",
            "    \"org.apache.hadoop:hadoop-aws:3.3.4\",",
            "])",
            "",
            "spark = (",
            "    SparkSession.builder",
            "    .appName(\"lakehouse-maintenance\")",
            "    .master(\"local[*]\")",
            "    .config(\"spark.jars.packages\", PACKAGES)",
            "    .config(",
            "        \"spark.sql.extensions\",",
            "        \"org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\"",
            "        \"org.projectnessie.spark.extensions.NessieSparkSessionExtensions\",",
            "    )",
            "    .config(\"spark.sql.catalog.nessie\", \"org.apache.iceberg.spark.SparkCatalog\")",
            "    .config(\"spark.sql.catalog.nessie.catalog-impl\", \"org.apache.iceberg.nessie.NessieCatalog\")",
            "    .config(\"spark.sql.catalog.nessie.uri\", \"http://nessie:19120/api/v2\")",
            "    .config(\"spark.sql.catalog.nessie.ref\", \"main\")",
            "    .config(\"spark.sql.catalog.nessie.warehouse\", f\"s3://{WAREHOUSE_BUCKET}/warehouse\")",
            "    .config(\"spark.sql.catalog.nessie.io-impl\", \"org.apache.iceberg.aws.s3.S3FileIO\")",
            "    .config(\"spark.sql.catalog.nessie.s3.endpoint\", os.environ[\"AWS_S3_ENDPOINT\"])",
            "    .config(\"spark.sql.catalog.nessie.s3.path-style-access\", \"true\")",
            "    .config(\"spark.sql.catalog.nessie.s3.access-key-id\", S3_ACCESS_KEY)",
            "    .config(\"spark.sql.catalog.nessie.s3.secret-access-key\", S3_SECRET_KEY)",
            "    # Map both s3 and s3a schemes to S3AFileSystem for remove_orphan_files's S3 scan.",
            "    .config(\"spark.hadoop.fs.s3.impl\",  \"org.apache.hadoop.fs.s3a.S3AFileSystem\")",
            "    .config(\"spark.hadoop.fs.s3a.impl\", \"org.apache.hadoop.fs.s3a.S3AFileSystem\")",
            "    .config(\"spark.hadoop.fs.s3a.endpoint\", os.environ[\"AWS_S3_ENDPOINT\"])",
            "    .config(\"spark.hadoop.fs.s3a.path.style.access\", \"true\")",
            "    .config(\"spark.hadoop.fs.s3a.access.key\", S3_ACCESS_KEY)",
            "    .config(\"spark.hadoop.fs.s3a.secret.key\", S3_SECRET_KEY)",
            "    .config(\"spark.hadoop.fs.s3a.connection.ssl.enabled\", \"false\")",
            "    .config(\"spark.jars.ivy\", \"/home/jovyan/.ivy2\")",
            "    .getOrCreate()",
            ")",
            "",
            "print(\"Spark version:\", spark.version)",
        ),
        md("## 2. Smoke test — list tables via Spark"),
        code(
            "spark.sql(\"SHOW TABLES IN nessie.demo\").show()",
        ),
        md(
            "## 3. Compact small files — `rewrite_data_files`",
            "",
            "The default `min-input-files=5` skips partitions with fewer files. We lower it",
            "to 2 so the procedure at least *considers* our partitions, and shrink",
            "`target-file-size-bytes` to 1 MB.",
            "",
            "Expect a near-zero result here — this lab's table has ~10 tiny Parquet files",
            "spread across ~9 partitions, so most partitions still have just one file and",
            "have nothing to merge with. On a real table with thousands of small files per",
            "partition (typical of streaming ingest), you'd see meaningful consolidation.",
        ),
        code(
            "spark.sql(\"\"\"",
            "    CALL nessie.system.rewrite_data_files(",
            "      table => 'demo.events',",
            "      options => map(",
            "        'min-input-files', '2',",
            "        'target-file-size-bytes', '1048576'",
            "      )",
            "    )",
            "\"\"\").show(truncate=False)",
        ),
        md(
            "## 4. Rewrite manifests — `rewrite_manifests`",
            "",
            "Same logic: default `min-count-to-merge=100` is far too high for demo data,",
            "lower it so consolidation actually fires.",
        ),
        code(
            "spark.sql(\"\"\"",
            "    CALL nessie.system.rewrite_manifests(",
            "      table => 'demo.events'",
            "    )",
            "\"\"\").show(truncate=False)",
        ),
        md(
            "## 5. Remove orphan files (dry run)",
            "",
            "Three things to know:",
            "1. The `gc.enabled` table property must be `true` — Iceberg refuses to GC otherwise.",
            "2. The procedure refuses `older_than` < 24h to avoid racing with in-flight writes.",
            "3. **On Nessie, this procedure is dangerous in general.** Files written by past Nessie",
            "   commits (other branches, tags, history) look like \"orphans\" to Iceberg's snapshot-",
            "   based view, but they're still referenced by Nessie commits. Running this for real",
            "   on a Nessie-backed table can corrupt other branches. The Nessie-correct tool is",
            "   [`nessie-gc`](https://projectnessie.org/nessie-latest/gc/), which is",
            "   reference-aware. Spark will emit a `NessieUtil` warning when you flip `gc.enabled`.",
            "",
            "We use `dry_run => true` so Iceberg lists candidates without deleting anything.",
            "Expect 0 candidates in this lab — every file we wrote is still reachable from some",
            "Nessie commit, so by Iceberg's lights nothing is orphaned yet.",
        ),
        code(
            "spark.sql(\"\"\"",
            "    ALTER TABLE nessie.demo.events",
            "    SET TBLPROPERTIES ('gc.enabled' = 'true')",
            "\"\"\")",
            "",
            "result = spark.sql(\"\"\"",
            "    CALL nessie.system.remove_orphan_files(",
            "      table => 'demo.events',",
            "      dry_run => true",
            "    )",
            "\"\"\")",
            "",
            "rows = result.collect()",
            "print(f\"Orphan candidates (dry run): {len(rows)}\")",
            "for r in rows[:10]:",
            "    print(\" \", r['orphan_file_location'])",
        ),
        md("## 6. File count after maintenance"),
        code(
            "py_table.refresh()",
            "files_after = pl.from_arrow(py_table.inspect.files())",
            "print(f\"Data files after maintenance: {len(files_after)}\")",
            "print(f\"Net change: {len(files_before)} → {len(files_after)}\")",
        ),
        md("## 7. Stop Spark"),
        code(
            "spark.stop()",
        ),
        md(
            "---",
            "**Curriculum complete.** See [docs/iceberg/CURRICULUM.md](../docs/iceberg/CURRICULUM.md) for the notebook index and dependency DAG.",
        ),
    ]
    write_nb("10_spark_maintenance.ipynb", cells)


def main():
    nb_00()
    nb_01()
    nb_02()
    nb_03()
    nb_04()
    nb_05()
    nb_06()
    nb_07()
    nb_08()
    nb_09()
    nb_10()
    # Clean up old checkpoints (stale outputs from prior runs).
    ckpt_dir = NB_DIR / ".ipynb_checkpoints"
    if ckpt_dir.exists():
        for f in ckpt_dir.glob("*.ipynb"):
            f.unlink()
        try:
            ckpt_dir.rmdir()
        except OSError:
            pass


if __name__ == "__main__":
    main()
