"""Iceberg helpers shared by medallion jobs."""
from __future__ import annotations

import random
import time

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import CommitFailedException, TableAlreadyExistsError
from pyiceberg.expressions import BooleanExpression
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table

from etl_app.catalog import ensure_namespace


def load_or_create(
    catalog: Catalog,
    identifier: tuple[str, str],
    schema: Schema,
    partition_spec: PartitionSpec | None = None,
) -> Table:
    if catalog.table_exists(identifier):
        return catalog.load_table(identifier)
    ensure_namespace(catalog, identifier[0])
    try:
        if partition_spec is None:
            return catalog.create_table(identifier=identifier, schema=schema)
        return catalog.create_table(
            identifier=identifier, schema=schema, partition_spec=partition_spec,
        )
    except TableAlreadyExistsError:
        # check-then-create race, or pyiceberg's tenacity retry re-submitting a
        # create that already succeeded server-side (response lost) and 409s on
        # retry. Either way the table now exists — just load it.
        return catalog.load_table(identifier)


def write_with_retry(
    catalog: Catalog,
    identifier: tuple[str, str],
    schema: Schema,
    partition_spec: PartitionSpec | None,
    arrow: pa.Table,
    mode: str,  # "snapshot" | "incremental"
    overwrite_filter: BooleanExpression | str | None = None,
    attempts: int = 6,
) -> Table:
    """Write `arrow` to an Iceberg table, retrying on Nessie commit conflicts.

    Parallel bronze tasks each commit to the same Nessie branch; Nessie
    serializes branch commits, so simultaneous writes raise
    CommitFailedException. pyiceberg's own retry resubmits with stale
    requirements and cannot recover, so we retry here — reloading the table
    each attempt so the commit rebases on the latest metadata — with a
    jittered backoff to break up the conflict storm.

    Snapshot overwrite is idempotent. Incremental append may double-write on
    the rare lost-response case, which downstream dbt staging dedups by PK
    (latest ingest_ts wins), so the silver/gold layers stay correct.
    """
    last_exc: CommitFailedException | None = None
    for i in range(attempts):
        table = load_or_create(catalog, identifier, schema, partition_spec)
        try:
            if mode == "snapshot":
                table.overwrite(arrow, overwrite_filter=overwrite_filter)
            else:
                table.append(arrow)
            return table
        except CommitFailedException as exc:
            last_exc = exc
            # linear backoff with jitter; later attempts wait longer
            time.sleep(0.5 * (i + 1) + random.uniform(0, 0.5))
    raise last_exc
