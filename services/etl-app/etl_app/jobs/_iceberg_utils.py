"""Iceberg helpers shared by payments medallion jobs."""
from __future__ import annotations

from pyiceberg.catalog import Catalog
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
    if partition_spec is None:
        return catalog.create_table(identifier=identifier, schema=schema)
    return catalog.create_table(
        identifier=identifier, schema=schema, partition_spec=partition_spec,
    )
