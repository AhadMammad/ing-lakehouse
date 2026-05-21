"""PyIceberg catalog factory — mirrors notebooks/00_setup_catalog.ipynb."""
from __future__ import annotations

from pyiceberg.catalog.rest import RestCatalog

from etl_app.config import (
    NESSIE_URI,
    S3_ENDPOINT,
    S3_KEY,
    S3_SECRET,
    WAREHOUSE_URI,
)


def get_catalog() -> RestCatalog:
    return RestCatalog(
        name="nessie",
        **{
            "uri": NESSIE_URI,
            "warehouse": WAREHOUSE_URI,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_KEY,
            "s3.secret-access-key": S3_SECRET,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def ensure_namespace(catalog: RestCatalog, namespace: str) -> None:
    if (namespace,) not in catalog.list_namespaces():
        catalog.create_namespace(namespace)
