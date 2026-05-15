"""Drop the demo.events table via the Nessie REST catalog.

Invoked by `make reset-events-table` inside the Jupyter container, where the
pyiceberg + Nessie env vars are already present.
"""
import os

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError

catalog = RestCatalog(
    name="nessie",
    **{
        "uri": os.environ["NESSIE_URI"],
        "warehouse": f"s3://{os.environ['ICEBERG_WAREHOUSE_BUCKET']}/warehouse",
        "s3.endpoint": os.environ["AWS_S3_ENDPOINT"],
        "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
        "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    },
)

try:
    catalog.drop_table(("demo", "events"))
    print("Dropped demo.events.")
except NoSuchTableError:
    print("demo.events did not exist — nothing to drop.")
