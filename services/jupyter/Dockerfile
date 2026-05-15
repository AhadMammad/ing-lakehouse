# ─────────────────────────────────────────────────────────────────
#  Jupyter — Lakehouse notebook image
#  Base: quay.io/jupyter/scipy-notebook:python-3.11  (official)
#  Added: pyiceberg  Iceberg table format + REST catalog + S3 FileIO
#         polars     fast DataFrame library
#         boto3      S3 SDK used internally by pyiceberg/s3fs
#         pyspark    drives Spark-side maintenance from notebook 10
#         requests   Nessie API v2 calls in notebook 08
# ─────────────────────────────────────────────────────────────────

FROM quay.io/jupyter/scipy-notebook:python-3.11

# scipy-notebook already includes curl — no apt-get needed

# Layer 1 — lakehouse python deps (small, fast). Pin tight.
RUN mkdir -p /opt/conda/pip-tmp && \
    TMPDIR=/opt/conda/pip-tmp pip install --no-cache-dir --timeout=120 --retries=5 \
        "pyiceberg[s3fs,pyarrow]==0.9.1" \
        "polars==1.9.0" \
        "boto3==1.35.0" \
        "requests==2.32.3" && \
    rm -rf /opt/conda/pip-tmp

# Layer 2 — PySpark (drives notebook 10's maintenance ops).
# Separate layer because the 317 MB sdist is the failure-prone download;
# splitting means a retry doesn't redo the deps above.
RUN mkdir -p /opt/conda/pip-tmp && \
    TMPDIR=/opt/conda/pip-tmp pip install --no-cache-dir --timeout=600 --retries=10 \
        "pyspark==3.5.3" && \
    rm -rf /opt/conda/pip-tmp

# Layer 3 — Java runtime for PySpark.
# scipy-notebook ships with no JDK; PySpark needs one to launch its JVM gateway.
USER root
RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    ln -sfn "$(dirname $(dirname $(readlink -f $(which java))))" /usr/lib/jvm/default-java
USER ${NB_UID}
ENV JAVA_HOME=/usr/lib/jvm/default-java
