# ─────────────────────────────────────────────────────────────────
#  Jupyter — Lakehouse notebook image
#  Base: quay.io/jupyter/scipy-notebook:python-3.11  (official)
#  Added: pyiceberg (Iceberg table format + REST catalog + S3 FileIO)
#         polars     (fast DataFrame library)
#         boto3      (S3 SDK used internally by pyiceberg/s3fs)
# ─────────────────────────────────────────────────────────────────

FROM quay.io/jupyter/scipy-notebook:python-3.11

# scipy-notebook already includes curl — no apt-get needed

RUN mkdir -p /opt/conda/pip-tmp && \
    TMPDIR=/opt/conda/pip-tmp pip install --no-cache-dir \
        "pyiceberg[s3fs]==0.8.0" \
        "polars==1.9.0" \
        "boto3==1.35.0" && \
    rm -rf /opt/conda/pip-tmp
