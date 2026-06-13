# ─────────────────────────────────────────────────────────────────
#  etl-app-spark — custom Spark image for the auth medallion pipeline.
#
#  FROM apache/spark:3.5.3, baking in:
#    * Iceberg runtime + AWS bundle (table data via S3FileIO)
#    * hadoop-aws + aws-java-sdk-bundle (Spark event logs via s3a)
#    * postgresql JDBC driver (bronze source read)
#    * spark-defaults.conf (catalog + s3a + eventLog config)
#    * the etl_app_spark package (on PYTHONPATH)
#
#  Used by spark-master, spark-worker-*, and spark-history-server so the
#  driver (client mode, in master) and executors (workers) are identical.
#  Built per-instance and tagged ${COMPOSE_PROJECT_NAME}-spark:latest
#  (see services/spark/docker-compose.*.yml — the build context points here).
#
#  Run a job (via the auth_etl DAG's `docker exec` into spark-master):
#    spark-submit --master spark://spark-master:7077 --deploy-mode client \
#      -m etl_app_spark --job bronze --table users --date 2026-05-01
# ─────────────────────────────────────────────────────────────────

FROM apache/spark:3.5.3

USER root

# Pinned to Spark 3.5.x / Hadoop 3.3.4 / Scala 2.12. Mismatching hadoop-aws
# vs the bundled Hadoop (3.3.4) causes NoClassDefFound at s3a init (SPARK-45676).
ARG ICEBERG_VERSION=1.9.2
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_V1_VERSION=1.12.262
ARG PG_JDBC_VERSION=42.7.4

ENV JARS=/opt/spark/jars
ENV MAVEN=https://repo1.maven.org/maven2

RUN set -eux; \
    curl -fSL -o "$JARS/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      "$MAVEN/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    curl -fSL -o "$JARS/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "$MAVEN/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fSL -o "$JARS/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "$MAVEN/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fSL -o "$JARS/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar" \
      "$MAVEN/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar"; \
    curl -fSL -o "$JARS/postgresql-${PG_JDBC_VERSION}.jar" \
      "$MAVEN/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY src/etl_app_spark /opt/etl_app_spark/etl_app_spark

ENV PYTHONPATH=/opt/etl_app_spark

USER spark
