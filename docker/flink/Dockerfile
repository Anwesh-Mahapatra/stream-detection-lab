# Extends the official Flink image with Python + PyFlink + the Kafka connector jar.
# Needed because: the stock flink image has no Python at all, and PyFlink's Kafka
# source/sink is backed by a Java connector that isn't a pip package - it has to be
# a jar on Flink's classpath. Used for both jobmanager and taskmanager (see
# docker-compose.yml x-flink-common) because UDFs run on the taskmanager, not just
# the client that submits the job.

ARG FLINK_IMAGE_TAG
FROM flink:${FLINK_IMAGE_TAG}

ARG FLINK_VERSION

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir apache-flink==${FLINK_VERSION}

# flink-sql-connector-kafka version string is "<connector-version>-<flink-minor>",
# versioned separately from Flink itself since connectors split out of Flink core.
# 3.2.0-1.18 is the connector build matching Flink 1.18.x - if you bump
# FLINK_IMAGE_TAG to a new minor version, this URL needs to change too, or the job
# will fail at submit time with a ClassNotFoundException for the Kafka connector.
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.18/flink-sql-connector-kafka-3.2.0-1.18.jar \
    /opt/flink/lib/flink-sql-connector-kafka.jar

# No Elasticsearch connector jar here on purpose - see README "known trade-offs":
# Flink's official ES connector only ships an elasticsearch7/elasticsearch8 build,
# and matching one against Flink 1.18 + your ES 9.x cluster is an open question,
# not a solved one. Resolve that before pipeline/jobs/k8s_audit_job.py's ES-sink
# side can work.
