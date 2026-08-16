from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.common import SimpleStringSchema, WatermarkStrategy
import json
from pyflink.common.typeinfo import Types
from pipeline.parsers.k8s_audit import parse_audit_event
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema

BROKERS = "kafka:9092"
SOURCE_TOPIC = "k8s-audit-raw"
GROUP_ID = "flink-k8s-audit"
SINK_TOPIC = "k8s-audit-ecs"

#Flink and parser wiring (k8s audit events)


def to_ecs(line: str) -> str | None:
    try:
        doc = parse_audit_event(json.loads(line))
    except json.JSONDecodeError:
        return None
    if doc is None:
        return None
    return json.dumps(doc)













def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BROKERS)
        .set_topics(SOURCE_TOPIC)
        .set_group_id(GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "k8s-audit-raw")

    #Linking the parser logic to the stream
    ecs = (
        stream
        .map(to_ecs, output_type=Types.STRING())
        .filter(lambda x: x is not None)
    )

    #Sending it to the sink topic which then connects to elasticsearch SIEM
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(SINK_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    ecs.sink_to(sink)

    env.execute("k8s-audit-ecs")


if __name__ == "__main__":
    main()