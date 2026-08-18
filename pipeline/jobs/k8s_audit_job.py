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













# ============================================================================
# SCAFFOLD - allowlist broadcast-state enrichment (NOT WIRED IN)
#
# Everything below, down to the next "====" line, is inert - commented out
# on purpose. It's a wiring sketch for plugging pipeline.enrich.allowlist's
# apply_allowlist_record / stamp_approval into this job once those two
# functions are actually implemented (see pipeline/enrich/allowlist.py).
# The running pipeline above this block is untouched.
#
# --- new imports this would need ---
# from pyflink.datastream.functions import BroadcastProcessFunction
# from pyflink.datastream.state import MapStateDescriptor
# from pipeline.enrich.allowlist import apply_allowlist_record, stamp_approval
#
# --- the broadcast state descriptor ---
# ALLOWLIST_STATE_DESCRIPTOR = MapStateDescriptor(
#     "k8s-allowlist",
#     Types.STRING(),  # key: "<namespace>:<username>" - same shape as the
#                       # fixture keys in tests/fixtures/allowlist-records.json
#     Types.STRING(),  # value: your call - raw JSON string (let
#                       # apply_allowlist_record parse it), or something
#                       # pre-parsed. Whatever that function actually does
#                       # once written is the source of truth here.
# )
#
# --- second source: the allowlist topic itself ---
# ALLOWLIST_TOPIC = "k8s-allowlist"
#
# allowlist_source = (
#     KafkaSource.builder()
#     .set_bootstrap_servers(BROKERS)
#     .set_topics(ALLOWLIST_TOPIC)
#     .set_group_id(GROUP_ID + "-allowlist")
#     .set_starting_offsets(KafkaOffsetsInitializer.earliest())
#     .set_value_only_deserializer(SimpleStringSchema())
#     .build()
# )
#
# Why earliest(): broadcast state does not survive a job restart by itself -
# MapState here gets rebuilt from a full topic replay every time this job
# starts, the same way built_state() replays tests/fixtures/allowlist-records.json
# in the test file. The allowlist topic needs to be compacted so that replay
# stays cheap as it grows, instead of re-reading years of superseded grants.
#
# Gotcha worth flagging: SimpleStringSchema (used above and on the main
# k8s-audit-raw source) only deserializes the Kafka record VALUE. The
# allowlist's record KEY is the actual "<namespace>:<username>" identity -
# apply_allowlist_record's `key` argument has to come from somewhere, and
# SimpleStringSchema alone won't get it. Likely needs a
# KafkaRecordDeserializationSchema that reads ConsumerRecord.key() instead.
# Not solved here - worth a line in decisions/allowlist_enrichment.txt once
# you pick an approach.
#
# allowlist_stream = env.from_source(
#     allowlist_source, WatermarkStrategy.no_watermarks(), "k8s-allowlist"
# )
# broadcast_stream = allowlist_stream.broadcast(ALLOWLIST_STATE_DESCRIPTOR)
#
# --- where this connects into the existing chain: open tradeoff ---
#
#   Option A - connect onto `stream` (the raw k8s-audit-raw source),
#   BEFORE .map(to_ecs):
#     AllowlistEnrichment would see raw k8s audit JSON, not an ECS doc, so
#     it would need to pull namespace/username out of the raw shape itself
#     (objectRef.namespace / user.username) rather than reusing
#     doc["orchestrator"]["namespace"] / doc["user"]["name"]. More coupling
#     to the raw audit event's field names inside the enrichment step.
#
#   Option B - connect onto `ecs` (AFTER .map(to_ecs).filter(...)),
#   i.e. `ecs.connect(broadcast_stream)`:
#     By this point every element is a JSON *string*, not a dict, because
#     to_ecs ends with json.dumps(). process_element would need to
#     json.loads() it back into a dict before calling stamp_approval, then
#     json.dumps() the result again on the way out. Extra (de)serialization
#     per record, but AllowlistEnrichment only ever deals in ECS shape,
#     matching exactly what stamp_approval's tests assume it receives.
#
#   Neither is chosen here - pick based on whether the raw-shape coupling
#   (A) or the extra json round-trip (B) is the cost you'd rather pay.
#
# enriched = ecs.connect(broadcast_stream).process(AllowlistEnrichment())
#
#
# class AllowlistEnrichment(BroadcastProcessFunction):
#     """Joins ECS audit docs against k8s-allowlist broadcast state.
#
#     process_element runs once per audit doc, on whichever parallel
#     instance it landed on - each instance has its own copy of the
#     broadcast state. process_broadcast_element runs once per allowlist
#     record, on EVERY parallel instance (that's what "broadcast" means) -
#     it's how every instance's copy of the state stays identical.
#     """
#
#     def process_element(self, value, ctx, out):
#         # READ-ONLY STATE RULE:
#         # ctx.get_broadcast_state(...) here returns a read-only view.
#         # Flink enforces this at the API level, not just by convention.
#         # Why: process_element runs independently on N parallel instances.
#         # If one instance could write through process_element, each
#         # instance's copy of the allowlist could drift out of sync with
#         # the others - which one "wins" would depend on processing
#         # order/timing, breaking the determinism Flink promises. Only
#         # process_broadcast_element (guaranteed to see the SAME broadcast
#         # input, in the SAME order, on every instance) is allowed to
#         # write - which is exactly why apply_allowlist_record's mutation
#         # belongs there, and stamp_approval (read-only) belongs here.
#         #
#         # 1. Get `doc` from `value` (see Option A/B above for whether a
#         #    json.loads() is needed first).
#         # 2. state = ctx.get_broadcast_state(ALLOWLIST_STATE_DESCRIPTOR)
#         # 3. out.collect(stamp_approval(doc, state)) - pipeline.enrich
#         #    .allowlist.stamp_approval does the actual exec-only /
#         #    default-deny logic; this method is wiring only.
#         raise NotImplementedError
#
#     def process_broadcast_element(self, value, ctx, out):
#         # This is the ONLY method allowed to mutate broadcast state -
#         # ctx.get_broadcast_state(...) here returns a read-write view.
#         # 1. `value` is one record off the k8s-allowlist topic - see the
#         #    KafkaRecordDeserializationSchema gotcha noted above for how
#         #    `key` actually gets in here; a tombstoned record's value
#         #    deserializes as None.
#         # 2. state = ctx.get_broadcast_state(ALLOWLIST_STATE_DESCRIPTOR)
#         # 3. apply_allowlist_record(state, key, value) - reuse the exact
#         #    same pure function tests/test_allowlist_enrich.py pins down,
#         #    so this method stays wiring-only, no logic of its own.
#         raise NotImplementedError
#
# ============================================================================


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