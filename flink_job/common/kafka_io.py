from __future__ import annotations

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)


def kafka_source(topic: str, bootstrap_servers: str, group_id: str, starting_offsets: str) -> KafkaSource:
    """Create a Kafka source for string events."""
    offsets_initializer = KafkaOffsetsInitializer.earliest()
    if starting_offsets == "latest":
        offsets_initializer = KafkaOffsetsInitializer.latest()

    return (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(offsets_initializer)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def kafka_sink(topic: str, bootstrap_servers: str) -> KafkaSink:
    """Create a Kafka sink for string events."""
    serializer = KafkaRecordSerializationSchema.builder().set_topic(topic).set_value_serialization_schema(SimpleStringSchema()).build()
    return KafkaSink.builder().set_bootstrap_servers(bootstrap_servers).set_record_serializer(serializer).set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE).build()
