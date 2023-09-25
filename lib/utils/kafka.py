from pyflink.common import Types
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from lib.common.settings import KAFKA_SERVERS


def get_flink_kafka_consumer(schema, topic, group_id, start_date=None):
    KEYS = [k for k in schema.keys()]
    VALUES = [schema[k] for k in KEYS]
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(Types.ROW_NAMED(KEYS, VALUES)) \
        .build()
    kafka_consumer = FlinkKafkaConsumer(
        topics=topic,
        deserialization_schema=deserialization_schema,
        properties={
            'bootstrap.servers': KAFKA_SERVERS,
            'group.id': group_id,
        })
    if start_date:
        date_object = start_date
        kafka_consumer.set_start_from_timestamp(
            int(date_object.timestamp()) * 1000)
    return kafka_consumer
