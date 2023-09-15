from datetime import datetime
import time
from pyflink.common import Types
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from common.settings import KAFKA_SERVERS


def get_flink_kafka_consumer(schema, topic, group_id, start_date):
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
    date_object = start_date
    kafka_consumer.set_start_from_timestamp(
        int(date_object.timestamp()) * 1000)
    return kafka_consumer


def datetime_str_to_int(datetime_str: str) -> int:
    datetime_YMD = datetime_str.split(' ')[0]
    datetime_object = datetime.strptime(datetime_YMD, "%Y-%m-%d")
    date_int = int(time.mktime((datetime_object).timetuple()))
    return date_int


def time_str_to_int(time_str: str) -> int:
    time_object = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    time_int = int(time.mktime((time_object).timetuple()))
    return time_int


def datetime_obj_to_int(datetime) -> int:
    time_int = int(time.mktime((datetime).timetuple()))
    return time_int


def timestr_to_datestr(time_str: str) -> str:
    return time_str.split(' ')[0]


def timestr_to_minute_int(time_str: str) -> int:
    time_object = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    time_minute = time_object.replace(second=0, microsecond=0)
    time_int = int(time.mktime((time_minute).timetuple()))
    return time_int


def timestr_to_minutestr(time_str: str) -> str:
    time_object = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    time_minute = time_object.replace(second=0, microsecond=0)
    return str(time_minute)
