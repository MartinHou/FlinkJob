import logging
from datetime import datetime
import json
from pyflink.common import (
    Types, )
from pyflink.datastream import (
    StreamExecutionEnvironment,
    FilterFunction,
)
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaProducer

FLINK_SQL_CONNECTOR_KAFKA_LOC = '/home/martin.hou/flink-sql-connector-kafka-1.15.4.jar'
KAFKA_TOPIC_OF_NODE_MONITOR = 'ars_prod_node_monitor'
KAFKA_SERVERS = "10.10.2.224:9092,10.10.2.81:9092,10.10.3.141:9092"
KAFKA_CONSUMUER_GOURP_ID = "ars_prod"
KAFKA_PRODUCER_GOURP_ID = "ars_prod"
KAFKA_TOPIC_OF_ARS_BAG_CRASH = 'ars_prod_bag_crash_result'
KAFKA_TOPIC_OF_ARS_BAG = 'ars_prod_bag_result'
START_TIME = datetime(2023, 10, 14, 0, 0, 0)
TEST_ARS_BAG_SCHEMA = {
    'result_id': Types.STRING(),
    'pod_id': Types.STRING(),
    'job_id': Types.STRING(),
    'type': Types.STRING(),
    'node_name': Types.STRING(),
    'cluster_name': Types.STRING(),
    'status': Types.STRING(),
    'pod_start_timestamp': Types.FLOAT(),
    'playback_start_timestamp': Types.FLOAT(),
    'playback_end_timestamp': Types.FLOAT(),
    'device': Types.STRING(),
    'device_num': Types.INT(),
    'priority': Types.INT(),
    'group': Types.STRING(),
    'error_type': Types.STRING(),
    'error_details': Types.STRING(),
    'error_stage': Types.STRING(),
    'log': Types.STRING(),
    'data_source': Types.STRING(),
    'input_bag': Types.STRING(),
    'output_bag': Types.STRING(),
    'metric': Types.STRING(),
    'release': Types.STRING(),
    'coredump': Types.STRING(),
    'backtrace': Types.STRING(),
    'final_attempt': Types.BOOLEAN(),
    'config': Types.STRING(),
}
KEYS = [k for k in TEST_ARS_BAG_SCHEMA.keys()]
VALUES = [TEST_ARS_BAG_SCHEMA[k] for k in KEYS]
serialization_schema = JsonRowDeserializationSchema.Builder() \
    .type_info(Types.ROW_NAMED(KEYS, VALUES)) \
    .build()
        
logger = logging.getLogger(__name__)

# class Flatten(FlatMapFunction):
#     def flat_map(self, value):
#         producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS)
#         producer.send(
#             KAFKA_TOPIC_OF_ARS_BAG_CRASH,
#             value=json.dumps(named_tuple_to_dict(value)).encode('utf-8'))
#         yield value


def named_tuple_to_dict(nt):
    return {field: getattr(nt, field) for field in nt._fields}


class Filter(FilterFunction):
    def filter(self, value):
        # print(value.type,value.status,value.coredump)
        # if value.type=='replay'and value.status=='FAILURE' and value.coredump is not None:
        print(value.result_id, value.status, value.pod_start_timestamp)
        if value.type == 'replay' and value.status == 'FAILURE' and value.backtrace is not None:
            return True
        else:
            return False


def read_from_kafka():
    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC_OF_ARS_BAG,
        deserialization_schema=serialization_schema,
        properties={
            'bootstrap.servers': KAFKA_SERVERS,
            'group.id': KAFKA_CONSUMUER_GOURP_ID,
        })

    # date_object = START_TIME
    # date_int = int(int(time.mktime((date_object).timetuple())) * 1000)
    # kafka_consumer.set_start_from_timestamp(date_int)

    return kafka_consumer


def analyse(env: StreamExecutionEnvironment):
    stream = env.add_source(read_from_kafka())
    result = stream.filter(Filter()).map(
        lambda x: json.dumps(named_tuple_to_dict(x)), 
        output_type=Types.STRING(),
    ).add_sink(
        sink_func=FlinkKafkaProducer(
            topic=KAFKA_TOPIC_OF_ARS_BAG_CRASH, 
            serialization_schema=SimpleStringSchema(), 
            producer_config={
                'bootstrap.servers': KAFKA_SERVERS,
                'group.id': KAFKA_PRODUCER_GOURP_ID,   
            },
        ),
    )


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute("bag_crash")
