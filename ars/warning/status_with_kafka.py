import logging
import datetime

from pyflink.datastream import (
    StreamExecutionEnvironment, 
    FlatMapFunction, 
    RuntimeContext,
)
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.state import MapStateDescriptor

import requests

logger = logging.getLogger(__name__)


NODE_MONITOR_MSG_SCHEMA = {
    "cluster_name": Types.STRING(),
    "node_name": Types.STRING(),
    "msg": Types.STRING(),
    "msg_level": Types.STRING(),
    "fault_type": Types.STRING(),
    "timestamp": Types.DOUBLE(),
}
KAFKA_TOPIC_OF_NODE_MONITOR = 'ars_prod_node_monitor'
KAFKA_SERVERS = "10.10.2.224:9092,10.10.2.81:9092,10.10.3.141:9092"
KAFKA_CONSUMUER_GOURP_ID = "ars_prod"


class NodeMonitor(FlatMapFunction):
    def __init__(self) -> None:
        self.fault_timer = None
        self.fault_timer_descriptor_name = "fault_timer"
        self.warning_api = "https://ars-dev.ddinfra.momenta.works/api/v1/notification/warn/"
        self.mq_message_source = "ars_node_monitor"
        self.mq_message_type = "ARSNodeMonitor"
        self.warning_chat_group = "oc_d29ae06fec6bc5d6a35583157cea6285"
        self.warning_assignees = ["ou_0c135f719351847da272c21880f9b96f"]
        self.warning_assignees_str = " ".join([self.assign_someone(user) for user in self.warning_assignees])
    
    def open(self, context: RuntimeContext) -> None:
        # fault_time is a map state, key is fault type, value is timestamp
        descriptor = MapStateDescriptor(
            name=self.fault_timer_descriptor_name, 
            key_type_info=Types.STRING(), 
            value_type_info=Types.DOUBLE(),
        )
        self.fault_timer = context.get_map_state(descriptor)
        
    def flat_map(self, value):
        try:
            if value.msg_level == "fault":
                st_time = self.fault_timer.get(value.fault_type)
                if st_time is None:
                    self.fault_timer.put(value.fault_type, value.timestamp)
                    # TODO: send alert
                    requests.post(
                        url=self.warning_api,
                        json={
                            "user_id": self.warning_chat_group,
                            "info": f"FAULT! {value.fault_type} on machine {value.node_name}" + \
                                    f"in cluter {value.cluster_name} found at " + \
                                    f"{datetime.datetime.fromtimestamp(value.timestamp)}. " + \
                                    self.warning_assignees_str,
                            "user_type": "chat",
                            "source": self.mq_message_source,
                            "level": "fault",
                            "type": self.mq_message_type,
                            "message_id": "1"
                        }
                    )
            elif value.msg_level == 'health':
                st_time = self.fault_timer.get(value.fault_type)
                if st_time is not None:
                    fault_duration = value.timestamp - st_time
                    # TODO: send alert
                    requests.post(
                        url=self.warning_api,
                        json={
                            "user_id": self.warning_chat_group,
                            "info": f"Recovery! {value.fault_type} on machine {value.node_name} " + \
                                    f"in cluter {value.cluster_name} recovered at " + \
                                    f"{datetime.datetime.fromtimestamp(value.timestamp)}. Fault " + \
                                    f"duration: {int(fault_duration / 60)} minutes.",
                            "user_type": "chat",
                            "source": self.mq_message_source,
                            "level": "recovery",
                            "type": self.mq_message_type,
                            "message_id": "1"
                        }
                    )
                    self.fault_timer.remove(value.fault_type)
            else:
                logger.error('Unknown msg_level: %s', value.msg_level)
        except Exception as e:
            logger.error('Error: %s', str(e)) 
            
        yield value

    def assign_someone(self, user: str) -> str:
        return f'<at open_id=\"{user}\"> </at>'


def monitor_node(env: StreamExecutionEnvironment):
    KEYS = [k for k in NODE_MONITOR_MSG_SCHEMA]
    VALUES = [NODE_MONITOR_MSG_SCHEMA[k] for k in NODE_MONITOR_MSG_SCHEMA]
    schema = JsonRowDeserializationSchema.builder().type_info(Types.ROW_NAMED(KEYS, VALUES)).build()
    kafka_consumer = FlinkKafkaConsumer(
        topics=[KAFKA_TOPIC_OF_NODE_MONITOR],
        deserialization_schema=schema,
        properties={
            "bootstrap.servers": KAFKA_SERVERS,
            "group.id": KAFKA_CONSUMUER_GOURP_ID,
        },
    )

    ds = env.add_source(kafka_consumer)
    ds.key_by(lambda row: (row.cluster_name, row.node_name)) \
        .flat_map(NodeMonitor()) \
        .print()


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file:///home2/hanwen.qiu/dev/ars/flink-jobs/flink-sql-connector-kafka-1.15.4.jar")

    monitor_node(env)
    
    env.execute()
    