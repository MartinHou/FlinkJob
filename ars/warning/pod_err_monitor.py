import logging
from datetime import datetime, timedelta

from pyflink.datastream import (
    StreamExecutionEnvironment,
    FlatMapFunction,
    RuntimeContext,
    ProcessFunction,
    ProcessWindowFunction,
)
from pyflink.common.time import Time
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from lib.common.settings import (
    FLINK_SQL_CONNECTOR_KAFKA_LOC,
    ARS_HOST,
    ARS_API_ROOT_TOKEN,
)
from pyflink.datastream.window import SlidingProcessingTimeWindows
from lib.utils.kafka import get_flink_kafka_consumer
from lib.common.schema import POD_ERR_SCHEMA
import requests
from lib.utils.utils import http_request

# WINDOW_SIZE = Time.minutes(5)
# WINDOW_SLIDE = Time.seconds(10)

WINDOW_SIZE = Time.seconds(30)
WINDOW_SLIDE = Time.seconds(3)


class PodErrMonitor(ProcessWindowFunction):
    def __init__(self, window_size, window_slide):
        self.window_size = window_size
        self.window_slide = window_slide
        self.last_warn_timestamp = None  # in seconds
        self.warning_api = ARS_HOST + "/api/v1/notification/warn/"
        self.warning_chat_group = "oc_d29ae06fec6bc5d6a35583157cea6285"
        self.warning_assignees = ["ou_0c135f719351847da272c21880f9b96f"]
        self.warning_assignees_str = " ".join(
            [self.assign_someone(user) for user in self.warning_assignees])
        self.mq_message_source = "ars_pod_err_monitor"
        self.mq_message_type = "ARSPodErrMonitor"

    def open(self, runtime_context: RuntimeContext):
        self.last_warn_timestamp_descriptor = ValueStateDescriptor(
            "last_warn_timestamp", Types.INT())
        self.last_warn_timestamp = runtime_context.get_state(
            self.last_warn_timestamp_descriptor)

    def process(self, key, context, elements):
        if len(elements) < 5:
            yield 'normal: ' + str(len(elements))
        else:
            cluster_name, node_name = key.split('__')
            workflows = [
                self.parse_pod_name(ele['pod_name']) for ele in elements
            ]
            # TODO: cordon

            # try:
            #     http_request(
            #         method='PUT',
            #         url=ARS_HOST + '/api/v1/driver/workflow/retry',
            #         data={'workflow_ids': workflows},
            #         headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
            # except Exception as e:
            #     print(f"error: {e}")
            if self.last_warn_timestamp.value() is not None \
                    and datetime.now() - datetime.fromtimestamp(self.last_warn_timestamp.value()) <= timedelta(minutes=5):
                yield 'coolling, last: ' + datetime.strftime(
                    datetime.fromtimestamp(self.last_warn_timestamp.value()),
                    '%Y-%m-%d %H:%M:%S')
            elif len(elements) >= 5:
                print(
                    f"shoot warning: {len(elements)},{node_name},{cluster_name}"
                )
                # http_request(method='POST', url=ARS_HOST+'/api/v1/notification/warn/',data={
                #     "user_id": self.warning_chat_group,
                #     "info": f"FAULT! GPU error on machine {node_name}" + \
                #             f"in cluter {cluster_name} found at " + \
                #             f"{datetime.fromtimestamp(elements[0].happened_at)}. " + \
                #             self.warning_assignees_str,
                #     "user_type": "chat",
                #     "source": self.mq_message_source,
                #     "level": "fault",
                #     "type": self.mq_message_type,
                #     "message_id": "1"
                # },headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
                self.last_warn_timestamp.update(datetime.now().timestamp())
                yield 'alert: ' + datetime.strftime(
                    datetime.fromtimestamp(self.last_warn_timestamp.value()),
                    '%Y-%m-%d %H:%M:%S')

    def assign_someone(self, user: str) -> str:
        return f'<at open_id=\"{user}\"> </at>'

    def parse_pod_name(self, pod_name: str) -> str:
        return pod_name.split('-')[3]


def monitor(env: StreamExecutionEnvironment):
    ds = env.add_source(
        get_flink_kafka_consumer(
            schema=POD_ERR_SCHEMA, topic="martin_test", group_id="ars_prod"))

    ds.key_by(lambda x: f"{x['cluster_name']}__{x['node_name']}") \
        .window(SlidingProcessingTimeWindows.of(WINDOW_SIZE,WINDOW_SLIDE))\
        .process(PodErrMonitor(WINDOW_SIZE,WINDOW_SLIDE))\
        .print()


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)

    monitor(env)

    env.execute()
