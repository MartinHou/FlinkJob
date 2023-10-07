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
    GRM_HOST,
    ARS_DEV_HOST,
    ARS_API_ROOT_TOKEN,
    KAFKA_TOPIC_OF_JOB_MONITOR,
)
from pyflink.datastream.window import SlidingProcessingTimeWindows
from lib.utils.kafka import get_flink_kafka_consumer
from lib.common.schema import POD_ERR_SCHEMA
import requests
from lib.utils.utils import http_request

WINDOW_SIZE = Time.minutes(5)
WINDOW_SLIDE = Time.seconds(10)


class PodErrMonitor(ProcessWindowFunction):
    def __init__(self, window_size, window_slide):
        self.window_size = window_size
        self.window_slide = window_slide
        self.last_warn_timestamp = None  # in seconds
        self.retried_pods = None  # avoid duplicate retry
        self.cordoned_nodes = None  # avoid duplicate cordon

        self.warning_api = ARS_HOST + "/api/v1/notification/warn/"
        self.warning_chat_group = "oc_d29ae06fec6bc5d6a35583157cea6285"
        self.warning_assignees = [
            "ou_0c135f719351847da272c21880f9b96f",
            "ou_cb867b8fedad86c53850ad776877aba7"
        ]
        self.warning_assignees_str = " ".join(
            [self.assign_someone(user) for user in self.warning_assignees])
        self.mq_message_source = "ars_pod_err_monitor"
        self.mq_message_type = "ARSPodErrMonitor"

    def open(self, runtime_context: RuntimeContext):
        self.last_warn_timestamp_descriptor = ValueStateDescriptor(
            "last_warn_timestamp", Types.INT())
        self.last_warn_timestamp = runtime_context.get_state(
            self.last_warn_timestamp_descriptor)
        self.retried_pods_descriptor = MapStateDescriptor(
            "retried_pods", Types.STRING(), Types.BOOLEAN())
        self.retried_pods = runtime_context.get_map_state(
            self.retried_pods_descriptor)
        self.cordoned_nodes_descriptor = MapStateDescriptor(
            "cordoned_nodes", Types.STRING(), Types.BOOLEAN())
        self.cordoned_nodes = runtime_context.get_map_state(
            self.cordoned_nodes_descriptor)

    def process(self, key, context, elements):
        if len(elements) < 5:
            yield 'normal: ' + str(len(elements))
        else:
            node_name = key
            workflows = []
            for ele in elements:
                pod_name = self.parse_pod_name(ele.job_name)
                if not self.retried_pods.contains(pod_name):
                    workflows.append(pod_name)
                    self.retried_pods.put(pod_name, True)
            # cordon node
            if not self.cordoned_nodes.contains(node_name):
                print(f'cordon: {node_name}')
                self.cordoned_nodes.put(node_name, True)
                body = {
                    "cluster_name": "ddinfra-prod",
                    "node_name": node_name,
                    "operation": "cordon"
                }
                try:
                    response = http_request(
                        method='PATCH',
                        url=GRM_HOST + '/api/v1/cluster/node',
                        data=body,
                        headers={
                            'Authorization': 'Token ' + ARS_API_ROOT_TOKEN
                        })
                    if response.status_code == 200:
                        self.cordoned_nodes.put(node_name, True)
                except Exception as e:
                    print(f'error: {e}')

            # retry pods
            if workflows:
                print(f'提升优先级到3: {workflows}')
                try:
                    http_request(
                        method='PUT',
                        url=ARS_HOST +
                        '/api/v1/driver/workflow/improve_priority/3',  # TODO: change to ARS_HOST
                        data={'workflow_id__in': workflows},
                        headers={
                            'Authorization': 'Token ' + ARS_API_ROOT_TOKEN
                        })
                except Exception as e:
                    print(f"error: {e}")

            if self.last_warn_timestamp.value() is not None \
                    and datetime.now() - datetime.fromtimestamp(self.last_warn_timestamp.value()) <= timedelta(minutes=5):
                yield 'cooling, last: ' + datetime.strftime(
                    datetime.fromtimestamp(self.last_warn_timestamp.value()),
                    '%Y-%m-%d %H:%M:%S')
            else:
                print(
                    f"通知: {node_name}在{datetime.fromtimestamp(float(elements[0].timestamp)/1000)}"
                )
                if self.last_warn_timestamp.value() is not None:
                    self.retried_pods.clear()
                    self.cordoned_nodes.clear()
                try:
                    requests.post(url=ARS_HOST+'/api/v1/notification/warn/',json={
                        "user_id": self.warning_chat_group,
                        "info": f"FAULT! GPU error on machine {node_name} " + \
                                f"in cluter ddinfra-prod found at " + \
                                f"{datetime.fromtimestamp(float(elements[0].timestamp)/1000)}. " + \
                                self.warning_assignees_str,
                        "user_type": "chat",
                        "source": self.mq_message_source,
                        "level": "fault",
                        "type": self.mq_message_type,
                        "message_id": "1"
                    })
                except Exception as e:
                    print(f"error: {e}")
                self.last_warn_timestamp.update(datetime.now().timestamp())
                yield 'alert: ' + datetime.strftime(
                    datetime.fromtimestamp(self.last_warn_timestamp.value()),
                    '%Y-%m-%d %H:%M:%S')

    def assign_someone(self, user: str) -> str:
        return f'<at open_id=\"{user}\"> </at>'

    def parse_pod_name(self, job_name: str) -> str:
        return job_name.split('-')[3]


def monitor(env: StreamExecutionEnvironment):
    ds = env.add_source(
        get_flink_kafka_consumer(
            schema=POD_ERR_SCHEMA,
            topic=KAFKA_TOPIC_OF_JOB_MONITOR,
            group_id="ars_prod"))

    ds.filter(lambda x: x.job_status!='successed' and x.job_name.split('-')[4]=='replay') \
        .key_by(lambda x: x['node_name']) \
        .window(SlidingProcessingTimeWindows.of(WINDOW_SIZE,WINDOW_SLIDE))\
        .process(PodErrMonitor(WINDOW_SIZE,WINDOW_SLIDE))\
        # .print()


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)

    monitor(env)

    env.execute()
