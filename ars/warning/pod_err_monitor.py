from datetime import datetime, timedelta
from pyflink.datastream import (
    StreamExecutionEnvironment,
    RuntimeContext,
    ProcessWindowFunction,
)
from pyflink.common import Types as PyTypes
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types as InfoTypes
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from pyflink.datastream.window import SlidingProcessingTimeWindows
from typing import Optional, List
import requests

"""
This job does not depend on outside files to avoid bugs, though redundant.
"""

WINDOW_SIZE = Time.minutes(5)
WINDOW_SLIDE = Time.seconds(10)
KAFKA_SERVERS = '10.10.2.224:9092,10.10.2.81:9092,10.10.3.141:9092'
# FLINK_SQL_CONNECTOR_KAFKA_LOC = '/mnt/data/userdata/martin.hou/flink-sql-connector-kafka-1.15.4.jar'
KAFKA_TOPIC_OF_JOB_MONITOR = 'ars_prod_job_monitor'

POD_ERR_SCHEMA = {
    "node_name": PyTypes.STRING(),
    "job_status": PyTypes.STRING(),
    "job_name": PyTypes.STRING(),
    "timestamp": PyTypes.STRING(),
}


def http_request(
        method: str,
        url: str,
        data: Optional[dict] = None,
        json: Optional[dict] = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
        skip: Optional[List[int]] = None,
):

    r = requests.request(
        method=method,
        url=url,
        data=data,
        json=json,
        params=params,
        headers=headers,
        timeout=timeout,
    )
    if skip and r.status_code in skip:
        return r
    if r.status_code >= 400:
        print(
            f"Failed to {method.upper()} -> '{url}', status code: {r.status_code}, error: {r.text}"
        )
    r.raise_for_status()
    print(
        f"Success {method.upper()} -> '{url}' with status code: {r.status_code}"
    )
    return r


def get_flink_kafka_consumer(schema, topic, group_id, start_date=None):
    KEYS = [k for k in schema.keys()]
    VALUES = [schema[k] for k in KEYS]
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(PyTypes.ROW_NAMED(KEYS, VALUES)) \
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
    else:
        kafka_consumer.set_start_from_latest()
    return kafka_consumer


class PodErrMonitor(ProcessWindowFunction):
    GRM_HOST = 'https://grm.momenta.works'
    ARS_HOST = 'https://ars.momenta.works'
    ARS_API_ROOT_TOKEN = '8e4c872d-b688-4900-83b1-b28a8efd4001'
    mq_message_source = "ars_pod_err_monitor"
    mq_message_type = "ARSPodErrMonitor"
    warning_api = ARS_HOST + "/api/v1/notification/warn/"
    warning_chat_group = "oc_d29ae06fec6bc5d6a35583157cea6285"  # [ SRE & ARS ] ARS 生产集群讨论群
    warning_assignees = [
        "ou_0c135f719351847da272c21880f9b96f",  # peter.huang
        "ou_cb867b8fedad86c53850ad776877aba7"   # hanwen.qiu
    ]

    def __init__(self, window_size, window_slide):
        self.window_size = window_size
        self.window_slide = window_slide
        self.last_warn_timestamp = None  # in seconds
        self.retried_pods = None  # avoid duplicate retry
        self.cordoned_nodes = None  # avoid duplicate cordon
        self.warning_assignees_str = " ".join(
            [self.assign_someone(user) for user in self.warning_assignees])

    def open(self, runtime_context: RuntimeContext):
        self.last_warn_timestamp_descriptor = ValueStateDescriptor(
            "last_warn_timestamp", InfoTypes.INT())
        self.last_warn_timestamp = runtime_context.get_state(
            self.last_warn_timestamp_descriptor)
        self.retried_pods_descriptor = MapStateDescriptor(
            "retried_pods", InfoTypes.STRING(), InfoTypes.BOOLEAN())
        self.retried_pods = runtime_context.get_map_state(
            self.retried_pods_descriptor)
        self.cordoned_nodes_descriptor = MapStateDescriptor(
            "cordoned_nodes", InfoTypes.STRING(), InfoTypes.BOOLEAN())
        self.cordoned_nodes = runtime_context.get_map_state(
            self.cordoned_nodes_descriptor)

    def process(self, key, context, elements):
        if len(elements) < 5:
            yield 'normal: ' + str(len(elements))
        else:
            node_name = key
            print('Found error on node: ' + node_name)
            workflows = []
            print(elements)
            for ele in elements:
                pod_name = self.parse_pod_name(ele.job_name)
                if not self.retried_pods.contains(pod_name):
                    workflows.append(pod_name)
                    self.retried_pods.put(pod_name, True)
            # cordon node
            if not self.cordoned_nodes.contains(node_name):
                print(f'Cordon: {node_name}')
                self.cordoned_nodes.put(node_name, True)
                body = {
                    "cluster_name": "ddinfra-prod",
                    "node_name": node_name,
                    "operation": "cordon"
                }
                try:
                    response = http_request(
                        method='PATCH',
                        url=self.GRM_HOST + '/api/v1/cluster/node',
                        data=body,
                        headers={
                            'Authorization': 'Token ' + self.ARS_API_ROOT_TOKEN
                        })
                    if response.status_code == 200:
                        self.cordoned_nodes.put(node_name, True)
                except Exception as e:
                    print(f'Cordon failed for {node_name}: {e}')

            # retry pods
            if workflows:
                print(
                    f'Improve priority to 3 from node {node_name}: {workflows}'
                )
                try:
                    http_request(
                        method='PUT',
                        url=self.ARS_HOST +
                        '/api/v1/driver/workflow/improve_priority/3',
                        data={'workflow_id__in': workflows},
                        headers={
                            'Authorization': 'Token ' + self.ARS_API_ROOT_TOKEN
                        })
                except Exception as e:
                    print(
                        f"Improve priority failed from node {node_name}, error: {e}"
                    )

            if self.last_warn_timestamp.value() is not None \
                    and datetime.fromtimestamp(float(elements[0].timestamp)/1000) - datetime.fromtimestamp(self.last_warn_timestamp.value()) <= timedelta(minutes=5):
                print(f'cooling {node_name}, last: ' + datetime.strftime(
                    datetime.fromtimestamp(self.last_warn_timestamp.value()),
                    '%Y-%m-%d %H:%M:%S'))
                yield 'cooling, last: ' + datetime.strftime(
                    datetime.fromtimestamp(self.last_warn_timestamp.value()),
                    '%Y-%m-%d %H:%M:%S')
            else:
                print(
                    f"Send warning: {node_name} at {datetime.fromtimestamp(float(elements[0].timestamp)/1000)}"
                )
                if self.last_warn_timestamp.value() is not None:
                    self.retried_pods.clear()
                    self.cordoned_nodes.clear()
                try:
                    requests.post(url=self.ARS_HOST+'/api/v1/notification/warn/',json={
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
                    print(
                        f"Failed to send warning, node:{node_name}. Err: {e}")
                self.last_warn_timestamp.update(
                    float(elements[-1].timestamp) / 1000)
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
        .print()


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)

    monitor(env)

    env.execute("GPU_err_monitor")
