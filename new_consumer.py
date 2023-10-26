import logging
import json
from pyflink.common import (
    Types, )
from pyflink.datastream import (
    StreamExecutionEnvironment,
    FlatMapFunction,
    RuntimeContext,
    MapFunction,
    ReduceFunction,
)
import pandas as pd
from pyflink.datastream.state import MapStateDescriptor
from lib.utils.sql import StatisticsActions
from lib.common.settings import *
from lib.utils.kafka import get_flink_kafka_consumer
from lib.utils.dates import *
from lib.utils.utils import merge_dicts
from lib.common.schema import TEST_ARS_WORKFLOW_SCHEMA, TEST_ARS_BAG_SCHEMA

logger = logging.getLogger(__name__)


class AddPodCount(MapFunction):
    def map(self, value):
        return {
            'value': value,
            'count_failure': 1 if value['workflow_status'] == 'FAILURE' else 0,
            'count_success': 1 if value['workflow_status'] == 'SUCCESS' else 0
        }



def analyse(env: StreamExecutionEnvironment):

    stream = env.add_source(
        get_flink_kafka_consumer(
            # schema=TEST_ARS_WORKFLOW_SCHEMA,
            # topic=KAFKA_TOPIC_OF_ARS_WORKFLOW,
            schema=TEST_ARS_BAG_SCHEMA,
            topic=KAFKA_TOPIC_OF_ARS_BAG,
            group_id='test',
            start_date=datetime(2023, 10, 24)))
    
    # count_stream = stream.filter(lambda x: x.update_time >= '2023-10-22 00:00:00' and x.update_time < '2023-10-24 00:00:00' and x.workflow_type=='replay').map(OneMap(), output_type=Types.INT())
    # count_result = count_stream.key_by(lambda x: "constant").reduce(SumReduce()).print()
    # stream.filter(lambda x:x.workflow_id=='eab4e75db1824a7a96a0a15f47ee1253').flat_map(AddBagCount()).print()
    # stream.filter(lambda x:x.workflow_id=='352d161526da4098954bcfcf31122bf0').map(OneMap())
    stream.filter(lambda value:value.type=='replay' and value['output_bag']!='').map(OneMap())#.flat_map(AddBagCount()).print()
class OneMap(MapFunction):
    def map(self, value):
        # with open('mq_1024_00-01.txt','+a') as f:
        #     f.write(value.workflow_id + '\n')
        if 'bag_duration' not in json.loads(value['metric']):
            print(value.result_id)
        return value

class SumReduce(ReduceFunction):
    def reduce(self, value1, value2):
        return value1 + value2
    
class AddBagCount(FlatMapFunction):
    def flat_map(self, value):
        workflow_output = json.loads(value.workflow_output)
        workflow_status = value.workflow_status
        count_success, count_failure = 0, 0
        # print('gen_pod_stat', value['update_time'])
        if workflow_status == 'FAILURE':
            count_failure += value.bag_nums
            yield {
                'count_failure': count_failure,
                'count_success': count_success,
                'value': value
            }
        elif workflow_status == 'SUCCESS' and 'bag_replayed_list' in workflow_output:
            for one in workflow_output['bag_replayed_list']:
                if one:
                    count_success += 1
                else:
                    count_failure += 1
            yield {
                'count_failure': count_failure,
                'count_success': count_success,
                'value': value
            }

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute()
