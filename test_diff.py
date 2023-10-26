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
from lib.common.schema import TEST_ARS_WORKFLOW_SCHEMA
from collections import defaultdict
from lib.utils.db import get_engine

logger = logging.getLogger(__name__)
import configparser

import sqlalchemy

class AddBagCount(FlatMapFunction):
    def flat_map(self, value):
        
        workflow_output = json.loads(value.workflow_output)
        workflow_status = value.workflow_status
        count_success, count_failure = 0, 0
        print('gen_pod_stat', value['update_time'])
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




def analyse(env: StreamExecutionEnvironment):

    stream = env.add_source(
        get_flink_kafka_consumer(
            schema=TEST_ARS_WORKFLOW_SCHEMA,
            topic=KAFKA_TOPIC_OF_ARS_WORKFLOW,
            group_id='test',
            start_date=datetime(2023, 10, 22)))
    
    count_stream = stream.filter(lambda x: x.update_time >= '2023-10-22 00:00:00' and x.update_time < '2023-10-24 00:00:00' and x.workflow_type=='replay').map(OneMap(),output_type=Types.INT())
    # count_result = count_stream.key_by(lambda x: "constant").reduce(SumReduce()).print()
    


class SumReduce(ReduceFunction):
    def reduce(self, value1, value2):
        return value1 + value2

fail_fail_sql = """
    select COUNT(*)
    from result
    where workflow_id_id = '%s'
"""
succ_succ_sql = """
    select COUNT(*)
    from result
    where workflow_id_id = '%s' and output_md5!=''
"""
succ_fail_sql = """
    select COUNT(*)
    from result
    where workflow_id_id = '%s' and output_md5=''
"""
class OneMap(MapFunction):
    def map(self, value):
        path = '/home/martin.hou/flink-jobs/etc/prod_mysql.conf'
        configer = configparser.ConfigParser()
        configer.read(path)
        src_config = dict(dict(configer["client"]))
        src_connection_str = f"mysql+pymysql://{src_config.get('user')}:{src_config.get('password')}@{src_config.get('host')}:{src_config.get('port')}/{src_config.get('database')}"
        engine = sqlalchemy.create_engine(
            src_connection_str, pool_pre_ping=True, pool_size=1, pool_recycle=30)
        workflow_output = json.loads(value.workflow_output)
        workflow_status = value.workflow_status
        count_success, count_failure = 0, 0
        print('gen_pod_stat', value['update_time'])
        if workflow_status == 'FAILURE':
            count_failure += value.bag_nums
            with engine.connect() as conn:
                fail_cnt = conn.execute(fail_fail_sql % value.workflow_id).scalar()
            if count_failure!=fail_cnt:
                with open("tmp.txt", 'a') as f:
                    f.write(f'{value.workflow_id}, kafka_fail:{count_failure}, sql_fail:{fail_cnt}\n')
                print()
        elif workflow_status == 'SUCCESS' and 'bag_replayed_list' in workflow_output:
            for one in workflow_output['bag_replayed_list']:
                if one:
                    count_success += 1
                else:
                    count_failure += 1
            with engine.connect() as conn:
                succ_cnt = conn.execute(succ_succ_sql % value.workflow_id).scalar()
                fail_cnt = conn.execute(succ_fail_sql % value.workflow_id).scalar()
            if count_success!=succ_cnt or count_failure!=fail_cnt:
                with open("tmp.txt", 'a') as f:
                    f.write(f'{value.workflow_id}, kafka_succ:{count_success},sql_succ:{succ_cnt},kafka_fail:{count_failure}, sql_fail:{fail_cnt}\n')
            
        return 1
    

    
if __name__ == "__main__":
        
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute()

    sql = defaultdict(lambda: defaultdict(int))