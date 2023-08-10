import logging
import time
from typing import Iterable
import datetime
import json
from pyflink.common import (
    Types,
)
from pyflink.datastream import (
    StreamExecutionEnvironment, 
    FlatMapFunction,
    RuntimeContext,
)
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.connectors.kafka import  FlinkKafkaConsumer
from pyflink.datastream.formats.json import  JsonRowDeserializationSchema
from pyflink.datastream.state import MapStateDescriptor

logger = logging.getLogger(__name__)


TEST_ARS_WORKFLOW_SCHEMA = {
        "workflow_id": Types.STRING(),
        "workflow_type": Types.STRING(),
        "workflow_name":Types.STRING(),
        "category":Types.STRING(),
        "device":Types.STRING(),
        "device_num":Types.INT(),
        "user":Types.STRING(),
        "data_source":Types.STRING(),
        "upload_ttl":Types.FLOAT(),
        "bag_nums":Types.INT(),
        "workflow_input":Types.STRING(),
        'workflow_output':Types.STRING(),
        'log':Types.STRING(),
        'workflow_status':Types.STRING(),
        'priority':Types.INT(),
        'tag':Types.STRING(),
        'hook':Types.STRING(),
        'create_time':Types.STRING(),
        'update_time':Types.STRING(),
        'batch_id_id':Types.STRING(),
        'tos_id':Types.STRING(),
        'metric':Types.STRING(),
    }
KAFKA_TOPIC_OF_ARS_WORKFLOW = 'test_ars_workflow_statistics'
KAFKA_SERVERS = "10.10.2.224:9092,10.10.2.81:9092,10.10.3.141:9092"
KAFKA_CONSUMUER_GOURP_ID = "pyflink-ars"

    
class MyflatmapFunction(FlatMapFunction):
    def __init__(self,tag:str='noraml') -> None:
        self.tag=tag,
        self.count_timer=None,
        self.count_timer_name='count_timer'
    def open(self, context: RuntimeContext) -> None:
        descriptor = MapStateDescriptor(
            name=self.count_timer_name, 
            key_type_info=Types.INT(), 
            value_type_info=Types.STRING(),
        )
        self.count_timer = context.get_map_state(descriptor)
    def flat_map(self, value):
        return_dict={'label':value['label'],
                     'count_success':value['count_success'],
                     'count_failure':value['count_failure'],
                    'datetime_int':value['datetime_int'],
                    'datetime':value['datetime']}
        count_json=self.count_timer.get(value['datetime_int'])
        if self.count_timer.is_empty():
            max_time_int=0
        else:
            max_time_int=max(list(self.count_timer.keys()))
        if count_json is None:
            
            self.count_timer.put(value['datetime_int'],json.dumps(return_dict))
            if value['datetime_int']>max_time_int and max_time_int!=0:
                print(self.count_timer.get(max_time_int),self.tag)
            elif max_time_int==0:
                pass
            else:
                print(self.count_timer.get(value['datetime_int']),self.tag)

        else:
            count_json=json.loads(count_json)
            count_json['count_success']+=value['count_success']
            count_json['count_failure']+=value['count_failure']
            self.count_timer.put(value['datetime_int'],json.dumps(count_json))
            if value['datetime_int']==max_time_int:
                pass
            else:
                print(self.count_timer.get(value['datetime_int']),self.tag)
        yield value
        

def datetime_str_to_int(datetime_str:str)->int:
    datetime_YMD=datetime_str.split(' ')[0]
    datetime_object = datetime.datetime.strptime(datetime_YMD, "%Y-%m-%d")
    date_int= int(time.mktime((datetime_object).timetuple()))
    return date_int
def time_str_to_int(time_str:str)->int:
    time_object = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    time_int= int(time.mktime((time_object).timetuple()))
    return time_int
def datetime_obj_to_int(datetime)->int:
    time_int= int(time.mktime((datetime).timetuple()))
    return time_int
def timestr_to_datestr(time_str:str)->str:
    return time_str.split(' ')[0]


class JSONTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(self.datetime_str_to_int(value.create_time) * 1e3)
    def datetime_str_to_int(self,datetime_str:str)->int:
        datetime_object = datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        date_int= int(time.mktime((datetime_object).timetuple()))
        return date_int


class Flatten(FlatMapFunction):
    def flat_map(self, value):
        workflow_output=json.loads(value.workflow_output)
        count_success=0
        count_failure=0
        for one in workflow_output['bag_replayed_list']:
            if one=='':
                count_failure+=1
            else:
                count_success+=1
        yield {'count_failure':count_failure,'count_success':count_success,'value':value}


def read_from_kafka():

    KEYS = [k for k in TEST_ARS_WORKFLOW_SCHEMA.keys()]
    VALUES = [TEST_ARS_WORKFLOW_SCHEMA[k] for k in KEYS]
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(Types.ROW_NAMED(KEYS, VALUES)) \
        .build()
    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC_OF_ARS_WORKFLOW,
        deserialization_schema=deserialization_schema,
        properties={
            'bootstrap.servers':KAFKA_SERVERS
            ,
            'group.id':KAFKA_CONSUMUER_GOURP_ID,
        })
    date_string = "2023-08-10 20:36:00"
    date_object = datetime.datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
    date_int= int(int(
            time.mktime((date_object).timetuple()))*1000)
    # 也可以使用set_start_from_timestamp
    kafka_consumer.set_start_from_timestamp(date_int)

    return kafka_consumer


def analyse(env: StreamExecutionEnvironment):

    stream = env.add_source(read_from_kafka())



    result1=stream.filter(lambda x:x.workflow_type=='replay').flat_map(Flatten())\
        .map(lambda x:{'count_failure':x['count_failure'],
                       'value':x['value'],
                       'count_success':x['count_success'],
                       'label':x['value']['category'],
                       'datetime_int':datetime_str_to_int(x['value']['create_time']),
                       'time_int':time_str_to_int(x['value']['create_time']),
                       'time':x['value']['create_time'],
                       'datetime':timestr_to_datestr(x['value']['create_time']),
                       })\
        .key_by(lambda x: x['label'])\
       .flat_map(MyflatmapFunction(tag='Replay categories(bags)'))
    result2=stream.filter(lambda x:x.workflow_type=='replay').flat_map(Flatten())\
        .map(lambda x:{'count_failure':x['count_failure'],
                       'value':x['value'],
                       'count_success':x['count_success'],
                       'label':'normal',
                       'datetime_int':datetime_str_to_int(x['value']['create_time']),
                       'time_int':time_str_to_int(x['value']['create_time']),
                       'time':x['value']['create_time'],
                       'datetime':timestr_to_datestr(x['value']['create_time']),
                       })\
        .flat_map(MyflatmapFunction(tag='Total replay processing'))
    result3=stream.filter(lambda x:x.workflow_type=='replay').flat_map(Flatten())\
        .map(lambda x:{'count_failure':x['count_failure'],
                       'value':x['value'],
                       'count_success':x['count_success'],
                       'label':json.loads(x['value']['workflow_input'])['extra_args']['mode'],
                       'datetime_int':datetime_str_to_int(x['value']['create_time']),
                       'time_int':time_str_to_int(x['value']['create_time']),
                       'time':x['value']['create_time'],
                       'datetime':timestr_to_datestr(x['value']['create_time']),
                       })\
        .key_by(lambda x: x['label'])\
        .flat_map(MyflatmapFunction(tag='Relay modes'))
    result4=stream.flat_map(Flatten())\
        .map(lambda x:{'count_failure':x['count_failure'],
                       'value':x['value'],
                       'count_success':x['count_success'],
                       'label':x['value']['workflow_type'],
                       'datetime_int':datetime_str_to_int(x['value']['create_time']),
                       'time_int':time_str_to_int(x['value']['create_time']),
                       'time':x['value']['create_time'],
                       'datetime':timestr_to_datestr(x['value']['create_time']),
                       })\
        .key_by(lambda x: x['label']).flat_map(MyflatmapFunction(tag='Successfully processed bags'))

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(
         "file:///home/simon.feng/flink_demo/flink_demo/flink-sql-connector-kafka-1.15.4.jar"
    )
    analyse(env)
    env.execute()