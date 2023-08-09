import logging
import time
from typing import Iterable
import datetime
import json
from pyflink.common import (
    Types, 
    WatermarkStrategy,
    Time, 
    Duration,
)
from pyflink.datastream import (
    StreamExecutionEnvironment, 
    ProcessWindowFunction, 
    AggregateFunction,
    FlatMapFunction,
)
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.connectors.kafka import  FlinkKafkaConsumer
from pyflink.datastream.formats.json import  JsonRowDeserializationSchema
from pyflink.datastream.window import TumblingEventTimeWindows, TimeWindow

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


def datetime_str_to_int(datetime_str:str)->int:
    datetime_object = datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    date_int= int(time.mktime((datetime_object).timetuple()))
    return date_int

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
        for one in workflow_output['bag_replayed_list']:
            if one=='':
                yield {'result':'failure','value':value}
            else:
                yield {'result':'success','value':value}


class CountAggFunction(AggregateFunction):
    def __init__(self, tag:str='normal'):
        self.tag = tag
    def create_accumulator(self) -> int:
        return 0

    def add(self, value: dict, accumulator: int) -> int:
        if self.tag=='normal':
            return accumulator + 1


    def get_result(self, accumulator) -> int:
        return accumulator

    def merge(self, a: int, b: int) -> tuple:
        return a + b

class AddMetaProcessFunction(
        ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    def __init__(self, tag:str='count'):
        self.tag = tag

    def process(self, key: str,
                context: ProcessWindowFunction.Context[TimeWindow],
                elements: Iterable[tuple]) -> Iterable[tuple]:
        count = elements[0]
        return dict(time=str(datetime.datetime.fromtimestamp(context.window().start * 1e-3)),
               key= key, count=count,tag=self.tag).items()


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

    # 也可以使用set_start_from_timestamp
    # kafka_consumer.set_start_from_earliest()

    return kafka_consumer


def analyse(env: StreamExecutionEnvironment):
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_hours(2)) \
        .with_timestamp_assigner(JSONTimestampAssigner())

    stream = env.add_source(read_from_kafka())
    result= stream \
       .assign_timestamps_and_watermarks(watermark_strategy)\
       
    result1=result.filter(lambda x:x.workflow_type=='replay').flat_map(Flatten())\
        .filter(lambda x:x['result']=='success')\
        .key_by(lambda x: x['value']['category'])\
       .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))\
       .aggregate(CountAggFunction(), window_function=AddMetaProcessFunction(tag='Replay categories(bags)'))
    result2=result.filter(lambda x:x.workflow_type=='replay').flat_map(Flatten())\
        .key_by(lambda x: x['result'])\
       .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))\
       .aggregate(CountAggFunction(), window_function=AddMetaProcessFunction(tag='Total replay processing'))
    result3=result.filter(lambda x:x.workflow_type=='replay').flat_map(Flatten())\
        .filter(lambda x:x['result']=='success')\
        .key_by(lambda x: json.loads(x['value']['workflow_input'])['extra_args']['mode'])\
       .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))\
       .aggregate(CountAggFunction(), window_function=AddMetaProcessFunction(tag='Replay modes'))
    result4=result.flat_map(Flatten())\
        .filter(lambda x:x['result']=='success')\
        .key_by(lambda x: x['value']['workflow_type'])\
       .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))\
       .aggregate(CountAggFunction(), window_function=AddMetaProcessFunction(tag='Successfully processed bags'))

    result4.print()

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # jar path
    env.add_jars(
         "file:///home/simon.feng/flink_demo/flink_demo/flink-sql-connector-kafka-1.15.4.jar"
    )
    analyse(env)
    env.execute()