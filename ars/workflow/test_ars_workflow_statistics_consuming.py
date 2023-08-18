import logging
import time
from typing import Iterable
import datetime
import json
from pyflink.common import (
    Types, )
from pyflink.datastream import (StreamExecutionEnvironment, FlatMapFunction,
                                RuntimeContext, FilterFunction)
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.state import MapStateDescriptor
from createsql import StatisticsActions

logger = logging.getLogger(__name__)

TEST_ARS_WORKFLOW_SCHEMA = {
    "workflow_id": Types.STRING(),
    "workflow_type": Types.STRING(),
    "workflow_name": Types.STRING(),
    "category": Types.STRING(),
    "device": Types.STRING(),
    "device_num": Types.INT(),
    "user": Types.STRING(),
    "data_source": Types.STRING(),
    "upload_ttl": Types.FLOAT(),
    "bag_nums": Types.INT(),
    "workflow_input": Types.STRING(),
    'workflow_output': Types.STRING(),
    'log': Types.STRING(),
    'workflow_status': Types.STRING(),
    'priority': Types.INT(),
    'tag': Types.STRING(),
    'hook': Types.STRING(),
    'create_time': Types.STRING(),
    'update_time': Types.STRING(),
    'batch_id_id': Types.STRING(),
    'tos_id': Types.STRING(),
    'metric': Types.STRING(),
}
KAFKA_TOPIC_OF_ARS_WORKFLOW = 'ars_prod_pod_result'
KAFKA_SERVERS = "10.10.2.224:9092,10.10.2.81:9092,10.10.3.141:9092"
KAFKA_CONSUMUER_GOURP_ID = "test-1"


class MyflatmapFunction(FlatMapFunction):
    def __init__(self, tag: str = 'noraml') -> None:
        self.tag = tag
        self.count_timer = None
        self.count_timer_name = 'count_timer'
        self.day_count_timer = None,
        self.day_count_timer_name = 'day_count_timer'
        self.day_temp_timer = None
        self.day_temp_timer_name = 'day_temp_timer_name'
        self.delay_time = 86400

    def open(self, context: RuntimeContext) -> None:
        descriptor = MapStateDescriptor(
            name=self.count_timer_name,
            key_type_info=Types.INT(),
            value_type_info=Types.STRING(),
        )
        self.count_timer = context.get_map_state(descriptor)
        descriptor1 = MapStateDescriptor(
            name=self.day_count_timer_name,
            key_type_info=Types.INT(),
            value_type_info=Types.STRING(),
        )
        self.day_count_timer = context.get_map_state(descriptor1)
        descriptor2 = MapStateDescriptor(
            name=self.day_temp_timer_name,
            key_type_info=Types.INT(),
            value_type_info=Types.INT(),
        )
        self.day_temp_timer = context.get_map_state(descriptor2)

    def flat_map(self, value):
        
        count_json = self.count_timer.get(value['minutetime_int'])
        result=self.value_to_consuming(value,count_json,'minute')
        if self.count_timer.is_empty():
            max_time_int = 0
        else:
            max_time_int = max(list(self.count_timer.keys()))

        if count_json is None:

            if value['minutetime_int'] > max_time_int and max_time_int != 0:
                self.count_timer.put(value['minutetime_int'],
                                     json.dumps(result))
                self.minute_to_day(self.count_timer.get(max_time_int))
                self.day_temp_timer.clear()

            elif max_time_int == 0:
                self.count_timer.put(value['minutetime_int'],
                                     json.dumps(result))
            else:
                if max_time_int - value['minutetime_int'] <= self.delay_time:
                    self.count_timer.put(value['minutetime_int'],
                                         json.dumps(result))
                    self.value_to_day(value)

        else:
           
            
            if value['minutetime_int'] == max_time_int:
                self.count_timer.put(value['minutetime_int'],
                                     json.dumps(result))
            else:
                if max_time_int - value['minutetime_int'] <= self.delay_time:
                    self.count_timer.put(value['minutetime_int'],
                                         json.dumps(result))
                    self.value_to_day(value)
        yield value

    def consuming_to_consuming(self,minute_json:dict,day_json:dict):
        info=day_json['info']
        for device,device_dict in dict(minute_json['info']).items():
            if device not in dict(info).keys():
                info[device] = device_dict
            else:
                for category,category_dict in dict(minute_json['info'][device]).items():
                    if category not in dict(info[device]).keys():
                        info[device][category]=category_dict
                    else:
                        for status_label in ['total', 'SUCCESS', 'FAILURE']:
                            if len(
                                    dict(info[device][category]
                                        [status_label]).keys()) == 0:
                                info[device][
                                    category][status_label] =category_dict[status_label]
                            elif len(
                                    dict(category_dict
                                        [status_label]).keys()) != 0:
                                for k, v in dict(info[device][
                                        category][status_label]).items():
                                    info[device][category][status_label][
                                            k] +=category_dict[status_label][k]
        day_json['info']=info
        return day_json
        

    def value_to_consuming(self, value, count_json,time_type):
        if time_type=='minute':
            result={
                    'daytime_int': value['daytime_int'],
                    'daytime': value['daytime'],
                    'minutetime': value['minutetime'],
                    'minutetime_int': value['minutetime_int'],
                }
        if time_type=='day':
            result={
                    'daytime_int': value['daytime_int'],
                    'daytime': value['daytime'],
                }
        if count_json == None:
            result['info']={
                    value['device']: {
                        value['category']: value['bags_profile_summary']
                    }
                }
        else:
            count_json_dict = json.loads(count_json)
            if 'info' not in dict(count_json_dict).keys():
                print(count_json_dict)
            info = count_json_dict['info']
            if value['device'] not in dict(info).keys():
                info[value['device']] = {
                    value['category']: value['bags_profile_summary']
                }
            else:
                if value['category'] not in dict(info[value['device']]).keys():
                    info[value['device']][
                        value['category']] = value['bags_profile_summary']
                else:
                    for status_label in ['total', 'SUCCESS', 'FAILURE']:
                        if len(
                                dict(info[value['device']][value['category']]
                                     [status_label]).keys()) == 0:
                            info[value['device']][
                                value['category']][status_label] = value[
                                    'bags_profile_summary'][status_label]
                        elif len(
                                dict(value[
                                    'bags_profile_summary'][status_label]).keys()) != 0:
                            for k, v in dict(info[value['device']][
                                    value['category']][status_label]).items():
                                info[value['device']][
                                    value['category']][status_label][
                                        k] += value['bags_profile_summary'][
                                            status_label][k]
            result['info']=info
        return result

    def stat_replay_time_consuming_group_by_category(self, json_str, tag: str):
        json_mysql = json.loads(json_str)
        stat_date = json_mysql['daytime'] + ' 00:00:00'
        name = tag
        period = 'daily'
        statis_one = StatisticsActions()
        list_get = list(
            statis_one.get_statistics(
                name=name, period=period, stat_date=stat_date))
        info=json_mysql['info']
        if len(list_get) == 0:
            statis_one.add_statistics(
                name=name, period=period, stat_date=stat_date, info=info)
        else:
            
            statis_one.update_statistics(
                name=name, period=period, stat_date=stat_date, info=info)

    def minute_to_day(self, minute_json: str):
        minute_json = json.loads(minute_json)
        daytime_int = minute_json['daytime_int']
        day_json = self.day_count_timer.get(daytime_int)
        self.day_temp_timer.put(daytime_int, 1)
        if day_json == None:
            self.day_count_timer.put(
                daytime_int,
                json.dumps({'daytime_int':daytime_int,
                'daytime':minute_json['daytime'],
                'info':minute_json['info']}))
        else:
            if type(day_json)==type({}):
                day_json=json.dumps(day_json)
            day_json_dict = json.loads(day_json)
            day_json_new=self.consuming_to_consuming(minute_json,day_json_dict)
            self.day_count_timer.put(daytime_int, json.dumps(day_json_new))
        for daytime_int_one in self.day_temp_timer.keys():

            self.stat_replay_time_consuming_group_by_category(
                json_str=self.day_count_timer.get(daytime_int_one),
                tag=self.tag)

    def value_to_day(self, value: dict):
        daytime_int = value['daytime_int']
        day_json = self.day_count_timer.get(daytime_int)
        if type(day_json)==type({}):
            day_json=json.dumps(day_json)
        self.day_temp_timer.put(daytime_int, 1)
        day_json=self.value_to_consuming(value,day_json,'day')
        self.day_count_timer.put(daytime_int, json.dumps(day_json))


def datetime_str_to_int(datetime_str: str) -> int:
    datetime_YMD = datetime_str.split(' ')[0]
    datetime_object = datetime.datetime.strptime(datetime_YMD, "%Y-%m-%d")
    date_int = int(time.mktime((datetime_object).timetuple()))
    return date_int


def time_str_to_int(time_str: str) -> int:
    time_object = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    time_int = int(time.mktime((time_object).timetuple()))
    return time_int


def datetime_obj_to_int(datetime) -> int:
    time_int = int(time.mktime((datetime).timetuple()))
    return time_int


def timestr_to_datestr(time_str: str) -> str:
    return time_str.split(' ')[0]


def timestr_to_minute_int(time_str: str) -> int:
    time_object = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    time_minute = time_object.replace(second=0, microsecond=0)
    time_int = int(time.mktime((time_minute).timetuple()))
    return time_int


def timestr_to_minutestr(time_str: str) -> str:
    time_object = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    time_minute = time_object.replace(second=0, microsecond=0)
    return str(time_minute)


class Flatten(FlatMapFunction):
    def flat_map(self, value):
        workflow_output = json.loads(value.workflow_output)

        count_success = 0
        count_failure = 0
        if 'bag_replayed_list' in dict(workflow_output).keys():
            for one in workflow_output['bag_replayed_list']:
                if one == '':
                    count_failure += 1
                else:
                    count_success += 1
            yield {
                'count_failure': count_failure,
                'count_success': count_success,
                'value': value
            }


class Filter(FilterFunction):
    def filter(self, value):

        return len(dict(json.loads(value.metric)).keys()) != 0


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
            'bootstrap.servers': KAFKA_SERVERS,
            'group.id': KAFKA_CONSUMUER_GOURP_ID,
        })
    # date_string = "2023-08-11 13:30:00"
    # date_object = datetime.datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
    # date_int = int(int(time.mktime((date_object).timetuple())) * 1000)
    # # 也可以使用set_start_from_timestamp
    # kafka_consumer.set_start_from_timestamp(date_int)
    kafka_consumer.set_start_from_earliest()
    return kafka_consumer


def analyse(env: StreamExecutionEnvironment):

    stream = env.add_source(read_from_kafka())
    result1 = stream.filter(Filter()).map(
        lambda x: {
            'daytime_int': datetime_str_to_int(x.update_time),
            'time_int': time_str_to_int(x.update_time),
            'time': x.update_time,
            'daytime': timestr_to_datestr(x.update_time),
            'minutetime_int': timestr_to_minute_int(x.update_time),
            'minutetime': timestr_to_minutestr(x.update_time),
            'device':x.device,
            'category':x.category,
            'bags_profile_summary':json.loads(x.metric)['bags_profile_summary']
            }).key_by(lambda x:x['device']).key_by(lambda x:x['category']).flat_map(MyflatmapFunction(tag='stat_replay_time_consuming_group_by_category'))


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(
        "file:///home/simon.feng/flink_demo/flink_demo/flink-sql-connector-kafka-1.15.4.jar"
    )
    analyse(env)
    env.execute()