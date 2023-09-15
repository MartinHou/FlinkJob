import logging
import json
from pyflink.common import (
    Types, )
from pyflink.datastream import (
    StreamExecutionEnvironment,
    FlatMapFunction,
    RuntimeContext,
)
from pyflink.datastream.state import MapStateDescriptor
from createsql import StatisticsActions
from lib.common.settings import *
from lib.utils.kafka import get_flink_kafka_consumer
from lib.utils.dates import *
from lib.common.schema import TEST_ARS_WORKFLOW_SCHEMA

logger = logging.getLogger(__name__)


class MyflatmapFunction(FlatMapFunction):
    def __init__(self, tag: str = 'noraml') -> None:
        self.tag = tag
        self.count_timer = None  # 分钟级:return_dict
        self.day_count_timer = None,  # day级: return_dict-minutes级时间戳
        self.day_temp_timer = None,  # day级:占位的1?
        self.delay_time = 86400

    def open(self, context: RuntimeContext) -> None:
        descriptor = MapStateDescriptor(
            name='count_timer',
            key_type_info=Types.INT(),
            value_type_info=Types.STRING(),
        )
        self.count_timer = context.get_map_state(descriptor)
        descriptor1 = MapStateDescriptor(
            name='day_count_timer',
            key_type_info=Types.INT(),
            value_type_info=Types.STRING(),
        )
        self.day_count_timer = context.get_map_state(descriptor1)
        descriptor2 = MapStateDescriptor(
            name='day_temp_timer_name',
            key_type_info=Types.INT(),
            value_type_info=Types.INT(),
        )
        self.day_temp_timer = context.get_map_state(descriptor2)

    def flat_map(self, value):
        return_dict = {
            'label': value['label'],
            'count_success': value['count_success'],
            'count_failure': value['count_failure'],
            'daytime_int': value['daytime_int'],
            'daytime': value['daytime'],
            'minutetime': value['minutetime'],
            'minutetime_int': value['minutetime_int'],
        }
        count_json = self.count_timer.get(value['minutetime_int'])
        if self.count_timer.is_empty():
            max_time_int = 0
        else:
            max_time_int = max(list(self.count_timer.keys()))  # 目前为止最大的分钟时间戳

        if count_json is None:  # 首次进入这个60秒窗口

            if value['minutetime_int'] > max_time_int and max_time_int != 0:  # 这是更大的分钟
                self.count_timer.put(value['minutetime_int'],
                                     json.dumps(return_dict))
                self.minute_to_day(self.count_timer.get(max_time_int))
                self.day_temp_timer.clear()

            elif max_time_int == 0:  # 没有旧数据
                self.count_timer.put(value['minutetime_int'],
                                     json.dumps(return_dict))
            elif max_time_int - value[
                    'minutetime_int'] <= self.delay_time:  # 在以前的时间，且不超1天前
                self.count_timer.put(value['minutetime_int'],
                                     json.dumps(return_dict))
                self.value_to_day(value)

        else:  # 该分钟有过数据了
            count_json = json.loads(count_json)
            count_json['count_success'] += value['count_success']
            count_json['count_failure'] += value['count_failure']

            if value['minutetime_int'] == max_time_int:  # 当前分钟时最新的分钟
                self.count_timer.put(value['minutetime_int'],
                                     json.dumps(count_json))
            elif max_time_int - value[
                    'minutetime_int'] <= self.delay_time:  # 当前分钟不是最新的分钟，且不超1天前
                self.count_timer.put(value['minutetime_int'],
                                     json.dumps(count_json))
                self.value_to_day(value)
        yield value

    def stat_failure_bag(self, json_str, tag: str):
        json_mysql = json.loads(json_str)
        stat_date = json_mysql['daytime'] + ' 00:00:00'
        name = tag.replace('status', 'failure')
        period = 'daily'
        statis_one = StatisticsActions()
        list_get = list(
            statis_one.get_statistics(
                name=name, period=period, stat_date=stat_date))
        if len(list_get) == 0:
            info = {}
            info[json_mysql['label']] = json_mysql['count_failure']
            statis_one.add_statistics(
                name=name, period=period, stat_date=stat_date, info=info)
        else:
            info = list_get[0].info
            info[json_mysql['label']] = json_mysql['count_failure']
            statis_one.update_statistics(
                name=name, period=period, stat_date=stat_date, info=info)

    def stat_success_bag(self, json_str, tag: str):
        json_mysql = json.loads(json_str)
        stat_date = json_mysql['daytime'] + ' 00:00:00'
        name = tag.replace('status', 'success')
        period = 'daily'
        statis_one = StatisticsActions()
        list_get = list(
            statis_one.get_statistics(
                name=name, period=period, stat_date=stat_date))
        if len(list_get) == 0:
            info = {}
            info[json_mysql['label']] = json_mysql['count_success']
            statis_one.add_statistics(
                name=name, period=period, stat_date=stat_date, info=info)
        else:
            info = list_get[0].info
            info[json_mysql['label']] = json_mysql['count_success']
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
                json.dumps({
                    'daytime_int': daytime_int,
                    'daytime': minute_json['daytime'],
                    'label': minute_json['label'],
                    'count_success': minute_json['count_success'],
                    'count_failure': minute_json['count_failure'],
                }))
        else:
            day_json = json.loads(day_json)
            day_json['count_success'] += minute_json['count_success']
            day_json['count_failure'] += minute_json['count_failure']
            self.day_count_timer.put(daytime_int, json.dumps(day_json))
        for daytime_int_one in self.day_temp_timer.keys():

            self.stat_success_bag(
                json_str=self.day_count_timer.get(daytime_int_one),
                tag=self.tag)
            self.stat_failure_bag(
                json_str=self.day_count_timer.get(daytime_int_one),
                tag=self.tag)

    def value_to_day(self, value: dict):
        daytime_int = value['daytime_int']
        day_json = self.day_count_timer.get(daytime_int)
        self.day_temp_timer.put(daytime_int, 1)
        if day_json == None:
            self.day_count_timer.put(
                daytime_int,
                json.dumps({
                    'daytime_int': daytime_int,
                    'daytime': value['daytime'],
                    'label': value['label'],
                    'count_success': value['count_success'],
                    'count_failure': value['count_failure'],
                }))
        else:
            day_json = json.loads(day_json)
            day_json['count_success'] += value['count_success']
            day_json['count_failure'] += value['count_failure']
            self.day_count_timer.put(daytime_int, json.dumps(day_json))


class PodFlatten(FlatMapFunction):
    def flat_map(self, value):
        yield {
            'value': value,
            'count_failure': 1 if value['workflow_status'] == 'FAILURE' else 0,
            'count_success': 1 if value['workflow_status'] == 'SUCCESS' else 0
        }


class Flatten(FlatMapFunction):
    def flat_map(self, value):

        workflow_output = json.loads(value.workflow_output)

        count_success = 0
        count_failure = 0
        if 'bag_replayed_list' in workflow_output:
            for one in workflow_output['bag_replayed_list']:
                if one:
                    count_success += 1
                else:
                    count_failure += 1
            print(value['update_time'])
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
            group_id='martin_test01',
            start_date=START_TIME))

    result_pod = stream.flat_map(PodFlatten()).map(lambda x:{
        'count_failure': x['count_failure'],
        'value':x['value'],
        'count_success': x['count_success'],
        'label':x['value']['workflow_type'],
        'daytime_int':datetime_str_to_int(x['value']['update_time']),
        'time_int':time_str_to_int(x['value']['update_time']),
        'time':x['value']['update_time'],
        'daytime':timestr_to_datestr(x['value']['update_time']),
        'minutetime_int':timestr_to_minute_int(x['value']['update_time']),
        'minutetime':timestr_to_minutestr(x['value']['update_time']),
    }).key_by(lambda x: x['label'])\
    .flat_map(MyflatmapFunction(tag='stat_status_pod_group_by_type'))

    result1=stream.filter(lambda x:x.workflow_type=='replay').flat_map(Flatten())\
        .map(lambda x:{'count_failure':x['count_failure'],
                       'value':x['value'],
                       'count_success':x['count_success'],
                       'label':x['value']['category'],
                       'daytime_int':datetime_str_to_int(x['value']['update_time']),
                       'time_int':time_str_to_int(x['value']['update_time']),
                       'time':x['value']['update_time'],
                       'daytime':timestr_to_datestr(x['value']['update_time']),
                       'minutetime_int':timestr_to_minute_int(x['value']['update_time']),
                       'minutetime':timestr_to_minutestr(x['value']['update_time']),
                       })\
        .key_by(lambda x: x['label'])\
       .flat_map(MyflatmapFunction(tag='stat_replay_status_bag_group_by_category'))
    # result2=stream.filter(lambda x:x.workflow_type=='replay').flat_map(Flatten())\
    #     .map(lambda x:{'count_failure':x['count_failure'],
    #                    'value':x['value'],
    #                    'count_success':x['count_success'],
    #                    'label':'normal',
    #                    'daytime_int':datetime_str_to_int(x['value']['update_time']),
    #                    'time_int':time_str_to_int(x['value']['update_time']),
    #                    'time':x['value']['update_time'],
    #                    'daytime':timestr_to_datestr(x['value']['update_time']),
    #                     'minutetime_int':timestr_to_minute_int(x['value']['update_time']),
    #                    'minutetime':timestr_to_minutestr(x['value']['update_time']),
    #                    })\
    #     .key_by(lambda x: x['label'])\
    #     .flat_map(MyflatmapFunction(tag='Total replay processing'))
    result3=stream.filter(lambda x:x.workflow_type=='replay').flat_map(Flatten())\
        .filter(lambda x:'extra_args' in json.loads(x['value']['workflow_input'])
                          and 'mode' in json.loads(x['value']['workflow_input'])['extra_args'])\
        .map(lambda x:{'count_failure':x['count_failure'],
                       'value':x['value'],
                       'count_success':x['count_success'],
                       'label':json.loads(x['value']['workflow_input'])['extra_args']['mode'],
                       'daytime_int':datetime_str_to_int(x['value']['update_time']),
                       'time_int':time_str_to_int(x['value']['update_time']),
                       'time':x['value']['update_time'],
                       'daytime':timestr_to_datestr(x['value']['update_time']),
                       'minutetime_int':timestr_to_minute_int(x['value']['update_time']),
                       'minutetime':timestr_to_minutestr(x['value']['update_time']),
                       })\
        .key_by(lambda x: x['label'])\
        .flat_map(MyflatmapFunction(tag='stat_replay_status_bag_group_by_mode'))
    result4=stream.flat_map(Flatten())\
        .map(lambda x:{'count_failure':x['count_failure'],
                       'value':x['value'],
                       'count_success':x['count_success'],
                       'label':x['value']['workflow_type'],
                       'daytime_int':datetime_str_to_int(x['value']['update_time']),
                       'time_int':time_str_to_int(x['value']['update_time']),
                       'time':x['value']['update_time'],
                       'daytime':timestr_to_datestr(x['value']['update_time']),
                       'minutetime_int':timestr_to_minute_int(x['value']['update_time']),
                       'minutetime':timestr_to_minutestr(x['value']['update_time']),
                       })\
        .key_by(lambda x: x['label']).flat_map(MyflatmapFunction(tag='stat_status_bag_group_by_type'))


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute()
