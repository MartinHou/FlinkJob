import logging
import json
from pyflink.common import (
    Types, )
from pyflink.datastream import (
    StreamExecutionEnvironment,
    FlatMapFunction,
    RuntimeContext,
    MapFunction,
)
from pyflink.datastream.state import MapStateDescriptor
from lib.utils.sql import StatisticsActions
from lib.common.settings import *
from lib.utils.kafka import get_flink_kafka_consumer
from lib.utils.dates import *
from lib.common.schema import TEST_ARS_WORKFLOW_SCHEMA

logger = logging.getLogger(__name__)


class AddPodCount(MapFunction):
    def map(self, value):
        return {
            'value': value,
            'count_failure': 1 if value['workflow_status'] == 'FAILURE' else 0,
            'count_success': 1 if value['workflow_status'] == 'SUCCESS' else 0
        }


class AddBagCount(FlatMapFunction):
    def flat_map(self, value):
        workflow_output = json.loads(value.workflow_output)
        count_success, count_failure = 0, 0
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


class HandleCountFlatMap(FlatMapFunction):
    def __init__(self, tag: str = 'noraml') -> None:
        self.tag = tag
        self.minute_data = None  # 分钟stamp: return_dict
        self.day_data = None,  # day级: return_dict-minutes级时间戳
        self.changed_days = None,  # day级:占位的1?
        self.delay_time = 86400

    def open(self, context: RuntimeContext) -> None:
        descriptor = MapStateDescriptor(
            name='minute_data',
            key_type_info=Types.INT(),
            value_type_info=Types.STRING(),
        )
        self.minute_data = context.get_map_state(descriptor)
        descriptor1 = MapStateDescriptor(
            name='day_data',
            key_type_info=Types.INT(),
            value_type_info=Types.STRING(),
        )
        self.day_data = context.get_map_state(descriptor1)
        descriptor2 = MapStateDescriptor(
            name='changed_days_name',
            key_type_info=Types.INT(),
            value_type_info=Types.INT(),
        )
        self.changed_days = context.get_map_state(descriptor2)

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
        count_json = self.minute_data.get(
            value['minutetime_int'])  # 这个分钟的return_dict累加数据
        max_minute_int = max(self.minute_data.keys()) if not self.minute_data.is_empty() else 0  # 目前为止最大的分钟时间戳

        if count_json is None:  # 首次进入这个60秒窗口
            if max_minute_int - value['minutetime_int'] > self.delay_time:  # 超过1天前的数据，不应该发生
                return iter([])
            self.minute_data.put(value['minutetime_int'],json.dumps(return_dict))
            if max_minute_int==0:   # kafka来的第一条数据(为什么不update_day_state?)
                return iter([])
            if value['minutetime_int'] > max_minute_int and max_minute_int != 0:  # 这是更大的分钟，把前一分钟数据持久化
                self.record_minute(self.minute_data.get(max_minute_int))
            else:  # 当前分钟不是最新的分钟，但不超1天前
                self.update_day_state(value)

        else:  # 该分钟有过数据了，count_json先更新为加上这次数据的这分钟数据
            count_json = json.loads(count_json)
            count_json['count_success'] += value['count_success']
            count_json['count_failure'] += value['count_failure']

            if value['minutetime_int'] == max_minute_int:  # 当前分钟时最新的分钟
                self.minute_data.put(value['minutetime_int'],
                                     json.dumps(count_json))
            elif max_minute_int - value[
                    'minutetime_int'] <= self.delay_time:  # 当前分钟不是最新的分钟，但不超1天前
                self.minute_data.put(value['minutetime_int'],
                                     json.dumps(count_json))
                self.update_day_state(value)
        return iter([])

    def stat_pod(self, json_str):
        json_mysql = json.loads(json_str)
        stat_date = json_mysql['daytime'] + ' 00:00:00'
        period = 'daily'
        statis_one = StatisticsActions()
        for status in ['success', 'failure']:
            name = self.tag.replace('status', status)
            list_get = list(
                statis_one.get_statistics(
                    name=name, period=period, stat_date=stat_date))
            if len(list_get) == 0:
                info = {}
                info[json_mysql['label']] = json_mysql[f'count_{status}']
                statis_one.add_statistics(
                    name=name, period=period, stat_date=stat_date, info=info)
            else:
                info = list_get[0].info
                info[json_mysql['label']] = json_mysql[f'count_{status}']
                statis_one.update_statistics(
                    name=name, period=period, stat_date=stat_date, info=info)

    def record_minute(self, minute_json: str):
        minute_json = json.loads(minute_json)
        daytime_int = minute_json['daytime_int']
        day_json = self.day_data.get(daytime_int)  # 旧的天级数据
        self.changed_days.put(daytime_int, 1)  # 表示哪些天的数据发生更改了
        if day_json == None:  # 第一次进入这个天级数据
            self.day_data.put(
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
            self.day_data.put(daytime_int, json.dumps(day_json))
        for daytime_int_one in self.changed_days.keys():
            self.stat_pod(json_str=self.day_data.get(daytime_int_one))
        self.changed_days.clear()

    def update_day_state(self, value: dict):
        daytime_int = value['daytime_int']
        day_json = self.day_data.get(daytime_int)
        self.changed_days.put(daytime_int, 1)  # 表示哪些天的数据发生更改了
        if day_json == None:  # 那天还没数据
            self.day_data.put(
                daytime_int,
                json.dumps({
                    'daytime_int': daytime_int,
                    'daytime': value['daytime'],
                    'label': value['label'],
                    'count_success': value['count_success'],
                    'count_failure': value['count_failure'],
                }))
        else:  # 那天有数据，更新这个天级数据day_data
            day_json = json.loads(day_json)
            day_json['count_success'] += value['count_success']
            day_json['count_failure'] += value['count_failure']
            self.day_data.put(daytime_int, json.dumps(day_json))


def analyse(env: StreamExecutionEnvironment):

    stream = env.add_source(
        get_flink_kafka_consumer(
            schema=TEST_ARS_WORKFLOW_SCHEMA,
            topic=KAFKA_TOPIC_OF_ARS_WORKFLOW,
            group_id='martin_stat_pod',
            start_date=START_TIME))

    stat_status_pod_group_by_type = stream.map(AddPodCount()).map(lambda x:{
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
    .flat_map(HandleCountFlatMap(tag='stat_status_pod_group_by_type'))

    stat_replay_status_bag_group_by_category=stream.filter(lambda x:x.workflow_type=='replay').flat_map(AddBagCount())\
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
       .flat_map(HandleCountFlatMap(tag='stat_replay_status_bag_group_by_category'))
       
    stat_replay_status_bag_group_by_mode=stream.filter(lambda x:x.workflow_type=='replay').flat_map(AddBagCount())\
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
        .flat_map(HandleCountFlatMap(tag='stat_replay_status_bag_group_by_mode'))
        
    stat_status_bag_group_by_type=stream.flat_map(AddBagCount())\
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
        .key_by(lambda x: x['label']).flat_map(HandleCountFlatMap(tag='stat_status_bag_group_by_type'))


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute()
