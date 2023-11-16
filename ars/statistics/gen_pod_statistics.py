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
import pandas as pd
from pyflink.datastream.state import MapStateDescriptor
from lib.utils.sql import StatisticsActions
from lib.common.settings import *
from lib.utils.kafka import get_flink_kafka_consumer
from lib.utils.dates import *
from lib.utils.utils import merge_dicts
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
        elif workflow_status == 'SUCCESS':
            if 'bag_replayed_list' in workflow_output:
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
            else:
                with open('no_bag_replayed_list.txt', 'a+') as f:
                    f.write(value.workflow_id + '\n')


class HandleCountFlatMap(FlatMapFunction):
    def __init__(self, tag: str = 'noraml') -> None:
        self.statistics_action = StatisticsActions()
        self.tag = tag
        self.minute_data = None  # 分钟0秒stamp: return_dict
        self.day_data = None  # 天0点stamp: return_dict
        self.week_data = None
        self.month_data = None
        self.changed_days = None  # 修改但未持久化的天stamp作为key，当hashset用
        self.changed_weeks = None
        self.changed_months = None
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
        descriptor3 = MapStateDescriptor(
            name='week_data',
            key_type_info=Types.INT(),
            value_type_info=Types.STRING(),
        )
        self.week_data = context.get_map_state(descriptor3)
        descriptor4 = MapStateDescriptor(
            name='changed_weeks_name',
            key_type_info=Types.INT(),
            value_type_info=Types.INT(),
        )
        self.changed_weeks = context.get_map_state(descriptor4)
        descriptor5 = MapStateDescriptor(
            name='month_data',
            key_type_info=Types.INT(),
            value_type_info=Types.STRING(),
        )
        self.month_data = context.get_map_state(descriptor5)
        descriptor6 = MapStateDescriptor(
            name='changed_months_name',
            key_type_info=Types.INT(),
            value_type_info=Types.INT(),
        )
        self.changed_months = context.get_map_state(descriptor6)

    def flat_map(self, value):
        def init(start_date, cur_date, state):
            inital = {
                'label': value['label'],
                'count_success': 0,
                'count_failure': 0,
                'daytime_int': value['daytime_int'],
                'daytime': value['daytime'],
                'minutetime': value['minutetime'],
                'minutetime_int': value['minutetime_int'],
                'weektime': value['weektime'],
                'weektime_int': value['weektime_int'],
                'monthtime': value['monthtime'],
                'monthtime_int': value['monthtime_int'],
            }
            start_date_ts = str_to_timestamp(start_date)
            for status in ['success', 'failure']:
                name = self.tag.replace('status', status)
                for date in pd.date_range(start_date, cur_date)[:-1]:
                    try:
                        day_res = self.statistics_action.get_statistics(
                            name=name, period='daily', stat_date=date)[0].info
                        if value['label'] in day_res:
                            inital[f'count_{status}'] += day_res[
                                value['label']]
                    except Exception as e:
                        print(f'{name}, {date}, no data. {e}')
            state.put(start_date_ts, json.dumps(inital))

        if not self.month_data.contains(value['monthtime_int']):
            init(value['monthtime'], value['daytime'], self.month_data)
        if not self.week_data.contains(value['weektime_int']):
            init(value['weektime'], value['daytime'], self.week_data)

        return_dict = {
            'label': value['label'],
            'count_success': value['count_success'],
            'count_failure': value['count_failure'],
            'daytime_int': value['daytime_int'],
            'daytime': value['daytime'],
            'minutetime': value['minutetime'],
            'minutetime_int': value['minutetime_int'],
            'weektime': value['weektime'],
            'weektime_int': value['weektime_int'],
            'monthtime': value['monthtime'],
            'monthtime_int': value['monthtime_int'],
        }

        count_json = self.minute_data.get(
            value['minutetime_int'])  # 这个分钟的return_dict累加数据
        max_minute_int = max(self.minute_data.keys(
        )) if not self.minute_data.is_empty() else 0  # 目前为止最大的分钟时间戳

        if max_minute_int - value[
                'minutetime_int'] > self.delay_time:  # 过期数据，丢弃
            return iter([])

        if count_json is None:  # 首次进入这个60秒窗口
            self.minute_data.put(value['minutetime_int'],
                                 json.dumps(return_dict))
            if max_minute_int == 0:  # kafka来的第一条数据
                return iter([])
            if value[
                    'minutetime_int'] > max_minute_int and max_minute_int != 0:  # 这是更大的分钟，把前一分钟数据持久化
                self.record_minute(self.minute_data.get(max_minute_int))
            else:  # 不是最新的分钟
                self.update_state(value)

        else:  # 该分钟有过数据了，count_json先更新为加上这次数据的这分钟数据
            count_json = json.loads(count_json)
            count_json['count_success'] += value['count_success']
            count_json['count_failure'] += value['count_failure']
            self.minute_data.put(value['minutetime_int'],
                                 json.dumps(count_json))
            if value['minutetime_int'] != max_minute_int:  # 不是的最新的分钟
                self.update_state(value)
        return iter([])

    def stat_pod(self, json_str, period):
        json_mysql = json.loads(json_str)
        if period == 'daily':
            stat_date = json_mysql['daytime']
        elif period == 'weekly':
            stat_date = json_mysql['weektime']
        elif period == 'monthly':
            stat_date = json_mysql['monthtime']
        for status in ['success', 'failure']:
            name = self.tag.replace('status', status)
            list_get = list(
                self.statistics_action.get_statistics(
                    name=name, period=period, stat_date=stat_date))
            if len(list_get) == 0:
                info = {}
                info[json_mysql['label']] = json_mysql[f'count_{status}']
                self.statistics_action.add_statistics(
                    name=name, period=period, stat_date=stat_date, info=info)
            else:
                info = list_get[0].info
                info[json_mysql['label']] = json_mysql[f'count_{status}']
                self.statistics_action.update_statistics(
                    name=name, period=period, stat_date=stat_date, info=info)

    def record_minute(self, minute_json: str):
        minute_json = json.loads(minute_json)
        self.update_state(minute_json)
        for daytime_int_one in self.changed_days.keys():
            self.stat_pod(self.day_data.get(daytime_int_one), 'daily')
        for weektime_int_one in self.changed_weeks.keys():
            self.stat_pod(self.week_data.get(weektime_int_one), 'weekly')
        for monthtime_int_one in self.changed_months.keys():
            self.stat_pod(self.month_data.get(monthtime_int_one), 'monthly')
        self.changed_days.clear()
        self.changed_weeks.clear()
        self.changed_months.clear()

    def update_state(self, data: dict):
        daytime_int, weektime_int, monthtime_int = data['daytime_int'], data[
            'weektime_int'], data['monthtime_int']
        daytime_json = self.day_data.get(daytime_int)
        weektime_json = self.week_data.get(weektime_int)
        monthtime_json = self.month_data.get(monthtime_int)
        self.changed_days.put(daytime_int, 1)
        self.changed_weeks.put(weektime_int, 1)
        self.changed_months.put(monthtime_int, 1)
        ret = {
            'daytime_int': daytime_int,
            'daytime': data['daytime'],
            'minutetime': data['minutetime'],
            'minutetime_int': data['minutetime_int'],
            'weektime': data['weektime'],
            'weektime_int': weektime_int,
            'monthtime': data['monthtime'],
            'monthtime_int': monthtime_int,
            'label': data['label'],
            'count_success': data['count_success'],
            'count_failure': data['count_failure'],
        }
        if not daytime_json:
            self.day_data.put(daytime_int, json.dumps(ret))
        else:
            daytime_json = json.loads(daytime_json)
            daytime_json['count_success'] += data['count_success']
            daytime_json['count_failure'] += data['count_failure']
            self.day_data.put(daytime_int, json.dumps(daytime_json))
        if not weektime_json:
            self.week_data.put(weektime_int, json.dumps(ret))
        else:
            weektime_json = json.loads(weektime_json)
            weektime_json['count_success'] += data['count_success']
            weektime_json['count_failure'] += data['count_failure']
            self.week_data.put(weektime_int, json.dumps(weektime_json))
        if not monthtime_json:
            self.month_data.put(monthtime_int, json.dumps(ret))
        else:
            monthtime_json = json.loads(monthtime_json)
            monthtime_json['count_success'] += data['count_success']
            monthtime_json['count_failure'] += data['count_failure']
            self.month_data.put(monthtime_int, json.dumps(monthtime_json))


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
        'daytime_int':timestr_to_day_int(x['value']['update_time']),
        'time_int':str_to_timestamp(x['value']['update_time']),
        'time':x['value']['update_time'],
        'daytime':timestr_to_daystr(x['value']['update_time']),
        'minutetime_int':timestr_to_minute_int(x['value']['update_time']),
        'minutetime':timestr_to_minutestr(x['value']['update_time']),
        'weektime':timestr_to_weekstr(x['value']['update_time']),
        'weektime_int':timestr_to_week_int(x['value']['update_time']),
        'monthtime':timestr_to_monthstr(x['value']['update_time']),
        'monthtime_int':timestr_to_month_int(x['value']['update_time']),
    }).key_by(lambda x: x['label'])\
    .flat_map(HandleCountFlatMap(tag='stat_status_pod_group_by_type'))

    stat_replay_status_bag_group_by_category=stream.filter(lambda x:x.workflow_type=='replay').flat_map(AddBagCount())\
        .map(lambda x:{'count_failure':x['count_failure'],
                       'value':x['value'],
                       'count_success':x['count_success'],
                       'label':x['value']['category'],
                        'daytime_int':timestr_to_day_int(x['value']['update_time']),
                        'time_int':str_to_timestamp(x['value']['update_time']),
                        'time':x['value']['update_time'],
                        'daytime':timestr_to_daystr(x['value']['update_time']),
                        'minutetime_int':timestr_to_minute_int(x['value']['update_time']),
                        'minutetime':timestr_to_minutestr(x['value']['update_time']),
                        'weektime':timestr_to_weekstr(x['value']['update_time']),
                        'weektime_int':timestr_to_week_int(x['value']['update_time']),
                        'monthtime':timestr_to_monthstr(x['value']['update_time']),
                        'monthtime_int':timestr_to_month_int(x['value']['update_time']),
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
                        'daytime_int':timestr_to_day_int(x['value']['update_time']),
                        'time_int':str_to_timestamp(x['value']['update_time']),
                        'time':x['value']['update_time'],
                        'daytime':timestr_to_daystr(x['value']['update_time']),
                        'minutetime_int':timestr_to_minute_int(x['value']['update_time']),
                        'minutetime':timestr_to_minutestr(x['value']['update_time']),
                        'weektime':timestr_to_weekstr(x['value']['update_time']),
                        'weektime_int':timestr_to_week_int(x['value']['update_time']),
                        'monthtime':timestr_to_monthstr(x['value']['update_time']),
                        'monthtime_int':timestr_to_month_int(x['value']['update_time']),
                       })\
        .key_by(lambda x: x['label'])\
        .flat_map(HandleCountFlatMap(tag='stat_replay_status_bag_group_by_mode'))

    stat_status_bag_group_by_type=stream.filter(lambda x:x.workflow_type!='probe_detect').flat_map(AddBagCount())\
        .map(lambda x:{'count_failure':x['count_failure'],
                       'value':x['value'],
                       'count_success':x['count_success'],
                       'label':x['value']['workflow_type'],
                        'daytime_int':timestr_to_day_int(x['value']['update_time']),
                        'time_int':str_to_timestamp(x['value']['update_time']),
                        'time':x['value']['update_time'],
                        'daytime':timestr_to_daystr(x['value']['update_time']),
                        'minutetime_int':timestr_to_minute_int(x['value']['update_time']),
                        'minutetime':timestr_to_minutestr(x['value']['update_time']),
                        'weektime':timestr_to_weekstr(x['value']['update_time']),
                        'weektime_int':timestr_to_week_int(x['value']['update_time']),
                        'monthtime':timestr_to_monthstr(x['value']['update_time']),
                        'monthtime_int':timestr_to_month_int(x['value']['update_time']),
                       })\
        .key_by(lambda x: x['label']).flat_map(HandleCountFlatMap(tag='stat_status_bag_group_by_type'))


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute("stat_pod")
