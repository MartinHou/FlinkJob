from pyflink.common import Types
from pyflink.datastream import (StreamExecutionEnvironment, FlatMapFunction,
                                RuntimeContext)
from pyflink.datastream.state import MapStateDescriptor
from datetime import datetime
from lib.utils.sql import StatisticsActions
from pyflink.datastream import ProcessFunction, FilterFunction
import json
from lib.common.settings import *
from lib.utils.dates import *
from lib.common.schema import TEST_ARS_BAG_SCHEMA
from lib.utils.kafka import get_flink_kafka_consumer
from collections import defaultdict
from lib.utils.utils import defaultdict2dict
import pandas as pd


class ConsumingFilter(FilterFunction):
    def filter(self, value):
        return value.type == 'replay' and value[
            'output_bag'] != '' and 'bag_duration' in json.loads(
                value['metric'])  # status?


class AddConsumingTimeProcess(ProcessFunction):
    def process_element(self, value, ctx: ProcessFunction.Context):
        result = {
            'type':
            value['type'],
            'status':
            value['status'],
            'device':
            value['device'],
            'group':
            value['group'],  #人的组
            'error_stage':
            value['error_stage'],
            'error_type':
            value['error_type'],
            'mode':
            json.loads(value['config'])['extra_args']['mode'],  # 4种mode
            'duration':
            json.loads(value['metric'])['bag_duration'],  # 耗时在这
            'update_time':
            datetime.strftime(
                datetime.fromtimestamp(ctx.timestamp() / 1000),
                "%Y-%m-%d %H:%M:%S"),  # 访问消息的时间戳并将其转化为 datetime 对象
        }
        print('gen_bag_stat', result['update_time'])
        yield result


class AddCrashTimeProcess(ProcessFunction):
    def process_element(self, value, ctx: ProcessFunction.Context):
        result = {
            'type':
            value['type'],
            'status':
            value['status'],
            'device':
            value['device'],
            'group':
            value['group'],  #人的组
            'error_stage':
            value['error_stage'],
            'error_type':
            value['error_type'],
            'update_time':
            datetime.strftime(
                datetime.fromtimestamp(ctx.timestamp() / 1000),
                "%Y-%m-%d %H:%M:%S"),  # 访问消息的时间戳并将其转化为 datetime 对象
        }
        # print('gen_crash_stat', result['update_time'])
        yield result


class HandleDurationFlatMap(FlatMapFunction):
    def __init__(self, tag: str = 'noraml') -> None:
        self.statistics_action = StatisticsActions()
        self.tag = tag
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
                'duration': 0,
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
            for date in pd.date_range(start_date, cur_date)[:-1]:
                try:
                    day_res = self.statistics_action.get_statistics(
                        name=self.tag, period='daily', stat_date=date)[0].info
                    if value['label'] in day_res:
                        inital['duration'] += day_res[value['label']]
                except Exception as e:
                    print(f'{self.tag}, {date}, no data')
            state.put(start_date_ts, json.dumps(inital))

        if not self.month_data.contains(value['monthtime_int']):
            init(value['monthtime'], value['daytime'], self.month_data)
        if not self.week_data.contains(value['weektime_int']):
            init(value['weektime'], value['daytime'], self.week_data)

        return_dict = {
            'label': value['label'],
            'duration': value['duration'],
            'daytime_int': value['daytime_int'],
            'daytime': value['daytime'],
            'minutetime': value['minutetime'],
            'minutetime_int': value['minutetime_int'],
            'weektime': value['weektime'],
            'weektime_int': value['weektime_int'],
            'monthtime': value['monthtime'],
            'monthtime_int': value['monthtime_int'],
        }
        count_json = self.minute_data.get(value['minutetime_int'])
        max_minute_int = max(self.minute_data.keys(
        )) if not self.minute_data.is_empty() else 0  # 目前为止最大的分钟时间戳

        # if max_minute_int - value[
        #         'minutetime_int'] > self.delay_time:  # 过期数据，丢弃
        #     return iter([])

        if count_json is None:  # 首次进入这个60秒窗口
            self.minute_data.put(value['minutetime_int'],
                                 json.dumps(return_dict))
            if max_minute_int == 0:  # kafka来的第一条数据
                self.update_state(value)
                return iter([])
            if value['minutetime_int'] > max_minute_int:  # 这是更大的分钟，把前一分钟数据持久化
                self.record_minute(self.minute_data.get(max_minute_int))
            else:  # 不是最新的分钟
                self.update_state(value)

        else:  # 该分钟有过数据了，count_json先更新为加上这次数据的这分钟数据
            count_json = json.loads(count_json)
            count_json['duration'] += value['duration']
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
        name = self.tag
        list_get = list(
            self.statistics_action.get_statistics(
                name=name, period=period, stat_date=stat_date))
        if len(list_get) == 0:
            info = {}
            info[json_mysql['label']] = json_mysql['duration']
            self.statistics_action.add_statistics(
                name=name, period=period, stat_date=stat_date, info=info)
        else:
            info = list_get[0].info
            info[json_mysql['label']] = json_mysql['duration']
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
            'label': data['label'],
            'duration': data['duration'],
            'daytime_int': daytime_int,
            'daytime': data['daytime'],
            'minutetime': data['minutetime'],
            'minutetime_int': data['minutetime_int'],
            'weektime': data['weektime'],
            'weektime_int': weektime_int,
            'monthtime': data['monthtime'],
            'monthtime_int': monthtime_int,
        }
        if not daytime_json:
            self.day_data.put(daytime_int, json.dumps(ret))
        else:
            daytime_json = json.loads(daytime_json)
            daytime_json['duration'] += data['duration']
            self.day_data.put(daytime_int, json.dumps(daytime_json))
        if not weektime_json:
            self.week_data.put(weektime_int, json.dumps(ret))
        else:
            weektime_json = json.loads(weektime_json)
            weektime_json['duration'] += data['duration']
            self.week_data.put(weektime_int, json.dumps(weektime_json))
        if not monthtime_json:
            self.month_data.put(monthtime_int, json.dumps(ret))
        else:
            monthtime_json = json.loads(monthtime_json)
            monthtime_json['duration'] += data['duration']
            self.month_data.put(monthtime_int, json.dumps(monthtime_json))


class HandleErrorFlatMap(FlatMapFunction):
    def __init__(self, tag: str = 'noraml') -> None:
        self.statistics_action = StatisticsActions()
        self.tag = tag
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
                'group': value['group'],
                'error_stage': value['error_stage'],
                'error_type': value['error_type'],
                'count': 0,
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
            for date in pd.date_range(start_date, cur_date)[:-1]:
                try:
                    day_res = self.statistics_action.get_statistics(
                        name=self.tag, period='daily', stat_date=date)[0].info
                    if value['group'] in day_res and value[
                            'error_stage'] in day_res[value[
                                'group']] and value['error_type'] in day_res[
                                    value['group']][value['error_stage']]:
                        inital['duration'] += day_res[value['group']][
                            value['error_stage']][value['error_type']]
                except Exception as e:
                    print(f'{self.tag}, {date}, no data. {e}')

            state.put(start_date_ts, json.dumps(inital))

        if not self.month_data.contains(value['monthtime_int']):
            init(value['monthtime'], value['daytime'], self.month_data)
        if not self.week_data.contains(value['weektime_int']):
            init(value['weektime'], value['daytime'], self.week_data)

        return_dict = {
            'group': value['group'],
            'error_stage': value['error_stage'],
            'error_type': value['error_type'],
            'count': 1,
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
            count_json['count'] += 1
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
        list_get = list(
            self.statistics_action.get_statistics(
                name=self.tag, period=period, stat_date=stat_date))
        if len(list_get) == 0:
            info = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
            info[json_mysql['group']][json_mysql['error_stage']][
                json_mysql['error_type']] = json_mysql['count']
            self.statistics_action.add_statistics(
                name=self.tag,
                period=period,
                stat_date=stat_date,
                info=defaultdict2dict(info))
        else:
            info = dict(list_get[0].info)
            info.setdefault(json_mysql['group'], {})
            info[json_mysql['group']].setdefault(json_mysql['error_stage'], {})
            info[json_mysql['group']][json_mysql['error_stage']][
                json_mysql['error_type']] = json_mysql['count']
            self.statistics_action.update_statistics(
                name=self.tag, period=period, stat_date=stat_date, info=info)

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
            'group': data['group'],
            'error_stage': data['error_stage'],
            'error_type': data['error_type'],
            'count': 1,
            'daytime_int': daytime_int,
            'daytime': data['daytime'],
            'minutetime': data['minutetime'],
            'minutetime_int': data['minutetime_int'],
            'weektime': data['weektime'],
            'weektime_int': weektime_int,
            'monthtime': data['monthtime'],
            'monthtime_int': monthtime_int,
        }
        if not daytime_json:
            self.day_data.put(daytime_int, json.dumps(ret))
        else:
            daytime_json = json.loads(daytime_json)
            daytime_json['count'] += data['count']
            self.day_data.put(daytime_int, json.dumps(daytime_json))
        if not weektime_json:
            self.week_data.put(weektime_int, json.dumps(ret))
        else:
            weektime_json = json.loads(weektime_json)
            weektime_json['count'] += data['count']
            self.week_data.put(weektime_int, json.dumps(weektime_json))
        if not monthtime_json:
            self.month_data.put(monthtime_int, json.dumps(ret))
        else:
            monthtime_json = json.loads(monthtime_json)
            monthtime_json['count'] += data['count']
            self.month_data.put(monthtime_int, json.dumps(monthtime_json))


def analyse(env):
    stream = env.add_source(
        get_flink_kafka_consumer(
            schema=TEST_ARS_BAG_SCHEMA,
            topic=KAFKA_TOPIC_OF_ARS_BAG,
            group_id='martin_stat_bag',
            start_date=START_TIME))

    stat_replay_success_bag_duration_group_by_mode=stream.filter(ConsumingFilter())\
        .process(AddConsumingTimeProcess())\
        .map(lambda x:
            {
                **x,
                'label': x['mode'],
                'daytime_int':timestr_to_day_int(x['update_time']),
                'time_int':str_to_timestamp(x['update_time']),
                'time':x['update_time'],
                'daytime':timestr_to_daystr(x['update_time']),
                'minutetime_int':timestr_to_minute_int(x['update_time']),
                'minutetime':timestr_to_minutestr(x['update_time']),
                'weektime':timestr_to_weekstr(x['update_time']),
                'weektime_int':timestr_to_week_int(x['update_time']),
                'monthtime':timestr_to_monthstr(x['update_time']),
                'monthtime_int':timestr_to_month_int(x['update_time']),
            })\
        .key_by(lambda x:x['label'])\
        .flat_map(HandleDurationFlatMap(tag='stat_replay_success_bag_duration_group_by_mode'))

    stat_replay_success_bag_duration_group_by_category=stream.filter(ConsumingFilter())\
        .process(AddConsumingTimeProcess())\
        .map(lambda x:
            {
                **x,
                'label': x['group'],
                'daytime_int':timestr_to_day_int(x['update_time']),
                'time_int':str_to_timestamp(x['update_time']),
                'time':x['update_time'],
                'daytime':timestr_to_daystr(x['update_time']),
                'minutetime_int':timestr_to_minute_int(x['update_time']),
                'minutetime':timestr_to_minutestr(x['update_time']),
                'weektime':timestr_to_weekstr(x['update_time']),
                'weektime_int':timestr_to_week_int(x['update_time']),
                'monthtime':timestr_to_monthstr(x['update_time']),
                'monthtime_int':timestr_to_month_int(x['update_time']),
            })\
        .key_by(lambda x:x['label'])\
        .flat_map(HandleDurationFlatMap(tag='stat_replay_success_bag_duration_group_by_category'))

    stat_replay_error_bag_count_group_by_category = stream.filter(lambda x:x.type == 'replay' and x.error_type!='')\
        .process(AddCrashTimeProcess())\
        .map(lambda x:
            {
                'group':x['group'],
                'error_stage': x['error_stage'],
                'error_type': x['error_type'],
                'count': 1,
                'daytime_int':timestr_to_day_int(x['update_time']),
                'time_int':str_to_timestamp(x['update_time']),
                'time':x['update_time'],
                'daytime':timestr_to_daystr(x['update_time']),
                'minutetime_int':timestr_to_minute_int(x['update_time']),
                'minutetime':timestr_to_minutestr(x['update_time']),
                'weektime':timestr_to_weekstr(x['update_time']),
                'weektime_int':timestr_to_week_int(x['update_time']),
                'monthtime':timestr_to_monthstr(x['update_time']),
                'monthtime_int':timestr_to_month_int(x['update_time']),
            })\
        .key_by(lambda x:x['group']+x['error_stage']+x['error_type'])\
        .flat_map(HandleErrorFlatMap(tag='stat_replay_error_bag_count_group_by_category'))


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute("stat_bag")
