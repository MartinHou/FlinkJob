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


class Filter(FilterFunction):
    def filter(self, value):
        return value.type=='replay' and value['status']=='SUCCESS' and value['output_bag']!='{}' \
            and 'bag_duration' in json.loads(value['metric'])


class AddTimeProcess(ProcessFunction):
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
            'mode':
            json.loads(value['config'])['extra_args']['mode'],  # 4种mode
            'duration':
            json.loads(value['metric'])['bag_duration'],  # 耗时在这
            'update_time':
            datetime.strftime(
                datetime.fromtimestamp(ctx.timestamp() / 1000),
                "%Y-%m-%d %H:%M:%S"),  # 访问消息的时间戳并将其转化为 datetime 对象
        }
        print(result['update_time'])
        yield result


class HandleDurationFlatMap(FlatMapFunction):
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
            'duration': value['duration'],
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
            count_json['duration'] += value['duration']

            if value['minutetime_int'] == max_time_int:  # 当前分钟时最新的分钟
                self.count_timer.put(value['minutetime_int'],
                                     json.dumps(count_json))
            elif max_time_int - value[
                    'minutetime_int'] <= self.delay_time:  # 当前分钟不是最新的分钟，且不超1天前
                self.count_timer.put(value['minutetime_int'],
                                     json.dumps(count_json))
                self.value_to_day(value)
        yield value

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
                    'duration': minute_json['duration'],
                }))
        else:
            day_json = json.loads(day_json)
            day_json['duration'] += minute_json['duration']
            self.day_count_timer.put(daytime_int, json.dumps(day_json))
        for daytime_int_one in self.day_temp_timer.keys():

            self.stat_duration(
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
                    'duration': value['duration'],
                }))
        else:
            day_json = json.loads(day_json)
            day_json['duration'] += value['duration']
            self.day_count_timer.put(daytime_int, json.dumps(day_json))

    def stat_duration(self, json_str, tag: str):
        json_mysql = json.loads(json_str)
        stat_date = json_mysql['daytime'] + ' 00:00:00'
        # name=tag.replace('status','success')
        name = tag
        period = 'daily'
        statis_one = StatisticsActions()
        list_get = list(
            statis_one.get_statistics(
                name=name, period=period, stat_date=stat_date))
        if len(list_get) == 0:
            info = {}
            info[json_mysql['label']] = json_mysql['duration']
            statis_one.add_statistics(
                name=name, period=period, stat_date=stat_date, info=info)
        else:
            info = list_get[0].info
            info[json_mysql['label']] = json_mysql['duration']
            statis_one.update_statistics(
                name=name, period=period, stat_date=stat_date, info=info)


def analyse(env):
    stream = env.add_source(
        get_flink_kafka_consumer(
            schema=TEST_ARS_BAG_SCHEMA,
            topic=KAFKA_TOPIC_OF_ARS_BAG,
            group_id='martin_stat_bag',
            start_date=START_TIME))

    stat_replay_success_bag_duration_group_by_mode=stream.filter(Filter())\
        .process(AddTimeProcess())\
        .map(lambda x:
            {
                **x,
                'label': x['mode'],
                'daytime_int':datetime_str_to_int(x['update_time']),
                'time_int':time_str_to_int(x['update_time']),
                'time':x['update_time'],
                'daytime':timestr_to_datestr(x['update_time']),
                'minutetime_int':timestr_to_minute_int(x['update_time']),
                'minutetime':timestr_to_minutestr(x['update_time']),
            })\
        .key_by(lambda x:x['label'])\
        .flat_map(HandleDurationFlatMap(tag='stat_replay_success_bag_duration_group_by_mode'))

    stat_replay_success_bag_duration_group_by_category=stream.filter(Filter())\
        .process(AddTimeProcess())\
        .map(lambda x:
            {
                **x,
                'label': x['group'],
                'daytime_int':datetime_str_to_int(x['update_time']),
                'time_int':time_str_to_int(x['update_time']),
                'time':x['update_time'],
                'daytime':timestr_to_datestr(x['update_time']),
                'minutetime_int':timestr_to_minute_int(x['update_time']),
                'minutetime':timestr_to_minutestr(x['update_time']),
            })\
        .key_by(lambda x:x['label'])\
        .flat_map(HandleDurationFlatMap(tag='stat_replay_success_bag_duration_group_by_category'))


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute()
