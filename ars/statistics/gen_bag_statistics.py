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
        print('gen_bag_stat', result['update_time'])
        yield result


class HandleDurationFlatMap(FlatMapFunction):
    def __init__(self, tag: str = 'noraml') -> None:
        self.tag = tag
        self.minute_data = None  # 分钟0秒stamp: return_dict
        self.day_data = None,  # 天0点stamp: return_dict
        self.changed_days = None,  # 修改但未持久化的天stamp作为key，当hashset用
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
            'duration': value['duration'],
            'daytime_int': value['daytime_int'],
            'daytime': value['daytime'],
            'minutetime': value['minutetime'],
            'minutetime_int': value['minutetime_int'],
        }
        count_json = self.minute_data.get(value['minutetime_int'])
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
                self.update_day_state(value)

        else:  # 该分钟有过数据了，count_json先更新为加上这次数据的这分钟数据
            count_json = json.loads(count_json)
            count_json['duration'] += value['duration']
            self.minute_data.put(value['minutetime_int'],
                                 json.dumps(count_json))
            if value['minutetime_int'] != max_minute_int:  # 不是的最新的分钟
                self.update_day_state(value)
        return iter([])

    def stat_duration(self, json_str):
        json_mysql = json.loads(json_str)
        stat_date = json_mysql['daytime'] + ' 00:00:00'
        name = self.tag
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

    def record_minute(self, minute_json: str):
        minute_json = json.loads(minute_json)
        self.update_day_state(minute_json)
        for daytime_int_one in self.changed_days.keys():
            self.stat_duration(self.day_data.get(daytime_int_one))
        self.changed_days.clear()

    def update_day_state(self, new_dict: dict):
        daytime_int = new_dict['daytime_int']
        day_json = self.day_data.get(daytime_int)
        self.changed_days.put(daytime_int, 1)
        if day_json == None:
            self.day_data.put(
                daytime_int,
                json.dumps({
                    'daytime_int': daytime_int,
                    'daytime': new_dict['daytime'],
                    'label': new_dict['label'],
                    'duration': new_dict['duration'],
                }))
        else:
            day_json = json.loads(day_json)
            day_json['duration'] += new_dict['duration']
            self.day_data.put(daytime_int, json.dumps(day_json))


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
        .flat_map(HandleDurationFlatMap(tag='REALTIME_stat_replay_success_bag_duration_group_by_mode'))

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
        .flat_map(HandleDurationFlatMap(tag='REALTIME_stat_replay_success_bag_duration_group_by_category'))


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute()
