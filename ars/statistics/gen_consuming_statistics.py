import logging
import json
from pyflink.common import (
    Types, )
from pyflink.datastream import (StreamExecutionEnvironment, FlatMapFunction,
                                RuntimeContext, FilterFunction)
from pyflink.datastream.state import MapStateDescriptor
from lib.utils.sql import StatisticsActions
from lib.common.settings import *
from lib.utils.dates import *
from lib.common.schema import TEST_ARS_WORKFLOW_SCHEMA
from lib.utils.kafka import get_flink_kafka_consumer

logger = logging.getLogger(__name__)


class Filter(FilterFunction):
    def filter(self, value):
        return len(dict(json.loads(value.metric)).keys()) != 0


class MyflatmapFunction(FlatMapFunction):
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
        print('gen_consuming', value['time'])
        count_json = self.minute_data.get(value['minutetime_int'])
        result = self.value_to_consuming(value, count_json, 'minute')
        max_minute_int = max(
            self.minute_data.keys()) if not self.minute_data.is_empty() else 0

        if max_minute_int - value[
                'minutetime_int'] > self.delay_time:  # 过期数据，丢弃
            return iter([])

        self.minute_data.put(value['minutetime_int'], json.dumps(result))
        if count_json is None:
            if max_minute_int == 0:  # kafka来的第一条数据
                return iter([])
            if value[
                    'minutetime_int'] > max_minute_int and max_minute_int != 0:  # 这是更大的分钟，把前一分钟数据持久化
                self.record_minute(self.minute_data.get(max_minute_int))
            else:  # 不是最新的分钟
                self.update_day_state(value)

        elif value['minutetime_int'] != max_minute_int:  # 该分钟有过数据了
            self.update_day_state(value)
        return iter([])

    def consuming_to_consuming(self, minute_json: dict, day_json: dict):
        info = day_json['info']
        for device, device_dict in dict(minute_json['info']).items():
            if device not in dict(info).keys():
                info[device] = device_dict
            else:
                for category, category_dict in dict(
                        minute_json['info'][device]).items():
                    if category not in dict(info[device]).keys():
                        info[device][category] = category_dict
                    else:
                        for status_label in ['total', 'SUCCESS', 'FAILURE']:
                            if len(
                                    dict(info[device][category][status_label]).
                                    keys()) == 0:
                                info[device][category][
                                    status_label] = category_dict[status_label]
                            elif len(dict(
                                    category_dict[status_label]).keys()) != 0:
                                for k, v in dict(info[device][category]
                                                 [status_label]).items():
                                    info[device][category][status_label][
                                        k] += category_dict[status_label][k]
        day_json['info'] = info
        return day_json

    def value_to_consuming(self, value, count_json, time_type):
        if time_type == 'minute':
            result = {
                'daytime_int': value['daytime_int'],
                'daytime': value['daytime'],
                'minutetime': value['minutetime'],
                'minutetime_int': value['minutetime_int'],
            }
        if time_type == 'day':
            result = {
                'daytime_int': value['daytime_int'],
                'daytime': value['daytime'],
            }
        if count_json == None:
            result['info'] = {
                value['device']: {
                    value['category']: value['bags_profile_summary']
                }
            }
        else:
            count_json_dict = json.loads(count_json)
            info = count_json_dict['info']
            if value['device'] not in info:
                info[value['device']] = {
                    value['category']: value['bags_profile_summary']
                }
            else:
                if value['category'] not in info[value['device']]:
                    info[value['device']][
                        value['category']] = value['bags_profile_summary']
                else:
                    for status_label in ['total', 'SUCCESS', 'FAILURE']:
                        if not info[value['device']][
                                value['category']][status_label]:
                            info[value['device']][
                                value['category']][status_label] = value[
                                    'bags_profile_summary'][status_label]
                        elif value['bags_profile_summary'][status_label]:
                            for k in info[value['device']][
                                    value['category']][status_label].keys():
                                info[value['device']][
                                    value['category']][status_label][
                                        k] += value['bags_profile_summary'][
                                            status_label][k]
            result['info'] = info
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
        if len(list_get) == 0:
            info = json_mysql['info']
            statis_one.add_statistics(
                name=name, period=period, stat_date=stat_date, info=info)
        else:
            info = list_get[0].info
            device = list(json_mysql['info'].keys())[0]
            val = json_mysql['info'][device]
            category = list(val.keys())[0]
            if device not in info:
                info[device] = val
            else:
                info[device][category] = val[category]
            statis_one.update_statistics(
                name=name, period=period, stat_date=stat_date, info=info)

    def record_minute(self, minute_json: str):
        minute_json = json.loads(minute_json)
        daytime_int = minute_json['daytime_int']
        day_json = self.day_data.get(daytime_int)
        self.changed_days.put(daytime_int, 1)
        if day_json == None:
            self.day_data.put(
                daytime_int,
                json.dumps({
                    'daytime_int': daytime_int,
                    'daytime': minute_json['daytime'],
                    'info': minute_json['info']
                }))
        else:
            if type(day_json) == type({}):
                day_json = json.dumps(day_json)
            day_json_dict = json.loads(day_json)
            day_json_new = self.consuming_to_consuming(minute_json,
                                                       day_json_dict)
            self.day_data.put(daytime_int, json.dumps(day_json_new))
        for daytime_int_one in self.changed_days.keys():

            self.stat_replay_time_consuming_group_by_category(
                json_str=self.day_data.get(daytime_int_one), tag=self.tag)

    def update_day_state(self, value: dict):
        daytime_int = value['daytime_int']
        day_json = self.day_data.get(daytime_int)
        if type(day_json) == type({}):
            day_json = json.dumps(day_json)
        self.changed_days.put(daytime_int, 1)
        day_json = self.value_to_consuming(value, day_json, 'day')
        self.day_data.put(daytime_int, json.dumps(day_json))


def analyse(env: StreamExecutionEnvironment):

    stream = env.add_source(
        get_flink_kafka_consumer(
            schema=TEST_ARS_WORKFLOW_SCHEMA,
            topic=KAFKA_TOPIC_OF_ARS_WORKFLOW,
            group_id='martin_stat_consuming',
            start_date=START_TIME))
    stat_replay_time_consuming_group_by_category = stream.filter(Filter()).map(
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
            }).key_by(lambda x:x['device']+x['category'])\
                .flat_map(MyflatmapFunction(tag='REALTIME_stat_replay_time_consuming_group_by_category'))


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute()
