import logging
from datetime import datetime
import json
from pyflink.common import (
    Types, )
from pyflink.datastream import (StreamExecutionEnvironment, FlatMapFunction,
                                RuntimeContext, FilterFunction)
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.state import MapStateDescriptor
from createsql import StatisticsActions
from lib.common.settings import *
from lib.utils.dates import *
from lib.common.schema import TEST_ARS_WORKFLOW_SCHEMA
from lib.utils.kafka import get_flink_kafka_consumer

logger = logging.getLogger(__name__)


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
        print(value['time'])
        count_json = self.count_timer.get(value['minutetime_int'])
        result = self.value_to_consuming(value, count_json, 'minute')
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
            if 'info' not in dict(count_json_dict):
                print(count_json_dict)
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
                    'info': minute_json['info']
                }))
        else:
            if type(day_json) == type({}):
                day_json = json.dumps(day_json)
            day_json_dict = json.loads(day_json)
            day_json_new = self.consuming_to_consuming(minute_json,
                                                       day_json_dict)
            self.day_count_timer.put(daytime_int, json.dumps(day_json_new))
        for daytime_int_one in self.day_temp_timer.keys():

            self.stat_replay_time_consuming_group_by_category(
                json_str=self.day_count_timer.get(daytime_int_one),
                tag=self.tag)

    def value_to_day(self, value: dict):
        daytime_int = value['daytime_int']
        day_json = self.day_count_timer.get(daytime_int)
        if type(day_json) == type({}):
            day_json = json.dumps(day_json)
        self.day_temp_timer.put(daytime_int, 1)
        day_json = self.value_to_consuming(value, day_json, 'day')
        self.day_count_timer.put(daytime_int, json.dumps(day_json))


class Filter(FilterFunction):
    def filter(self, value):
        return len(dict(json.loads(value.metric)).keys()) != 0


def analyse(env: StreamExecutionEnvironment):

    stream = env.add_source(
        get_flink_kafka_consumer(
            schema=TEST_ARS_WORKFLOW_SCHEMA,
            topic=KAFKA_TOPIC_OF_ARS_WORKFLOW,
            group_id='martin_test01',
            start_date=START_TIME))
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
            }).key_by(lambda x:x['device']+x['category'])\
                .flat_map(MyflatmapFunction(tag='stat_replay_time_consuming_group_by_category'))


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute()
