import logging
import json
from pyflink.common import (
    Types, )
from pyflink.datastream import (StreamExecutionEnvironment, FlatMapFunction,
                                RuntimeContext, FilterFunction, MapFunction)
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor, ValueState, MapState
from lib.utils.sql import StatisticsActions
from lib.common.settings import *
from lib.utils.dates import *
from lib.common.schema import TEST_ARS_WORKFLOW_SCHEMA
from lib.utils.kafka import get_flink_kafka_consumer
from lib.utils.utils import defaultdict2dict, add_value_to_dict, overwrite_value_to_dict, merge_dicts
from dateutil.relativedelta import relativedelta
import pandas as pd


class ConsumingMap(MapFunction):
    def map(self, value):
        dt = str_to_datetime(value['update_time'])
        return {
            'workflow_id':
            value.workflow_id,
            'datetime':
            dt,
            'daydt':
            dt_to_dayobj(dt),
            'weekdt':
            dt_to_weekobj(dt),
            'monthdt':
            dt_to_monthobj(dt),
            'device':
            value.device,
            'category':
            value.category,
            'bags_profile_summary':
            json.loads(value.metric)['bags_profile_summary']
        }


class ConsumingFlatMap(FlatMapFunction):
    def __init__(self, tag) -> None:
        self.statistics_action = StatisticsActions()
        self.tag = tag

        self.dt_yesterday = None
        self.dt_today = None
        self.dt_lastweek = None
        self.dt_thisweek = None
        self.dt_lastmonth = None
        self.dt_thismonth = None

        self.yesterday_data = None
        self.today_data = None
        self.this_week_data = None
        self.last_week_data = None
        self.this_month_data = None
        self.last_month_data = None
        self.last_fire_dt = None

    def fill_state(self, value, start_dt: datetime, end_dt: datetime,
                   state: ValueState) -> None:
        val = {}
        for date in pd.date_range(start_dt, end_dt)[:-1]:
            try:
                res = self.statistics_action.get_statistics(
                    name=self.tag, period='daily', stat_date=date)[0].info
                if value['device'] in res and value['category'] in res[
                        value['device']]:
                    val = merge_dicts(res[value['device']][value['category']],
                                      val)
            except IndexError:
                pass
            except Exception:
                print(start_dt, end_dt, date)
                raise
        state.update(json.dumps(val))

    def init(self, value) -> None:
        # print('init')
        if self.dt_yesterday.value() is None:
            self.dt_yesterday.update(
                datetime_to_str(value['daydt'] - timedelta(days=1)))
            self.fill_state(value, value['daydt'] - timedelta(days=1),
                            value['daydt'], self.yesterday_data)
        if self.dt_lastmonth.value() is None:
            self.dt_lastmonth.update(
                datetime_to_str(value['monthdt'] - relativedelta(months=1)))
            self.fill_state(value, value['monthdt'] - relativedelta(months=1),
                            value['monthdt'], self.last_month_data)
        if self.dt_lastweek.value() is None:
            self.dt_lastweek.update(
                datetime_to_str(value['weekdt'] - relativedelta(weeks=1)))
            self.fill_state(value, value['weekdt'] - relativedelta(weeks=1),
                            value['weekdt'], self.last_week_data)
        if self.dt_thismonth.value() is None:
            self.dt_thismonth.update(datetime_to_str(value['monthdt']))
            self.fill_state(value, value['monthdt'], value['daydt'],
                            self.this_month_data)
        if self.dt_thisweek.value() is None:
            self.dt_thisweek.update(datetime_to_str(value['weekdt']))
            self.fill_state(value, value['weekdt'], value['daydt'],
                            self.this_week_data)
        if self.dt_today.value() is None:
            self.dt_today.update(datetime_to_str(value['daydt']))
            self.today_data.update('{}')

    def check_expiration(self, daydt: datetime, weekdt: datetime,
                         monthdt: datetime) -> None:
        # print('check expiration')
        # check if day level data is expired
        if self.today_data.value() is not None:
            if daydt > str_to_datetime(self.dt_today.value()):
                self.yesterday_data.update(self.today_data.value())
                self.today_data.update('{}')
                self.dt_yesterday.update(self.dt_today.value())
                self.dt_today.update(datetime_to_str(daydt))
            elif str_to_datetime(
                    self.dt_yesterday.value()) < daydt < str_to_datetime(
                        self.dt_today.value()):
                # if yesterday dt is not today's dt - 1day, clear yesterday data
                self.dt_yesterday.update(datetime_to_str(daydt))
                self.yesterday_data.update('{}')

        # check if week level data is expired
        if self.this_week_data.value() is not None:
            if weekdt > str_to_datetime(self.dt_thisweek.value()):
                self.last_week_data.update(self.this_week_data.value())
                self.this_week_data.update('{}')
                self.dt_lastweek.update(self.dt_thisweek.value())
                self.dt_thisweek.update(datetime_to_str(weekdt))
            elif str_to_datetime(
                    self.dt_lastweek.value()) < weekdt < str_to_datetime(
                        self.dt_thisweek.value()):
                # if lastweek dt is not this week's - 1week, clear last week data
                self.dt_lastweek.update(datetime_to_str(weekdt))
                self.last_week_data.update('{}')

        # check if month level data is expired
        if self.this_month_data.value() is not None:
            if monthdt > str_to_datetime(self.dt_thismonth.value()):
                self.last_month_data.update(self.this_month_data.value())
                self.this_month_data.update('{}')
                self.dt_lastmonth.update(self.dt_thismonth.value())
                self.dt_thismonth.update(datetime_to_str(monthdt))
            elif str_to_datetime(
                    self.dt_lastmonth.value()) < monthdt < str_to_datetime(
                        self.dt_thismonth.value()):
                self.dt_lastmonth.update(datetime_to_str(monthdt))
                self.last_month_data.update('{}')

    def open(self, ctx: RuntimeContext) -> None:
        self.dt_yesterday = ctx.get_state(
            ValueStateDescriptor("dt_yesterday", Types.STRING()))
        self.dt_today = ctx.get_state(
            ValueStateDescriptor("dt_today", Types.STRING()))
        self.dt_lastweek = ctx.get_state(
            ValueStateDescriptor("dt_lastweek", Types.STRING()))
        self.dt_thisweek = ctx.get_state(
            ValueStateDescriptor("dt_thisweek", Types.STRING()))
        self.dt_lastmonth = ctx.get_state(
            ValueStateDescriptor("dt_lastmonth", Types.STRING()))
        self.dt_thismonth = ctx.get_state(
            ValueStateDescriptor("dt_thismonth", Types.STRING()))

        self.yesterday_data = ctx.get_state(
            ValueStateDescriptor("yesterday_data", Types.STRING()))
        self.today_data = ctx.get_state(
            ValueStateDescriptor("today_data", Types.STRING()))
        self.this_week_data = ctx.get_state(
            ValueStateDescriptor("this_week_data", Types.STRING()))
        self.last_week_data = ctx.get_state(
            ValueStateDescriptor("last_week_data", Types.STRING()))
        self.this_month_data = ctx.get_state(
            ValueStateDescriptor("this_month_data", Types.STRING()))
        self.last_month_data = ctx.get_state(
            ValueStateDescriptor("last_month_data", Types.STRING()))
        self.last_fire_dt = ctx.get_state(
            ValueStateDescriptor("last_fire_dt", Types.STRING()))

    def write_sql(self, device, category) -> None:
        def helper(period, dt, data):
            data = json.loads(data)
            res = list(
                self.statistics_action.get_statistics(
                    name=self.tag, period=period, stat_date=dt))
            if res:
                info = res[0].info
                overwrite_value_to_dict(info, data, device, category)
                self.statistics_action.update_statistics(
                    name=self.tag, period=period, stat_date=dt, info=info)
            else:
                self.statistics_action.add_statistics(
                    name=self.tag,
                    period=period,
                    stat_date=dt,
                    info={device: {
                        category: data
                    }})

        helper('daily', self.dt_today.value(), self.today_data.value())
        helper('daily', self.dt_yesterday.value(), self.yesterday_data.value())
        helper('weekly', self.dt_thisweek.value(), self.this_week_data.value())
        helper('weekly', self.dt_lastweek.value(), self.last_week_data.value())
        helper('monthly', self.dt_thismonth.value(),
               self.this_month_data.value())
        helper('monthly', self.dt_lastmonth.value(),
               self.last_month_data.value())

    def update_state(self, value, state: ValueState):
        # print('update state')
        bag_profile_summary = value['bags_profile_summary']
        state_dict = json.loads(state.value())
        try:
            state.update(
                json.dumps(merge_dicts(bag_profile_summary, state_dict)))
        except:
            with open('stat.txt', 'a') as f:
                f.write(
                    f"{value['workflow_id']}\n{value['bags_profile_summary']}\n"
                )
            raise

    def flat_map(self, value):
        self.init(value)
        self.check_expiration(value['daydt'], value['weekdt'],
                              value['monthdt'])
        if value['daydt'] < str_to_datetime(
                self.dt_yesterday.value()):  # expired
            return iter([])
        if value['daydt'] == str_to_datetime(
                self.dt_yesterday.value()):  # data from yesterday
            self.update_state(value, self.yesterday_data)
        elif value['daydt'] == str_to_datetime(
                self.dt_today.value()):  # data from today
            self.update_state(value, self.today_data)
        if value['weekdt'] == str_to_datetime(
                self.dt_lastweek.value()):  # data from last week
            self.update_state(value, self.last_week_data)
        elif value['weekdt'] == str_to_datetime(
                self.dt_thisweek.value()):  # data from this week
            self.update_state(value, self.this_week_data)
        if value['monthdt'] == str_to_datetime(
                self.dt_lastmonth.value()):  # data from last month
            self.update_state(value, self.last_month_data)
        elif value['monthdt'] == str_to_datetime(
                self.dt_thismonth.value()):  # data from this month
            self.update_state(value, self.this_month_data)

        if self.last_fire_dt.value(
        ) is None or value['datetime'] - str_to_datetime(
                self.last_fire_dt.value()) >= timedelta(minutes=1):
            self.last_fire_dt.update(datetime_to_str(value['datetime']))
            self.write_sql(value['device'], value['category'])
        yield {
            'time': datetime_to_str(value['datetime']),
            # 'day': datetime_to_str(value['daydt']),
            # 'week': datetime_to_str(value['weekdt']),
            # 'month': datetime_to_str(value['monthdt']),
            # 'day_data': self.today_data.value(),
            # 'week_data': self.this_week_data.value(),
            # 'month_data': self.this_month_data.value(),
        }


# ============== Count Pod & Bag down here ==============


class AddPodCount(MapFunction):
    def map(self, value):
        dt = str_to_datetime(value['update_time'])
        return {
            'workflow_id': value.workflow_id,
            'label': value.workflow_type,
            'datetime': dt,
            'daydt': dt_to_dayobj(dt),
            'weekdt': dt_to_weekobj(dt),
            'monthdt': dt_to_monthobj(dt),
            'count_failure': 1 if value['workflow_status'] == 'FAILURE' else 0,
            'count_success': 1 if value['workflow_status'] == 'SUCCESS' else 0,
        }


class AddBagCount(FlatMapFunction):
    def flat_map(self, value):
        dt = str_to_datetime(value['update_time'])
        workflow_input = value.workflow_input
        mode = None
        try:
            mode = json.loads(workflow_input)['extra_args']['mode']
        except Exception as e:
            with open('no_mode.txt', 'a') as f:
                f.write(f"{value.workflow_id}\n")
        d = {
            'workflow_id': value.workflow_id,
            'workflow_type': value.workflow_type,
            'mode': mode,
            'category': value.category,
            'datetime': dt,
            'daydt': dt_to_dayobj(dt),
            'weekdt': dt_to_weekobj(dt),
            'monthdt': dt_to_monthobj(dt),
        }
        workflow_output = json.loads(value.workflow_output)
        workflow_status = value.workflow_status
        count_success, count_failure = 0, 0
        if workflow_status == 'FAILURE':
            count_failure += value.bag_nums
            yield {
                **d,
                'count_failure': count_failure,
                'count_success': count_success,
            }
        elif workflow_status == 'SUCCESS' and 'bag_replayed_list' in workflow_output:
            for one in workflow_output['bag_replayed_list']:
                if one:
                    count_success += 1
                else:
                    count_failure += 1
            yield {
                **d,
                'count_failure': count_failure,
                'count_success': count_success,
            }
        else:
            with open(f'bag_missed.txt', 'a') as f:
                f.write(f"{value.workflow_id}\n")


class HandleCountFlatMap(FlatMapFunction):
    def __init__(self, tag) -> None:
        self.statistics_action = StatisticsActions()
        self.tag: str = tag

        self.dt_yesterday = None
        self.dt_today = None
        self.dt_lastweek = None
        self.dt_thisweek = None
        self.dt_lastmonth = None
        self.dt_thismonth = None

        self.yesterday_data = None
        self.today_data = None
        self.this_week_data = None
        self.last_week_data = None
        self.this_month_data = None
        self.last_month_data = None
        self.last_fire_dt = None

    def fill_state(self, value, start_dt: datetime, end_dt: datetime,
                   state: MapState) -> None:
        # print(f'fill {start_dt} to {end_dt}')
        val = {'success': 0, 'failure': 0}
        for date in pd.date_range(start_dt, end_dt)[:-1]:
            for status in val.keys():
                name = self.tag.replace('status', status)
                try:
                    res = self.statistics_action.get_statistics(
                        name=name, period='daily', stat_date=date)[0].info
                    val[status] += res[value['label']]
                except Exception as e:
                    # print(f'{name}, {date}, no data')
                    pass
        for status, cnt in val.items():
            state.put(status, cnt)

    def init(self, value) -> None:
        # print('init')
        if self.dt_yesterday.value() is None:
            self.dt_yesterday.update(
                datetime_to_str(value['daydt'] - timedelta(days=1)))
            self.fill_state(value, value['daydt'] - timedelta(days=1),
                            value['daydt'], self.yesterday_data)
        if self.dt_lastmonth.value() is None:
            self.dt_lastmonth.update(
                datetime_to_str(value['monthdt'] - relativedelta(months=1)))
            self.fill_state(value, value['monthdt'] - relativedelta(months=1),
                            value['monthdt'], self.last_month_data)
        if self.dt_lastweek.value() is None:
            self.dt_lastweek.update(
                datetime_to_str(value['weekdt'] - relativedelta(weeks=1)))
            self.fill_state(value, value['weekdt'] - relativedelta(weeks=1),
                            value['weekdt'], self.last_week_data)
        if self.dt_thismonth.value() is None:
            self.dt_thismonth.update(datetime_to_str(value['monthdt']))
            self.fill_state(value, value['monthdt'], value['daydt'],
                            self.this_month_data)
        if self.dt_thisweek.value() is None:
            self.dt_thisweek.update(datetime_to_str(value['weekdt']))
            self.fill_state(value, value['weekdt'], value['daydt'],
                            self.this_week_data)
        if self.dt_today.value() is None:
            self.dt_today.update(datetime_to_str(value['daydt']))
            for status in ['success', 'failure']:
                self.today_data.put(status, 0)

    def check_expiration(self, daydt: datetime, weekdt: datetime,
                         monthdt: datetime) -> None:
        # print('check expiration')
        # check if day level data is expired
        if not self.today_data.is_empty():
            if daydt > str_to_datetime(self.dt_today.value()):
                for status in ['success', 'failure']:
                    self.yesterday_data.put(status,
                                            self.today_data.get(status))
                    self.today_data.put(status, 0)
                self.dt_yesterday.update(self.dt_today.value())
                self.dt_today.update(datetime_to_str(daydt))
            elif str_to_datetime(
                    self.dt_yesterday.value()) < daydt < str_to_datetime(
                        self.dt_today.value()):
                # if yesterday dt is not today's dt - 1day, clear yesterday data
                self.dt_yesterday.update(datetime_to_str(daydt))
                for status in ['success', 'failure']:
                    self.yesterday_data.put(status, 0)

        # check if week level data is expired
        if not self.this_week_data.is_empty():
            if weekdt > str_to_datetime(self.dt_thisweek.value()):
                for status in ['success', 'failure']:
                    self.last_week_data.put(status,
                                            self.this_week_data.get(status))
                    self.this_week_data.put(status, 0)
                self.dt_lastweek.update(self.dt_thisweek.value())
                self.dt_thisweek.update(datetime_to_str(weekdt))
            elif str_to_datetime(
                    self.dt_lastweek.value()) < weekdt < str_to_datetime(
                        self.dt_thisweek.value()):
                # if lastweek dt is not this week's - 1week, clear last week data
                self.dt_lastweek.update(datetime_to_str(weekdt))
                for status in ['success', 'failure']:
                    self.last_week_data.put(status, 0)

        # check if month level data is expired
        if not self.this_month_data.is_empty():
            if monthdt > str_to_datetime(self.dt_thismonth.value()):
                for status in ['success', 'failure']:
                    self.last_month_data.put(status,
                                             self.this_month_data.get(status))
                    self.this_month_data.put(status, 0)
                self.dt_lastmonth.update(self.dt_thismonth.value())
                self.dt_thismonth.update(datetime_to_str(monthdt))
            elif str_to_datetime(
                    self.dt_lastmonth.value()) < monthdt < str_to_datetime(
                        self.dt_thismonth.value()):
                self.dt_lastmonth.update(datetime_to_str(monthdt))
                for status in ['success', 'failure']:
                    self.last_month_data.put(status, 0)

    def open(self, ctx: RuntimeContext) -> None:
        self.dt_yesterday = ctx.get_state(
            ValueStateDescriptor("dt_yesterday", Types.STRING()))
        self.dt_today = ctx.get_state(
            ValueStateDescriptor("dt_today", Types.STRING()))
        self.dt_lastweek = ctx.get_state(
            ValueStateDescriptor("dt_lastweek", Types.STRING()))
        self.dt_thisweek = ctx.get_state(
            ValueStateDescriptor("dt_thisweek", Types.STRING()))
        self.dt_lastmonth = ctx.get_state(
            ValueStateDescriptor("dt_lastmonth", Types.STRING()))
        self.dt_thismonth = ctx.get_state(
            ValueStateDescriptor("dt_thismonth", Types.STRING()))

        self.yesterday_data = ctx.get_map_state(
            MapStateDescriptor("yesterday_data", Types.STRING(), Types.INT()))
        self.today_data = ctx.get_map_state(
            MapStateDescriptor("today_data", Types.STRING(), Types.INT()))
        self.this_week_data = ctx.get_map_state(
            MapStateDescriptor("this_week_data", Types.STRING(), Types.INT()))
        self.last_week_data = ctx.get_map_state(
            MapStateDescriptor("last_week_data", Types.STRING(), Types.INT()))
        self.this_month_data = ctx.get_map_state(
            MapStateDescriptor("this_month_data", Types.STRING(), Types.INT()))
        self.last_month_data = ctx.get_map_state(
            MapStateDescriptor("last_month_data", Types.STRING(), Types.INT()))
        self.last_fire_dt = ctx.get_state(
            ValueStateDescriptor("last_fire_dt", Types.STRING()))

    def write_sql(self, label) -> None:
        def helper(period, dt, data: MapState):
            for status in ['success', 'failure']:
                name = self.tag.replace('status', status)
                res = list(
                    self.statistics_action.get_statistics(
                        name=name, period=period, stat_date=dt))
                if res:
                    info = res[0].info
                    info[label] = data.get(status)
                    self.statistics_action.update_statistics(
                        name=name, period=period, stat_date=dt, info=info)
                else:
                    self.statistics_action.add_statistics(
                        name=name,
                        period=period,
                        stat_date=dt,
                        info={label: data.get(status)})

        helper('daily', self.dt_today.value(), self.today_data)
        helper('daily', self.dt_yesterday.value(), self.yesterday_data)
        helper('weekly', self.dt_thisweek.value(), self.this_week_data)
        helper('weekly', self.dt_lastweek.value(), self.last_week_data)
        helper('monthly', self.dt_thismonth.value(), self.this_month_data)
        helper('monthly', self.dt_lastmonth.value(), self.last_month_data)

    def flat_map(self, value):
        self.init(value)
        self.check_expiration(value['daydt'], value['weekdt'],
                              value['monthdt'])

        if value['daydt'] < str_to_datetime(
                self.dt_yesterday.value()):  # expired
            return iter([])
        for status in ['success', 'failure']:
            key = f"count_{status}"
            if value['daydt'] == str_to_datetime(
                    self.dt_yesterday.value()):  # data from yesterday
                self.yesterday_data.put(
                    status,
                    self.yesterday_data.get(status) + value[key])
            elif value['daydt'] == str_to_datetime(
                    self.dt_today.value()):  # data from today
                self.today_data.put(status,
                                    self.today_data.get(status) + value[key])
            if value['weekdt'] == str_to_datetime(
                    self.dt_lastweek.value()):  # data from last week
                self.last_week_data.put(
                    status,
                    self.last_week_data.get(status) + value[key])
            elif value['weekdt'] == str_to_datetime(
                    self.dt_thisweek.value()):  # data from this week
                self.this_week_data.put(
                    status,
                    self.this_week_data.get(status) + value[key])
            if value['monthdt'] == str_to_datetime(
                    self.dt_lastmonth.value()):  # data from last month
                self.last_month_data.put(
                    status,
                    self.last_month_data.get(status) + value[key])
            elif value['monthdt'] == str_to_datetime(
                    self.dt_thismonth.value()):  # data from this month
                self.this_month_data.put(
                    status,
                    self.this_month_data.get(status) + value[key])

        if self.last_fire_dt.value(
        ) is None or value['datetime'] - str_to_datetime(
                self.last_fire_dt.value()) >= timedelta(minutes=1):
            self.last_fire_dt.update(datetime_to_str(value['datetime']))
            self.write_sql(value['label'])
        yield {
            'time': datetime_to_str(value['datetime']),
            # 'day': datetime_to_str(value['daydt']),
            # 'week': datetime_to_str(value['weekdt']),
            # 'month': datetime_to_str(value['monthdt']),
            # 'day_data': self.today_data.value(),
            # 'week_data': self.this_week_data.value(),
            # 'month_data': self.this_month_data.value(),
        }


def analyse(env: StreamExecutionEnvironment):

    stream = env.add_source(
        get_flink_kafka_consumer(
            schema=TEST_ARS_WORKFLOW_SCHEMA,
            topic=KAFKA_TOPIC_OF_ARS_WORKFLOW,
            group_id='stat_pod',
            start_date=START_TIME))

    # try:
    #     stat_replay_time_consuming_group_by_category = stream.filter(
    #             lambda x: x.metric != '{}' and 'bags_profile_summary' in json.loads(x.metric))\
    #         .map(ConsumingMap())\
    #         .key_by(lambda x:f"{x['device']}$${x['category']}")\
    #         .flat_map(ConsumingFlatMap(tag='stat_replay_time_consuming_group_by_category'))\
    #         .print()
    # except Exception as e:
    #     raise

    stat_status_pod_group_by_type = stream.map(AddPodCount())\
        .key_by(lambda x: x['label'])\
        .flat_map(HandleCountFlatMap(tag='stat_status_pod_group_by_type'))

    # stat_replay_status_bag_group_by_category=stream.filter(lambda x:x.workflow_type=='replay').flat_map(AddBagCount())\
    #     .map(lambda x: {
    #         **x,
    #         'label':x['category'],
    #     })\
    #     .key_by(lambda x: x['label'])\
    #     .flat_map(HandleCountFlatMap(tag='stat_replay_status_bag_group_by_category'))

    # stat_replay_status_bag_group_by_mode=stream.filter(lambda x:x.workflow_type=='replay').flat_map(AddBagCount())\
    #     .filter(lambda x: x['mode'] is not None)\
    #     .map(lambda x:{
    #             **x,
    #             'label':x['mode'],
    #     })\
    #     .key_by(lambda x: x['label'])\
    #     .flat_map(HandleCountFlatMap(tag='stat_replay_status_bag_group_by_mode'))

    # stat_status_bag_group_by_type=stream.filter(lambda x:x.workflow_type!='probe_detect').flat_map(AddBagCount())\
    #     .map(lambda x:{
    #             **x,
    #             'label':x['workflow_type'],
    #     })\
    #     .key_by(lambda x: x['label']).flat_map(HandleCountFlatMap(tag='stat_status_bag_group_by_type'))


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute("stat_pod")
