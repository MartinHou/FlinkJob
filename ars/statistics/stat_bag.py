from pyflink.common import Types
from pyflink.datastream import (StreamExecutionEnvironment, FlatMapFunction,
                                RuntimeContext)
from pyflink.datastream.state import MapStateDescriptor, ValueState, MapState, ValueStateDescriptor
from datetime import datetime
from lib.utils.sql import StatisticsActions
from pyflink.datastream import ProcessFunction, FilterFunction
import json
from lib.common.settings import *
from lib.utils.dates import *
from lib.common.schema import TEST_ARS_BAG_SCHEMA
from lib.utils.kafka import get_flink_kafka_consumer
from collections import defaultdict
from lib.utils.utils import defaultdict2dict,add_value_to_dict,overwrite_value_to_dict
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import create_engine

class BagDurationFilter(FilterFunction):
    def filter(self, value):
        return value.type=='replay' and value['output_bag']!='' and 'bag_duration' in json.loads(value['metric'])

class AddDurationTimeProcess(ProcessFunction):
    def process_element(self, value, ctx: ProcessFunction.Context):
        update_time_ts = int(ctx.timestamp() / 1000)
        update_time_dt = datetime.fromtimestamp(update_time_ts)
        result = {
            'group': value['group'],
            'mode': json.loads(value['config'])['extra_args']['mode'],
            'duration': json.loads(value['metric'])['bag_duration'],
            'datetime': update_time_dt,
            # 'timestamp': update_time_ts,
            'daydt': dt_to_dayobj(update_time_dt),
            # 'dayts': dt_to_dayint(update_time_dt),
            'weekdt': dt_to_weekobj(update_time_dt),
            # 'weekts': dt_to_weekint(update_time_dt),
            'monthdt': dt_to_monthobj(update_time_dt),
            # 'monthts': dt_to_monthint(update_time_dt),
        }
        yield result
        
class HandleDurationFlatMap(FlatMapFunction):
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
        
        
    def fill_state(self, value, start_dt: datetime, end_dt: datetime, state: ValueState) -> None:
        # print(f'fill {start_dt} to {end_dt}')
        val = 0
        for date in pd.date_range(start_dt,end_dt)[:-1]:
            try:
                res = self.statistics_action.get_statistics(name=self.tag,period='daily',stat_date=date)[0].info
                val += res[value['label']]
            except Exception as e:
                # print(f'{self.tag}, {date}, no data')
                pass
        state.update(val)
        
    def init(self, value) -> None:
        # print('init')
        if self.dt_yesterday.value() is None:
            self.dt_yesterday.update(datetime_to_str(value['daydt']-timedelta(days=1)))
            self.fill_state(value, value['daydt']-timedelta(days=1), value['daydt'], self.yesterday_data)
        if self.dt_lastmonth.value() is None:
            self.dt_lastmonth.update(datetime_to_str(value['monthdt']-relativedelta(months=1)))
            self.fill_state(value, value['monthdt']-relativedelta(months=1), value['monthdt'], self.last_month_data)
        if self.dt_lastweek.value() is None:
            self.dt_lastweek.update(datetime_to_str(value['weekdt']-relativedelta(weeks=1)))
            self.fill_state(value, value['weekdt']-relativedelta(weeks=1), value['weekdt'], self.last_week_data)
        if self.dt_thismonth.value() is None:
            self.dt_thismonth.update(datetime_to_str(value['monthdt']))
            self.fill_state(value, value['monthdt'], value['daydt'], self.this_month_data)
        if self.dt_thisweek.value() is None:
            self.dt_thisweek.update(datetime_to_str(value['weekdt']))
            self.fill_state(value, value['weekdt'], value['daydt'], self.this_week_data)
        if self.dt_today.value() is None:
            self.dt_today.update(datetime_to_str(value['daydt']))
            self.today_data.update(0)
        
    def check_expiration(self, daydt: datetime, weekdt: datetime, monthdt: datetime) -> None:
        # print('check expiration')
        # check if day level data is expired
        if self.today_data.value() is not None:
            if daydt>str_to_datetime(self.dt_today.value()):
                self.yesterday_data.update(self.today_data.value())
                self.today_data.update(0)
                self.dt_yesterday.update(self.dt_today.value())
                self.dt_today.update(datetime_to_str(daydt))
            elif str_to_datetime(self.dt_yesterday.value())<daydt<str_to_datetime(self.dt_today.value()):   
                # if yesterday dt is not today's dt - 1day, clear yesterday data
                self.dt_yesterday.update(datetime_to_str(daydt))
                self.yesterday_data.update(0)
    
        # check if week level data is expired
        if self.this_week_data.value() is not None:
            if weekdt>str_to_datetime(self.dt_thisweek.value()):
                self.last_week_data.update(self.this_week_data.value())
                self.this_week_data.update(0)
                self.dt_lastweek.update(self.dt_thisweek.value())
                self.dt_thisweek.update(datetime_to_str(weekdt))
            elif str_to_datetime(self.dt_lastweek.value())<weekdt<str_to_datetime(self.dt_thisweek.value()):   
                # if lastweek dt is not this week's - 1week, clear last week data
                self.dt_lastweek.update(datetime_to_str(weekdt))
                self.last_week_data.update(0)
                
        # check if month level data is expired
        if self.this_month_data.value() is not None:
            if monthdt>str_to_datetime(self.dt_thismonth.value()):
                self.last_month_data.update(self.this_month_data.value())
                self.this_month_data.update(0)
                self.dt_lastmonth.update(self.dt_thismonth.value())
                self.dt_thismonth.update(datetime_to_str(monthdt))
            elif str_to_datetime(self.dt_lastmonth.value())<monthdt<str_to_datetime(self.dt_thismonth.value()):
                self.dt_lastmonth.update(datetime_to_str(monthdt))
                self.last_month_data.update(0)
        
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
            ValueStateDescriptor("yesterday_data", Types.FLOAT()))
        self.today_data = ctx.get_state(
            ValueStateDescriptor("today_data", Types.FLOAT()))
        self.this_week_data = ctx.get_state(
            ValueStateDescriptor("this_week_data", Types.FLOAT()))
        self.last_week_data = ctx.get_state(
            ValueStateDescriptor("last_week_data", Types.FLOAT()))
        self.this_month_data = ctx.get_state(
            ValueStateDescriptor("this_month_data", Types.FLOAT()))
        self.last_month_data = ctx.get_state(
            ValueStateDescriptor("last_month_data", Types.FLOAT()))
        self.last_fire_dt = ctx.get_state(
            ValueStateDescriptor("last_fire_dt", Types.STRING()))
        
    def write_sql(self, label) -> None:
        def helper(period, dt, data):
            res = list(self.statistics_action.get_statistics(name=self.tag,period=period,stat_date=dt))
            if res:
                info = res[0].info
                info[label] = data
                self.statistics_action.update_statistics(name=self.tag,period=period,stat_date=dt,info=info)
            else:
                self.statistics_action.add_statistics(name=self.tag,period=period,stat_date=dt,info={label:data})
                
        helper('daily', self.dt_today.value(), self.today_data.value())
        helper('daily', self.dt_yesterday.value(), self.yesterday_data.value())
        helper('weekly', self.dt_thisweek.value(), self.this_week_data.value())
        helper('weekly', self.dt_lastweek.value(), self.last_week_data.value())
        helper('monthly', self.dt_thismonth.value(), self.this_month_data.value())
        helper('monthly', self.dt_lastmonth.value(), self.last_month_data.value())
    
    def flat_map(self, value):
        self.init(value)
        self.check_expiration(value['daydt'], value['weekdt'], value['monthdt'])
        
        if value['daydt'] < str_to_datetime(self.dt_yesterday.value()):   # expired
            return iter([])
        if value['daydt'] == str_to_datetime(self.dt_yesterday.value()): # data from yesterday
            self.yesterday_data.update(self.yesterday_data.value()+float(value['duration']))
        elif value['daydt'] == str_to_datetime(self.dt_today.value()):   # data from today
            self.today_data.update(self.today_data.value()+float(value['duration']))
        if value['weekdt'] == str_to_datetime(self.dt_lastweek.value()):   # data from last week
            self.last_week_data.update(self.last_week_data.value()+float(value['duration']))
        elif value['weekdt'] == str_to_datetime(self.dt_thisweek.value()):   # data from this week
            self.this_week_data.update(self.this_week_data.value()+float(value['duration']))
        if value['monthdt'] == str_to_datetime(self.dt_lastmonth.value()):  # data from last month
            self.last_month_data.update(self.last_month_data.value()+float(value['duration']))
        elif value['monthdt'] == str_to_datetime(self.dt_thismonth.value()):    # data from this month
            self.this_month_data.update(self.this_month_data.value()+float(value['duration']))
            
        if self.last_fire_dt.value() is None or value['datetime']-str_to_datetime(self.last_fire_dt.value())>=timedelta(minutes=1):
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
            # 'label': value['label'],
        }
        
# ================= Count Error bag down here! =======================
        
class CountErrorTimeProcess(ProcessFunction):
    def process_element(self, value, ctx: ProcessFunction.Context):
        update_time_ts = int(ctx.timestamp() / 1000)
        update_time_dt = datetime.fromtimestamp(update_time_ts)
        result = {
            'datetime': update_time_dt,
            'daydt': dt_to_dayobj(update_time_dt),
            'weekdt': dt_to_weekobj(update_time_dt),
            'monthdt': dt_to_monthobj(update_time_dt),
            'group':value['group'],
            'error_stage':value['error_stage'],
            'error_type':value['error_type'],
        }
        yield result


class HandleErrorFlatMap(FlatMapFunction):
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
        
        
    def fill_state(self, value, start_dt: datetime, end_dt: datetime, state: ValueState) -> None:
        # print(f'fill {start_dt} to {end_dt}')
        val = 0
        for date in pd.date_range(start_dt,end_dt)[:-1]:
            try:
                res = self.statistics_action.get_statistics(name=self.tag,period='daily',stat_date=date)[0].info
                val += res[value['group']][value['error_stage']][value['error_type']]
            except Exception as e:
                # print(f'{self.tag}, {date}, no data')
                pass
        state.update(val)
        
    def init(self, value) -> None:
        # print('init')
        if self.dt_yesterday.value() is None:
            self.dt_yesterday.update(datetime_to_str(value['daydt']-timedelta(days=1)))
            self.fill_state(value, value['daydt']-timedelta(days=1), value['daydt'], self.yesterday_data)
        if self.dt_lastmonth.value() is None:
            self.dt_lastmonth.update(datetime_to_str(value['monthdt']-relativedelta(months=1)))
            self.fill_state(value, value['monthdt']-relativedelta(months=1), value['monthdt'], self.last_month_data)
        if self.dt_lastweek.value() is None:
            self.dt_lastweek.update(datetime_to_str(value['weekdt']-relativedelta(weeks=1)))
            self.fill_state(value, value['weekdt']-relativedelta(weeks=1), value['weekdt'], self.last_week_data)
        if self.dt_thismonth.value() is None:
            self.dt_thismonth.update(datetime_to_str(value['monthdt']))
            self.fill_state(value, value['monthdt'], value['daydt'], self.this_month_data)
        if self.dt_thisweek.value() is None:
            self.dt_thisweek.update(datetime_to_str(value['weekdt']))
            self.fill_state(value, value['weekdt'], value['daydt'], self.this_week_data)
        if self.dt_today.value() is None:
            self.dt_today.update(datetime_to_str(value['daydt']))
            self.today_data.update(0)
        
    def check_expiration(self, daydt: datetime, weekdt: datetime, monthdt: datetime) -> None:
        # print('check expiration')
        # check if day level data is expired
        if self.today_data.value() is not None:
            if daydt>str_to_datetime(self.dt_today.value()):
                self.yesterday_data.update(self.today_data.value())
                self.today_data.update(0)
                self.dt_yesterday.update(self.dt_today.value())
                self.dt_today.update(datetime_to_str(daydt))
            elif str_to_datetime(self.dt_yesterday.value())<daydt<str_to_datetime(self.dt_today.value()):   
                # if yesterday dt is not today's dt - 1day, clear yesterday data
                self.dt_yesterday.update(datetime_to_str(daydt))
                self.yesterday_data.update(0)
    
        # check if week level data is expired
        if self.this_week_data.value() is not None:
            if weekdt>str_to_datetime(self.dt_thisweek.value()):
                self.last_week_data.update(self.this_week_data.value())
                self.this_week_data.update(0)
                self.dt_lastweek.update(self.dt_thisweek.value())
                self.dt_thisweek.update(datetime_to_str(weekdt))
            elif str_to_datetime(self.dt_lastweek.value())<weekdt<str_to_datetime(self.dt_thisweek.value()):   
                # if lastweek dt is not this week's - 1week, clear last week data
                self.dt_lastweek.update(datetime_to_str(weekdt))
                self.last_week_data.update(0)
                
        # check if month level data is expired
        if self.this_month_data.value() is not None:
            if monthdt>str_to_datetime(self.dt_thismonth.value()):
                self.last_month_data.update(self.this_month_data.value())
                self.this_month_data.update(0)
                self.dt_lastmonth.update(self.dt_thismonth.value())
                self.dt_thismonth.update(datetime_to_str(monthdt))
            elif str_to_datetime(self.dt_lastmonth.value())<monthdt<str_to_datetime(self.dt_thismonth.value()):
                self.dt_lastmonth.update(datetime_to_str(monthdt))
                self.last_month_data.update(0)
        
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
            ValueStateDescriptor("yesterday_data", Types.INT()))
        self.today_data = ctx.get_state(
            ValueStateDescriptor("today_data", Types.INT()))
        self.this_week_data = ctx.get_state(
            ValueStateDescriptor("this_week_data", Types.INT()))
        self.last_week_data = ctx.get_state(
            ValueStateDescriptor("last_week_data", Types.INT()))
        self.this_month_data = ctx.get_state(
            ValueStateDescriptor("this_month_data", Types.INT()))
        self.last_month_data = ctx.get_state(
            ValueStateDescriptor("last_month_data", Types.INT()))
        self.last_fire_dt = ctx.get_state(
            ValueStateDescriptor("last_fire_dt", Types.STRING()))
        
    def write_sql(self, group, err_stage, err_type) -> None:
        def helper(period, dt, data):
            res = list(self.statistics_action.get_statistics(name=self.tag,period=period,stat_date=dt))
            if res:
                info = res[0].info
                overwrite_value_to_dict(info, data, group, err_stage, err_type)
                self.statistics_action.update_statistics(name=self.tag,period=period,stat_date=dt,info=info)
            else:
                self.statistics_action.add_statistics(name=self.tag,period=period,stat_date=dt,info={group:{err_stage:{err_type:data}}})
                
        helper('daily', self.dt_today.value(), self.today_data.value())
        helper('daily', self.dt_yesterday.value(), self.yesterday_data.value())
        helper('weekly', self.dt_thisweek.value(), self.this_week_data.value())
        helper('weekly', self.dt_lastweek.value(), self.last_week_data.value())
        helper('monthly', self.dt_thismonth.value(), self.this_month_data.value())
        helper('monthly', self.dt_lastmonth.value(), self.last_month_data.value())
    
    def flat_map(self, value):
        self.init(value)
        self.check_expiration(value['daydt'], value['weekdt'], value['monthdt'])
        
        if value['daydt'] < str_to_datetime(self.dt_yesterday.value()):   # expired
            return iter([])
        if value['daydt'] == str_to_datetime(self.dt_yesterday.value()): # data from yesterday
            self.yesterday_data.update(self.yesterday_data.value()+1)
        elif value['daydt'] == str_to_datetime(self.dt_today.value()):   # data from today
            self.today_data.update(self.today_data.value()+1)
        if value['weekdt'] == str_to_datetime(self.dt_lastweek.value()):   # data from last week
            self.last_week_data.update(self.last_week_data.value()+1)
        elif value['weekdt'] == str_to_datetime(self.dt_thisweek.value()):   # data from this week
            self.this_week_data.update(self.this_week_data.value()+1)
        if value['monthdt'] == str_to_datetime(self.dt_lastmonth.value()):  # data from last month
            self.last_month_data.update(self.last_month_data.value()+1)
        elif value['monthdt'] == str_to_datetime(self.dt_thismonth.value()):    # data from this month
            self.this_month_data.update(self.this_month_data.value()+1)
            
        if self.last_fire_dt.value() is None or value['datetime']-str_to_datetime(self.last_fire_dt.value())>=timedelta(minutes=1):
            self.last_fire_dt.update(datetime_to_str(value['datetime']))
            self.write_sql(value['group'],value['error_stage'],value['error_type'])
        yield {
            'time': datetime_to_str(value['datetime']),
            # 'day': datetime_to_str(value['daydt']),
            # 'week': datetime_to_str(value['weekdt']),
            # 'month': datetime_to_str(value['monthdt']),
            # 'day_data': self.today_data.value(),
            # 'week_data': self.this_week_data.value(),
            # 'month_data': self.this_month_data.value(),
        }
        

def analyse(env):
    stream = env.add_source(
        get_flink_kafka_consumer(
            schema=TEST_ARS_BAG_SCHEMA,
            topic=KAFKA_TOPIC_OF_ARS_BAG,
            group_id='stat_bag',
            start_date=START_TIME))
    
    try:
        stat_replay_success_bag_duration_group_by_mode=stream.filter(BagDurationFilter())\
            .process(AddDurationTimeProcess())\
            .map(lambda x:
                {
                    **x,
                    'label': x['mode'],
                })\
            .key_by(lambda x:x['label'])\
            .flat_map(HandleDurationFlatMap(tag='stat_replay_success_bag_duration_group_by_mode'))\
            .print()

        stat_replay_success_bag_duration_group_by_category=stream.filter(BagDurationFilter())\
            .process(AddDurationTimeProcess())\
            .map(lambda x:
                {
                    **x,
                    'label': x['group'],
                })\
            .key_by(lambda x:x['label'])\
            .flat_map(HandleDurationFlatMap(tag='stat_replay_success_bag_duration_group_by_category'))\
            .print()

        stat_replay_error_bag_count_group_by_category = stream.filter(lambda x:x.type == 'replay' and x.error_type!='')\
            .process(CountErrorTimeProcess())\
            .key_by(lambda x:f"{x['group']}$${x['error_stage']}$${x['error_type']}")\
            .flat_map(HandleErrorFlatMap(tag='stat_replay_error_bag_count_group_by_category'))\
            .print()
    except Exception as e:
        with open('err.log','a') as f:
            f.write(e+'\n')

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute("stat_bag")