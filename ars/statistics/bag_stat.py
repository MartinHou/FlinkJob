import json
from pyflink.common import Types
from pyflink.datastream import (StreamExecutionEnvironment, FlatMapFunction,
                                RuntimeContext, ProcessFunction)
from pyflink.datastream.state import ValueStateDescriptor, ValueState
from configs import (
    KAFKA_TOPIC_OF_ARS_BAG, 
    FLINK_SQL_CONNECTOR_KAFKA_LOC, 
    ARS_HOST,
    ARS_API_ROOT_TOKEN
)
from datetime import datetime, timedelta
from lib.dates import (
    datetime_to_str,
    str_to_datetime,
    dt_to_dayobj,
)
from lib.schema import TEST_ARS_BAG_SCHEMA
from lib.kafka import get_flink_kafka_consumer
from lib.utils import add_value_to_dict, http_request


class Process(ProcessFunction):
    def process_element(self, value, ctx: ProcessFunction.Context):
        ts = int(ctx.timestamp() / 1000)
        dt = datetime.fromtimestamp(ts)

        if value.group == 'cp':
            value.group = 'CP'

        mode = None
        config = json.loads(value.config)
        if 'extra_args' in config and 'mode' in config['extra_args']:
            mode = config['extra_args']['mode']

        duration = None
        metric = json.loads(value.metric)
        if 'bag_duration' in metric:
            duration = metric['bag_duration']

        yield {
            'datetime': dt,
            'daydt': dt_to_dayobj(dt),
            'mode': mode,
            'group': value.group,
            'error_stage': value.error_stage,
            'error_type': value.error_type,
            'output_bag': value.output_bag,
            'duration': duration,
        }


class StatBag(FlatMapFunction):
    def open(self, ctx: RuntimeContext):
        self.today_stat_replay_success_bag_duration_group_by_mode = ctx.get_state(
            ValueStateDescriptor(
                "today_stat_replay_success_bag_duration_group_by_mode",
                Types.STRING()))
        self.today_stat_replay_success_bag_duration_group_by_category = ctx.get_state(
            ValueStateDescriptor(
                "today_stat_replay_success_bag_duration_group_by_category",
                Types.STRING()))
        self.today_stat_replay_error_bag_count_group_by_category = ctx.get_state(
            ValueStateDescriptor(
                "today_stat_replay_error_bag_count_group_by_category",
                Types.STRING()))
        self.yesterday_stat_replay_success_bag_duration_group_by_mode = ctx.get_state(
            ValueStateDescriptor(
                "yesterday_stat_replay_success_bag_duration_group_by_mode",
                Types.STRING()))
        self.yesterday_stat_replay_success_bag_duration_group_by_category = ctx.get_state(
            ValueStateDescriptor(
                "yesterday_stat_replay_success_bag_duration_group_by_category",
                Types.STRING()))
        self.yesterday_stat_replay_error_bag_count_group_by_category = ctx.get_state(
            ValueStateDescriptor(
                "yesterday_stat_replay_error_bag_count_group_by_category",
                Types.STRING()))
        self.yesterday_dt = ctx.get_state(
            ValueStateDescriptor("yesterday_dt", Types.STRING()))
        self.today_dt = ctx.get_state(
            ValueStateDescriptor("today_dt", Types.STRING()))
        self.last_fire = ctx.get_state(
            ValueStateDescriptor("last_fire", Types.STRING()))

    def init(self):
        now_dt = datetime.now()
        self.last_fire.update(
            datetime_to_str(now_dt)
        )  # prevent writing sql before current dt (disabled in test)
        # self.last_fire.update(datetime_to_str(datetime.now().replace(hour=0, minute=0, second=0) -
        #     timedelta(days=1))) # only for test
        today_daydt = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_daydt = today_daydt - timedelta(days=1)
        self.yesterday_dt.update(datetime_to_str(yesterday_daydt))
        self.today_dt.update(datetime_to_str(today_daydt))
        self.today_stat_replay_success_bag_duration_group_by_mode.update(
            json.dumps({}))
        self.today_stat_replay_success_bag_duration_group_by_category.update(
            json.dumps({}))
        self.today_stat_replay_error_bag_count_group_by_category.update(
            json.dumps({}))
        self.yesterday_stat_replay_success_bag_duration_group_by_mode.update(
            json.dumps({}))
        self.yesterday_stat_replay_success_bag_duration_group_by_category.update(
            json.dumps({}))
        self.yesterday_stat_replay_error_bag_count_group_by_category.update(
            json.dumps({}))

    def check_expire(self, daydt: datetime):
        """
        If it enters a new day, move today's data to yesterday's and clear today's data.
        """
        if daydt <= str_to_datetime(self.today_dt.value()):
            return
        # assume that it is impossible that daydt is the day after today_dt
        self.yesterday_stat_replay_success_bag_duration_group_by_mode.update(
            self.today_stat_replay_success_bag_duration_group_by_mode.value())
        self.today_stat_replay_success_bag_duration_group_by_mode.update(
            json.dumps({}))
        self.yesterday_stat_replay_success_bag_duration_group_by_category.update(
            self.today_stat_replay_success_bag_duration_group_by_category.
            value())
        self.today_stat_replay_success_bag_duration_group_by_category.update(
            json.dumps({}))
        self.yesterday_stat_replay_error_bag_count_group_by_category.update(
            self.today_stat_replay_error_bag_count_group_by_category.value())
        self.today_stat_replay_error_bag_count_group_by_category.update(
            json.dumps({}))
        self.yesterday_dt.update(self.today_dt.value())
        self.today_dt.update(datetime_to_str(daydt))

    def write_sql(self):
        """
        Write data to MySQL statistics table.
        """
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_success_bag_duration_group_by_mode',
                period='daily',
                stat_date=self.today_dt.value(),
                info=json.loads(self.today_stat_replay_success_bag_duration_group_by_mode.
                value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_success_bag_duration_group_by_mode',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=json.loads(self.
                yesterday_stat_replay_success_bag_duration_group_by_mode.
                value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_success_bag_duration_group_by_category',
                period='daily',
                stat_date=self.today_dt.value(),
                info=json.loads(self.
                today_stat_replay_success_bag_duration_group_by_category.
                value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_success_bag_duration_group_by_category',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=json.loads(self.
                yesterday_stat_replay_success_bag_duration_group_by_category.
                value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_error_bag_count_group_by_category',
                period='daily',
                stat_date=self.today_dt.value(),
                info=json.loads(self.today_stat_replay_error_bag_count_group_by_category.
                value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_error_bag_count_group_by_category',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=json.loads(self.
                yesterday_stat_replay_error_bag_count_group_by_category.
                value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})

    def flat_map(self, value):
        def update(
                state_stat_replay_success_bag_duration_group_by_mode:
                ValueState,
                state_stat_replay_success_bag_duration_group_by_category:
                ValueState,
                state_stat_replay_error_bag_count_group_by_category: ValueState,
        ):
            """
            Update all states in this day.
            This is the core algorithm of this job.
            """
            stat_replay_success_bag_duration_group_by_mode = json.loads(
                state_stat_replay_success_bag_duration_group_by_mode.value())
            stat_replay_success_bag_duration_group_by_category = json.loads(
                state_stat_replay_success_bag_duration_group_by_category.
                value())
            stat_replay_error_bag_count_group_by_category = json.loads(
                state_stat_replay_error_bag_count_group_by_category.value())

            if output_bag != '' and duration is not None:  # workflow_type=='replay' and is removed for it is for sure
                if mode:
                    add_value_to_dict(
                        stat_replay_success_bag_duration_group_by_mode,
                        duration, mode)
                add_value_to_dict(
                    stat_replay_success_bag_duration_group_by_category,
                    duration, group)
            if error_type != '':  # workflow_type == 'replay' and is removed for it is for sure
                add_value_to_dict(
                    stat_replay_error_bag_count_group_by_category, 1, group,
                    error_stage, error_type)

            state_stat_replay_success_bag_duration_group_by_mode.update(
                json.dumps(stat_replay_success_bag_duration_group_by_mode))
            state_stat_replay_success_bag_duration_group_by_category.update(
                json.dumps(stat_replay_success_bag_duration_group_by_category))
            state_stat_replay_error_bag_count_group_by_category.update(
                json.dumps(stat_replay_error_bag_count_group_by_category))

        if not self.today_dt.value():  # init only when restarting server
            self.init()

        dt: datetime = value['datetime']
        daydt: datetime = value['daydt']
        daydt_str = datetime_to_str(daydt)
        self.check_expire(daydt)

        mode = value['mode']
        group = value['group']
        error_stage = value['error_stage']
        error_type = value['error_type']
        output_bag = value['output_bag']
        duration = value['duration']

        if daydt_str == self.yesterday_dt.value():
            update(
                self.yesterday_stat_replay_success_bag_duration_group_by_mode,
                self.
                yesterday_stat_replay_success_bag_duration_group_by_category,
                self.yesterday_stat_replay_error_bag_count_group_by_category)
        elif daydt_str == self.today_dt.value():
            update(
                self.today_stat_replay_success_bag_duration_group_by_mode,
                self.today_stat_replay_success_bag_duration_group_by_category,
                self.today_stat_replay_error_bag_count_group_by_category)
        else:  # expired data
            return iter([])

        last_fire = str_to_datetime(self.last_fire.value())
        if dt - last_fire > timedelta(seconds=20):
            self.last_fire.update(datetime_to_str(dt))
            self.write_sql()

        yield dt


def analyse(env: StreamExecutionEnvironment):

    stream = env.add_source(
        get_flink_kafka_consumer(
            schema=TEST_ARS_BAG_SCHEMA,
            topic=KAFKA_TOPIC_OF_ARS_BAG,
            group_id='stat_bag',
            start_date=datetime.now().replace(hour=0, minute=0, second=0) -
            timedelta(days=1)))

    stream.filter(lambda x: x.type == 'replay')\
        .process(Process())\
        .key_by(lambda x: 'dont_care')\
        .flat_map(StatBag())\
        # .print()


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(8)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute("stat_bag")
