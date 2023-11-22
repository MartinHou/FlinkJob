import json
from pyflink.common import (
    Types, )
from pyflink.datastream import (StreamExecutionEnvironment, FlatMapFunction,
                                RuntimeContext, MapFunction)
from pyflink.datastream.state import ValueStateDescriptor, ValueState
from lib.common.settings import *
from datetime import datetime, timedelta
from lib.utils.dates import *
from lib.common.schema import TEST_ARS_WORKFLOW_SCHEMA
from lib.utils.kafka import get_flink_kafka_consumer
from lib.utils.utils import add_value_to_dict, merge_dicts, http_request


class Map(MapFunction):
    def map(self, value):
        dt = str_to_datetime(value['update_time'])

        mode = None
        workflow_input = json.loads(value.workflow_input)
        if 'extra_args' in workflow_input and 'mode' in workflow_input[
                'extra_args']:
            mode = workflow_input['extra_args']['mode']

        bag_replay_list = []
        workflow_output = json.loads(value.workflow_output)
        if 'bag_replayed_list' in workflow_output:
            bag_replay_list = workflow_output['bag_replayed_list']

        bags_profile_summary = None
        metric = json.loads(value.metric)
        if 'bags_profile_summary' in metric:
            bags_profile_summary = metric['bags_profile_summary']

        if value.category == 'cp':
            value.category = 'CP'

        return {
            'datetime': dt,
            'daydt': dt_to_dayobj(dt),
            'workflow_id': value.workflow_id,
            'workflow_type': value.workflow_type,
            'category': value.category,
            'bag_nums': value.bag_nums,
            'workflow_status': value.workflow_status,
            'device': value.device,
            'mode': mode,
            'category': value.category,
            'bag_replay_list': bag_replay_list,
            'bags_profile_summary': bags_profile_summary,
        }


class StatPod(FlatMapFunction):
    def open(self, ctx: RuntimeContext):
        self.today_stat_success_pod_group_by_type = ctx.get_state(
            ValueStateDescriptor("today_stat_success_pod_group_by_type",
                                 Types.STRING()))
        self.today_stat_failure_pod_group_by_type = ctx.get_state(
            ValueStateDescriptor("today_stat_failure_pod_group_by_type",
                                 Types.STRING()))
        self.today_stat_replay_success_bag_group_by_category = ctx.get_state(
            ValueStateDescriptor(
                "today_stat_replay_success_bag_group_by_category",
                Types.STRING()))
        self.today_stat_replay_failure_bag_group_by_category = ctx.get_state(
            ValueStateDescriptor(
                "today_stat_replay_failure_bag_group_by_category",
                Types.STRING()))
        self.today_stat_replay_success_bag_group_by_mode = ctx.get_state(
            ValueStateDescriptor("today_stat_replay_success_bag_group_by_mode",
                                 Types.STRING()))
        self.today_stat_replay_failure_bag_group_by_mode = ctx.get_state(
            ValueStateDescriptor("today_stat_replay_failure_bag_group_by_mode",
                                 Types.STRING()))
        self.today_stat_success_bag_group_by_type = ctx.get_state(
            ValueStateDescriptor("today_stat_success_bag_group_by_type",
                                 Types.STRING()))
        self.today_stat_failure_bag_group_by_type = ctx.get_state(
            ValueStateDescriptor("today_stat_failure_bag_group_by_type",
                                 Types.STRING()))
        self.today_stat_replay_time_consuming_group_by_category = ctx.get_state(
            ValueStateDescriptor(
                "today_stat_replay_time_consuming_group_by_category",
                Types.STRING()))
        self.yesterday_stat_success_pod_group_by_type = ctx.get_state(
            ValueStateDescriptor("yesterday_stat_success_pod_group_by_type",
                                 Types.STRING()))
        self.yesterday_stat_failure_pod_group_by_type = ctx.get_state(
            ValueStateDescriptor("yesterday_stat_failure_pod_group_by_type",
                                 Types.STRING()))
        self.yesterday_stat_replay_success_bag_group_by_category = ctx.get_state(
            ValueStateDescriptor(
                "yesterday_stat_replay_success_bag_group_by_category",
                Types.STRING()))
        self.yesterday_stat_replay_failure_bag_group_by_category = ctx.get_state(
            ValueStateDescriptor(
                "yesterday_stat_replay_failure_bag_group_by_category",
                Types.STRING()))
        self.yesterday_stat_replay_success_bag_group_by_mode = ctx.get_state(
            ValueStateDescriptor(
                "yesterday_stat_replay_success_bag_group_by_mode",
                Types.STRING()))
        self.yesterday_stat_replay_failure_bag_group_by_mode = ctx.get_state(
            ValueStateDescriptor(
                "yesterday_stat_replay_failure_bag_group_by_mode",
                Types.STRING()))
        self.yesterday_stat_success_bag_group_by_type = ctx.get_state(
            ValueStateDescriptor("yesterday_stat_success_bag_group_by_type",
                                 Types.STRING()))
        self.yesterday_stat_failure_bag_group_by_type = ctx.get_state(
            ValueStateDescriptor("yesterday_stat_failure_bag_group_by_type",
                                 Types.STRING()))
        self.yesterday_stat_replay_time_consuming_group_by_category = ctx.get_state(
            ValueStateDescriptor(
                "yesterday_stat_replay_time_consuming_group_by_category",
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
        #     timedelta(days=1))) # TODO: only for test
        today_daydt = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_daydt = today_daydt - timedelta(days=1)
        self.yesterday_dt.update(datetime_to_str(yesterday_daydt))
        self.today_dt.update(datetime_to_str(today_daydt))
        self.today_stat_success_pod_group_by_type.update(json.dumps({}))
        self.today_stat_failure_pod_group_by_type.update(json.dumps({}))
        self.today_stat_replay_success_bag_group_by_category.update(
            json.dumps({}))
        self.today_stat_replay_failure_bag_group_by_category.update(
            json.dumps({}))
        self.today_stat_replay_success_bag_group_by_mode.update(json.dumps({}))
        self.today_stat_replay_failure_bag_group_by_mode.update(json.dumps({}))
        self.today_stat_success_bag_group_by_type.update(json.dumps({}))
        self.today_stat_failure_bag_group_by_type.update(json.dumps({}))
        self.today_stat_replay_time_consuming_group_by_category.update(
            json.dumps({}))
        self.yesterday_stat_success_pod_group_by_type.update(json.dumps({}))
        self.yesterday_stat_failure_pod_group_by_type.update(json.dumps({}))
        self.yesterday_stat_replay_success_bag_group_by_category.update(
            json.dumps({}))
        self.yesterday_stat_replay_failure_bag_group_by_category.update(
            json.dumps({}))
        self.yesterday_stat_replay_success_bag_group_by_mode.update(
            json.dumps({}))
        self.yesterday_stat_replay_failure_bag_group_by_mode.update(
            json.dumps({}))
        self.yesterday_stat_success_bag_group_by_type.update(json.dumps({}))
        self.yesterday_stat_failure_bag_group_by_type.update(json.dumps({}))
        self.yesterday_stat_replay_time_consuming_group_by_category.update(
            json.dumps({}))

    def check_expire(self, daydt: datetime):
        if daydt <= str_to_datetime(self.today_dt.value()):
            return
        # assume that it is impossible that daydt is the day after today_dt
        self.yesterday_stat_success_pod_group_by_type.update(
            self.today_stat_success_pod_group_by_type.value())
        self.today_stat_success_pod_group_by_type.update(json.dumps({}))
        self.yesterday_stat_failure_pod_group_by_type.update(
            self.today_stat_failure_pod_group_by_type.value())
        self.today_stat_failure_pod_group_by_type.update(json.dumps({}))
        self.yesterday_stat_replay_success_bag_group_by_category.update(
            self.today_stat_replay_success_bag_group_by_category.value())
        self.today_stat_replay_success_bag_group_by_category.update(
            json.dumps({}))
        self.yesterday_stat_replay_failure_bag_group_by_category.update(
            self.today_stat_replay_failure_bag_group_by_category.value())
        self.today_stat_replay_failure_bag_group_by_category.update(
            json.dumps({}))
        self.yesterday_stat_replay_success_bag_group_by_mode.update(
            self.today_stat_replay_success_bag_group_by_mode.value())
        self.today_stat_replay_success_bag_group_by_mode.update(json.dumps({}))
        self.yesterday_stat_replay_failure_bag_group_by_mode.update(
            self.today_stat_replay_failure_bag_group_by_mode.value())
        self.today_stat_replay_failure_bag_group_by_mode.update(json.dumps({}))
        self.yesterday_stat_success_bag_group_by_type.update(
            self.today_stat_success_bag_group_by_type.value())
        self.today_stat_success_bag_group_by_type.update(json.dumps({}))
        self.yesterday_stat_failure_bag_group_by_type.update(
            self.today_stat_failure_bag_group_by_type.value())
        self.today_stat_failure_bag_group_by_type.update(json.dumps({}))
        self.yesterday_stat_replay_time_consuming_group_by_category.update(
            self.today_stat_replay_time_consuming_group_by_category.value())
        self.today_stat_replay_time_consuming_group_by_category.update(
            json.dumps({}))
        self.yesterday_dt.update(self.today_dt.value())
        self.today_dt.update(datetime_to_str(daydt))

    def write_sql(self):
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_success_pod_group_by_type',
                period='daily',
                stat_date=self.today_dt.value(),
                info=self.today_stat_success_pod_group_by_type.value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_success_pod_group_by_type',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=self.yesterday_stat_success_pod_group_by_type.value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_failure_pod_group_by_type',
                period='daily',
                stat_date=self.today_dt.value(),
                info=self.today_stat_failure_pod_group_by_type.value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_failure_pod_group_by_type',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=self.yesterday_stat_failure_pod_group_by_type.value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_replay_success_bag_group_by_category',
                period='daily',
                stat_date=self.today_dt.value(),
                info=self.today_stat_replay_success_bag_group_by_category.
                value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_replay_success_bag_group_by_category',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=self.yesterday_stat_replay_success_bag_group_by_category.
                value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_replay_failure_bag_group_by_category',
                period='daily',
                stat_date=self.today_dt.value(),
                info=self.today_stat_replay_failure_bag_group_by_category.
                value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_replay_failure_bag_group_by_category',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=self.yesterday_stat_replay_failure_bag_group_by_category.
                value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_replay_success_bag_group_by_mode',
                period='daily',
                stat_date=self.today_dt.value(),
                info=self.today_stat_replay_success_bag_group_by_mode.value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_replay_success_bag_group_by_mode',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=self.yesterday_stat_replay_success_bag_group_by_mode.
                value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_replay_failure_bag_group_by_mode',
                period='daily',
                stat_date=self.today_dt.value(),
                info=self.today_stat_replay_failure_bag_group_by_mode.value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_replay_failure_bag_group_by_mode',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=self.yesterday_stat_replay_failure_bag_group_by_mode.
                value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_success_bag_group_by_type',
                period='daily',
                stat_date=self.today_dt.value(),
                info=self.today_stat_success_bag_group_by_type.value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_success_bag_group_by_type',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=self.yesterday_stat_success_bag_group_by_type.value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_failure_bag_group_by_type',
                period='daily',
                stat_date=self.today_dt.value(),
                info=self.today_stat_failure_bag_group_by_type.value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_failure_bag_group_by_type',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=self.yesterday_stat_failure_bag_group_by_type.value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_replay_time_consuming_group_by_category',
                period='daily',
                stat_date=self.today_dt.value(),
                info=self.today_stat_replay_time_consuming_group_by_category.
                value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics/add_or_update',
            json=dict(
                name='stat_replay_time_consuming_group_by_category',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=self.
                yesterday_stat_replay_time_consuming_group_by_category.
                value()),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})

    def flat_map(self, value):
        def update(
                state_stat_success_pod_group_by_type: ValueState,
                state_stat_failure_pod_group_by_type: ValueState,
                state_stat_replay_success_bag_group_by_category: ValueState,
                state_stat_replay_failure_bag_group_by_category: ValueState,
                state_stat_replay_success_bag_group_by_mode: ValueState,
                state_stat_replay_failure_bag_group_by_mode: ValueState,
                state_stat_success_bag_group_by_type: ValueState,
                state_stat_failure_bag_group_by_type: ValueState,
                state_stat_replay_time_consuming_group_by_category: ValueState
        ):
            stat_success_pod_group_by_type = json.loads(
                state_stat_success_pod_group_by_type.value())
            stat_failure_pod_group_by_type = json.loads(
                state_stat_failure_pod_group_by_type.value())
            stat_replay_success_bag_group_by_category = json.loads(
                state_stat_replay_success_bag_group_by_category.value())
            stat_replay_failure_bag_group_by_category = json.loads(
                state_stat_replay_failure_bag_group_by_category.value())
            stat_replay_success_bag_group_by_mode = json.loads(
                state_stat_replay_success_bag_group_by_mode.value())
            stat_replay_failure_bag_group_by_mode = json.loads(
                state_stat_replay_failure_bag_group_by_mode.value())
            stat_success_bag_group_by_type = json.loads(
                state_stat_success_bag_group_by_type.value())
            stat_failure_bag_group_by_type = json.loads(
                state_stat_failure_bag_group_by_type.value())
            stat_replay_time_consuming_group_by_category = json.loads(
                state_stat_replay_time_consuming_group_by_category.value())

            if workflow_status == 'SUCCESS':
                add_value_to_dict(stat_success_pod_group_by_type, 1,
                                  workflow_type)
                succ, fail = 0, 0
                for bag in bag_replay_list:
                    if bag:
                        succ += 1
                    else:
                        fail += 1
                if succ:
                    add_value_to_dict(
                        stat_replay_success_bag_group_by_category, succ,
                        category)
                    if mode is not None:
                        add_value_to_dict(
                            stat_replay_success_bag_group_by_mode, succ, mode)
                    if workflow_type != 'probe_detect':
                        add_value_to_dict(stat_success_bag_group_by_type, succ,
                                          workflow_type)
                if fail:
                    add_value_to_dict(
                        stat_replay_failure_bag_group_by_category, fail,
                        category)
                    if mode is not None:
                        add_value_to_dict(
                            stat_replay_failure_bag_group_by_mode, fail, mode)
                    if workflow_type != 'probe_detect':
                        add_value_to_dict(stat_failure_bag_group_by_type, fail,
                                          workflow_type)

            elif workflow_status == 'FAILURE':
                add_value_to_dict(stat_failure_pod_group_by_type, 1,
                                  workflow_type)
                add_value_to_dict(stat_replay_failure_bag_group_by_category,
                                  bag_nums, category)
                if mode is not None:
                    add_value_to_dict(stat_replay_failure_bag_group_by_mode,
                                      bag_nums, mode)
                if workflow_type != 'probe_detect':
                    add_value_to_dict(stat_failure_bag_group_by_type, bag_nums,
                                      workflow_type)

            # consuming
            if bags_profile_summary is not None:
                stat_replay_time_consuming_group_by_category = merge_dicts({
                    device: {
                        category: bags_profile_summary
                    }
                }, stat_replay_time_consuming_group_by_category)

            state_stat_success_pod_group_by_type.update(
                json.dumps(stat_success_pod_group_by_type))
            state_stat_failure_pod_group_by_type.update(
                json.dumps(stat_failure_pod_group_by_type))
            state_stat_replay_success_bag_group_by_category.update(
                json.dumps(stat_replay_success_bag_group_by_category))
            state_stat_replay_failure_bag_group_by_category.update(
                json.dumps(stat_replay_failure_bag_group_by_category))
            state_stat_replay_success_bag_group_by_mode.update(
                json.dumps(stat_replay_success_bag_group_by_mode))
            state_stat_replay_failure_bag_group_by_mode.update(
                json.dumps(stat_replay_failure_bag_group_by_mode))
            state_stat_success_bag_group_by_type.update(
                json.dumps(stat_success_bag_group_by_type))
            state_stat_failure_bag_group_by_type.update(
                json.dumps(stat_failure_bag_group_by_type))
            state_stat_replay_time_consuming_group_by_category.update(
                json.dumps(stat_replay_time_consuming_group_by_category))

        if not self.today_dt.value():  # init only when restarting server
            self.init()

        dt: datetime = value['datetime']
        daydt: datetime = value['daydt']
        daydt_str = datetime_to_str(daydt)
        self.check_expire(daydt)

        workflow_id = value['workflow_id']
        workflow_type = value['workflow_type']
        category = value['category']
        bag_nums = value['bag_nums']
        workflow_status = value['workflow_status']
        device = value['device']
        mode = value['mode']
        category = value['category']
        bag_replay_list = value['bag_replay_list']
        bags_profile_summary = value['bags_profile_summary']

        if daydt_str == self.yesterday_dt.value():
            update(self.yesterday_stat_success_pod_group_by_type,
                   self.yesterday_stat_failure_pod_group_by_type,
                   self.yesterday_stat_replay_success_bag_group_by_category,
                   self.yesterday_stat_replay_failure_bag_group_by_category,
                   self.yesterday_stat_replay_success_bag_group_by_mode,
                   self.yesterday_stat_replay_failure_bag_group_by_mode,
                   self.yesterday_stat_success_bag_group_by_type,
                   self.yesterday_stat_failure_bag_group_by_type,
                   self.yesterday_stat_replay_time_consuming_group_by_category)
        elif daydt_str == self.today_dt.value():
            update(self.today_stat_success_pod_group_by_type,
                   self.today_stat_failure_pod_group_by_type,
                   self.today_stat_replay_success_bag_group_by_category,
                   self.today_stat_replay_failure_bag_group_by_category,
                   self.today_stat_replay_success_bag_group_by_mode,
                   self.today_stat_replay_failure_bag_group_by_mode,
                   self.today_stat_success_bag_group_by_type,
                   self.today_stat_failure_bag_group_by_type,
                   self.today_stat_replay_time_consuming_group_by_category)
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
            schema=TEST_ARS_WORKFLOW_SCHEMA,
            topic=KAFKA_TOPIC_OF_ARS_WORKFLOW,
            group_id='stat_pod',
            start_date=datetime.now().replace(hour=0, minute=0, second=0) -
            timedelta(days=1)))

    stream.map(Map())\
        .key_by(lambda x: 'dont_care')\
        .flat_map(StatPod())\
        .print()


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(8)
    env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute("stat_pod")
