import json
from pyflink.common import Types
from pyflink.datastream import (StreamExecutionEnvironment, FlatMapFunction,
                                RuntimeContext, MapFunction)
from pyflink.datastream.state import ValueStateDescriptor, ValueState, MapStateDescriptor, MapState
from configs import (
    KAFKA_TOPIC_OF_ARS_WORKFLOW,
    # FLINK_SQL_CONNECTOR_KAFKA_LOC,
    ARS_HOST,
    ARS_API_ROOT_TOKEN)
from datetime import datetime, timedelta
from lib.dates import (
    str_to_datetime,
    datetime_to_str,
    dt_to_dayobj,
)
from lib.schema import TEST_ARS_WORKFLOW_SCHEMA
from lib.kafka import get_flink_kafka_consumer
from lib.utils import add_value_to_dict, merge_dicts, http_request


class Map(MapFunction):
    def map(self, value):
        dt = str_to_datetime(value['update_time'])

        mode = None
        workflow_input = json.loads(value.workflow_input)
        if 'extra_args' in workflow_input and 'mode' in workflow_input[
                'extra_args']:
            mode = workflow_input['extra_args']['mode'].upper()

        bag_replay_list = []
        workflow_output = json.loads(value.workflow_output)
        if 'bag_replayed_list' in workflow_output:
            bag_replay_list = workflow_output['bag_replayed_list']

        bags_profile_summary = None
        metric = json.loads(value.metric)
        if 'bags_profile_summary' in metric:
            bags_profile_summary = metric['bags_profile_summary']

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
        self.today_success_pods = ctx.get_map_state(
            MapStateDescriptor("today_success_pods", Types.STRING(),
                               Types.STRING()))
        self.today_failure_pods = ctx.get_map_state(
            MapStateDescriptor("today_failure_pods", Types.STRING(),
                               Types.STRING()))
        self.yesterday_dt = ctx.get_state(
            ValueStateDescriptor("yesterday_dt", Types.STRING()))
        self.today_dt = ctx.get_state(
            ValueStateDescriptor("today_dt", Types.STRING()))
        self.last_fire = ctx.get_state(
            ValueStateDescriptor("last_fire", Types.STRING()))

    def init(self):
        """
        1. initialize variables
        2. determine date
        """
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
        """
        If it enters a new day, move today's data to yesterday's and clear today's data.
        """
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
        self.today_failure_pods.clear()
        self.today_success_pods.clear()
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
                name='stat_success_pod_group_by_type',
                period='daily',
                stat_date=self.today_dt.value(),
                info=json.loads(
                    self.today_stat_success_pod_group_by_type.value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_success_pod_group_by_type',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=json.loads(
                    self.yesterday_stat_success_pod_group_by_type.value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_failure_pod_group_by_type',
                period='daily',
                stat_date=self.today_dt.value(),
                info=json.loads(
                    self.today_stat_failure_pod_group_by_type.value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_failure_pod_group_by_type',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=json.loads(
                    self.yesterday_stat_failure_pod_group_by_type.value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_success_bag_group_by_category',
                period='daily',
                stat_date=self.today_dt.value(),
                info=json.loads(
                    self.today_stat_replay_success_bag_group_by_category.
                    value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_success_bag_group_by_category',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=json.loads(
                    self.yesterday_stat_replay_success_bag_group_by_category.
                    value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_failure_bag_group_by_category',
                period='daily',
                stat_date=self.today_dt.value(),
                info=json.loads(
                    self.today_stat_replay_failure_bag_group_by_category.
                    value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_failure_bag_group_by_category',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=json.loads(
                    self.yesterday_stat_replay_failure_bag_group_by_category.
                    value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_success_bag_group_by_mode',
                period='daily',
                stat_date=self.today_dt.value(),
                info=json.loads(
                    self.today_stat_replay_success_bag_group_by_mode.value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_success_bag_group_by_mode',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=json.loads(
                    self.yesterday_stat_replay_success_bag_group_by_mode.
                    value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_failure_bag_group_by_mode',
                period='daily',
                stat_date=self.today_dt.value(),
                info=json.loads(
                    self.today_stat_replay_failure_bag_group_by_mode.value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_failure_bag_group_by_mode',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=json.loads(
                    self.yesterday_stat_replay_failure_bag_group_by_mode.
                    value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_success_bag_group_by_type',
                period='daily',
                stat_date=self.today_dt.value(),
                info=json.loads(
                    self.today_stat_success_bag_group_by_type.value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_success_bag_group_by_type',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=json.loads(
                    self.yesterday_stat_success_bag_group_by_type.value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_failure_bag_group_by_type',
                period='daily',
                stat_date=self.today_dt.value(),
                info=json.loads(
                    self.today_stat_failure_bag_group_by_type.value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_failure_bag_group_by_type',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=json.loads(
                    self.yesterday_stat_failure_bag_group_by_type.value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_time_consuming_group_by_category',
                period='daily',
                stat_date=self.today_dt.value(),
                info=json.loads(
                    self.today_stat_replay_time_consuming_group_by_category.
                    value())),
            headers={'Authorization': 'Token ' + ARS_API_ROOT_TOKEN})
        http_request(
            method='POST',
            url=ARS_HOST + '/api/v1/driver/statistics',
            json=dict(
                name='stat_replay_time_consuming_group_by_category',
                period='daily',
                stat_date=self.yesterday_dt.value(),
                info=json.loads(
                    self.yesterday_stat_replay_time_consuming_group_by_category
                    .value())),
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
                state_stat_replay_time_consuming_group_by_category: ValueState,
                is_today=False  # rule out duplicate pods in today's pod err rate only
        ):
            """
            Update all states in this day.
            This is the core algorithm of this job.
            """
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
                if is_today:
                    if self.today_failure_pods.contains(workflow_id):
                        add_value_to_dict(
                            stat_failure_pod_group_by_type, -1,
                            self.today_failure_pods.get(workflow_id))
                        self.today_failure_pods.remove(workflow_id)
                    elif self.today_success_pods.contains(workflow_id):
                        return
                    self.today_success_pods.put(workflow_id, category)
                succ, fail = 0, 0
                for bag in bag_replay_list:
                    if bag:
                        succ += 1
                    else:
                        fail += 1
                if succ:
                    if workflow_type == 'replay':
                        add_value_to_dict(
                            stat_replay_success_bag_group_by_category, succ,
                            category)
                        if mode is not None:
                            add_value_to_dict(
                                stat_replay_success_bag_group_by_mode, succ,
                                mode)
                    if workflow_type != 'probe_detect':
                        add_value_to_dict(stat_success_bag_group_by_type, succ,
                                          workflow_type)
                if fail:
                    if workflow_type == 'replay':
                        add_value_to_dict(
                            stat_replay_failure_bag_group_by_category, fail,
                            category)
                        if mode is not None:
                            add_value_to_dict(
                                stat_replay_failure_bag_group_by_mode, fail,
                                mode)
                    if workflow_type != 'probe_detect':
                        add_value_to_dict(stat_failure_bag_group_by_type, fail,
                                          workflow_type)

            elif workflow_status == 'FAILURE':
                if is_today:
                    if self.today_success_pods.contains(workflow_id):
                        add_value_to_dict(
                            stat_success_pod_group_by_type, -1,
                            self.today_success_pods.get(workflow_id))
                        self.today_success_pods.remove(workflow_id)
                    elif self.today_failure_pods.contains(workflow_id):
                        return
                    self.today_failure_pods.put(workflow_id, category)
                add_value_to_dict(stat_failure_pod_group_by_type, 1,
                                  workflow_type)
                if workflow_type == 'replay':
                    add_value_to_dict(
                        stat_replay_failure_bag_group_by_category, bag_nums,
                        category)
                    if mode is not None:
                        add_value_to_dict(
                            stat_replay_failure_bag_group_by_mode, bag_nums,
                            mode)
                if workflow_type != 'probe_detect':
                    add_value_to_dict(stat_failure_bag_group_by_type, bag_nums,
                                      workflow_type)

            # consuming
            if workflow_type == 'replay' and bags_profile_summary is not None:
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
            update(
                self.today_stat_success_pod_group_by_type,
                self.today_stat_failure_pod_group_by_type,
                self.today_stat_replay_success_bag_group_by_category,
                self.today_stat_replay_failure_bag_group_by_category,
                self.today_stat_replay_success_bag_group_by_mode,
                self.today_stat_replay_failure_bag_group_by_mode,
                self.today_stat_success_bag_group_by_type,
                self.today_stat_failure_bag_group_by_type,
                self.today_stat_replay_time_consuming_group_by_category,
                is_today=True)
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
        # .print()


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    # env.add_jars("file://" + FLINK_SQL_CONNECTOR_KAFKA_LOC)
    analyse(env)
    env.execute("stat_pod")
