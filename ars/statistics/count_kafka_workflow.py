from kafka import KafkaConsumer, TopicPartition
from datetime import datetime, timedelta
import time
import json
from common.urls import *


def time_str_to_int(time_str: str) -> int:
    time_object = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    time_int = int(time.mktime((time_object).timetuple()))
    return time_int


if __name__ == "__main__":
    # 创建一个Kafka消费者
    consumer = KafkaConsumer(
        # 订阅的主题名
        'ars_prod_pod_result',
        bootstrap_servers=KAFKA_SERVERS,  # Kafka集群的地址
        group_id=f'test-{datetime.now()}',  # 这个消费者所在的组
        value_deserializer=lambda x: x.decode('utf-8'),
        auto_offset_reset='earliest')

    # date_string = "2023-08-12 23:56:00"
    # date_object = datetime.datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
    # date_int = int(int(time.mktime((date_object).timetuple())) * 1000)

    # # 创建三个 TopicPartition 实例
    # partitions = [0, 1, 2]
    # tps = [TopicPartition('ars_prod_pod_result', p) for p in partitions]

    # offsets = {tp: date_int for tp in tps}

    # # 获取最早的大于或等于给定时间的消息的偏移量
    # results = consumer.offsets_for_times(offsets)

    # # 为每个 TopicPartition 设置消费的起始偏移量
    # for tp in tps:
    #     consumer.assign([tp])
    #     assert tp in consumer.assignment(), 'Partition not assigned'
    #     if results[tp] is not None:
    #         consumer.seek(tp, results[tp].offset)

    try:
        # 消费者会一直运行，除非按Ctrl+C
        success, failure = 0, 0
        workflow_list = []
        workflow_dict = {}
        workflow_might_be_in_sql = []
        for message in consumer:
            workflow_one = json.loads(message.value)
            if workflow_one['workflow_status'] not in ('SUCCESS', 'FAILURE'):
                continue
            workflow_id = workflow_one['workflow_id']
            update_time = datetime.strptime(workflow_one['update_time'],
                                            "%Y-%m-%d %H:%M:%S")
            if update_time >= START_TIME - timedelta(minutes=2):
                if update_time < START_TIME:  # might be in sql but it shouldnt be
                    # print('might be in sql but it shouldnt be: ',update_time)
                    workflow_might_be_in_sql.append(workflow_id)
                elif update_time < END_TIME:
                    if workflow_one['workflow_status'] == 'SUCCESS':
                        total = len(workflow_one['workflow_input']['bag_list'])
                        succ, fail = 0, 0
                        for bag in workflow_one['workflow_output'][
                                'bag_replayed_list']:
                            if bag:
                                succ += 1
                            else:
                                fail += 1
                        success += succ
                        failure += fail
                        assert succ + fail == total, f'{succ+fail} != {total}'
                    print(update_time)
                    if workflow_id not in workflow_list:
                        workflow_list.append(workflow_id)
                    else:
                        if workflow_id in workflow_dict:
                            workflow_dict[workflow_id] += 1
                        else:
                            workflow_dict[workflow_id] = 2
                elif update_time > KILL_TIME:
                    break
        print(
            len(workflow_list), len(workflow_dict),
            len(workflow_might_be_in_sql))
        print('success:', success, 'failure:', failure, 'total:',
              success + failure)
        with open(get_might_be_in_sql_workflow_loc(), 'w') as f:
            json.dump(workflow_might_be_in_sql, f, indent=4)
        with open(get_kafka_workflow_loc(), 'w') as f:
            json.dump(workflow_list, f, indent=4)
        with open(get_kafka_workflow_retry_loc(), 'w') as f:
            json.dump(workflow_dict, f, indent=4)

    except KeyboardInterrupt:
        print('success:', success, 'failure:', failure, 'total:',
              success + failure)
        # 用户按了Ctrl+C，就退出程序
        print("Stopping consumer")
        consumer.close()
