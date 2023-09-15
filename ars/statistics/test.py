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
        'ars_prod_bag_result',
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

    d = dict()
    try:
        # 消费者会一直运行，除非按Ctrl+C
        for message in consumer:
            print(datetime.fromtimestamp(message.timestamp / 1000))
            msg = json.loads(message.value)
            category = msg['group']
            created_at = msg['pod_start_timestamp']
            if not category in d:
                d[category] = 1
            else:
                d[category] += 1

            # if workflow_one['pod_id'] == '32e8e5d8e8cc47f89c9c62de073c0929':
            # # if workflow_one['workflow_id']=='32e8e5d8e8cc47f89c9c62de073c0929':
            #     print(message)
            #     break
            # print(workflow_one)
        print(d)
    except KeyboardInterrupt:
        # 用户按了Ctrl+C，就退出程序
        print(d)
        print("Stopping consumer")
        consumer.close()
