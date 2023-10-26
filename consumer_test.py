from kafka import KafkaConsumer, TopicPartition
from datetime import datetime, timedelta
import time
import json
from lib.common.urls import *


def time_str_to_int(time_str: str) -> int:
    time_object = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    time_int = int(time_object.timestamp())
    return time_int


if __name__ == "__main__":
    # 创建一个Kafka消费者
    # dt = datetime(2023, 10, 21)
    # timestamp = int(dt.timestamp() * 1000)
    consumer = KafkaConsumer(
        # 订阅的主题名
        'test',
        bootstrap_servers=KAFKA_SERVERS,  # Kafka集群的地址
        group_id=f'yestest',  # 这个消费者所在的组
        value_deserializer=lambda x: x.decode('utf-8'),
        auto_offset_reset='earliest')

    # topic_partition = TopicPartition('ars_prod_bag_result', 0)
    # topic_partition = TopicPartition('test', 0)
    # offsets = consumer.offsets_for_times({topic_partition: timestamp})

    # offset_and_timestamp = offsets[topic_partition]
    # if offset_and_timestamp is not None:
    #     consumer.assign([topic_partition])
    #     consumer.seek(topic_partition, offset_and_timestamp.offset)
    # else:
    #     # 如果没有找到合适的offset，那么可能需要处理这种情况
    #     print(f"No messages after {dt}")

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
        length = 0
        data = []
        for message in consumer:
            # if message.timestamp < time_str_to_int("2023-10-21 0:0:0")*1000 or message.timestamp >= 1000*time_str_to_int("2023-10-22 0:0:0"):
            #     continue
            # print(datetime.fromtimestamp(message.timestamp/1000))
            one = json.loads(message.value)
            # if one['pod_id']=='7d587ae5e1a34a86bfaf788edd46cfd4':
            #     print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11")
            print(one)
            # if one['workflow_id']=='2176b0eb1b3e4108a5a72a4df60f6f74':
            #     print(one['update_time'])
            #     break
                
            # # if one['update_time']>='2023-10-24':
            # #     continue
            
            # if one['workflow_type']=='replay':  # and one['category']=='Evaluation System'
            #     print(one['update_time'])
            #     data.append(one['workflow_id'])
                # length += 1

    except KeyboardInterrupt:
        # 用户按了Ctrl+C，就退出程序
        # with open('mq_1023.txt', 'w') as f:
        #     for result_id in data:
        #         f.write(result_id+'\n')
        # print(f'length of result_ids: {length}')
        print("Stopping consumer")
        consumer.close()
