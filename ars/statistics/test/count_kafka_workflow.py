from kafka import KafkaConsumer, TopicPartition
from datetime import datetime, timedelta
import time
import json
from lib.common.urls import *
from collections import defaultdict


def time_str_to_int(time_str: str) -> int:
    time_object = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    time_int = int(time_object.timestamp())
    return time_int


if __name__ == "__main__":
    # 创建一个Kafka消费者
    topic = 'ars_prod_bag_result'
    dt = datetime(2023, 10, 25)
    timestamp = int(dt.timestamp() * 1000)
    consumer = KafkaConsumer(
        # 订阅的主题名
        # 'ars_prod_bag_result',
        bootstrap_servers=['10.10.2.224:9092','10.10.2.81:9092','10.10.3.141:9092'],  # Kafka集群的地址
        group_id=f'test-martin',  # 这个消费者所在的组
        value_deserializer=lambda x: x.decode('utf-8'),
        auto_offset_reset='earliest')

    # topic_partition = TopicPartition('ars_prod_bag_result', 0)
    partitions = consumer.partitions_for_topic(topic)   # 改topic 1/2
    if partitions is None:
        print(f"No partitions for topic {topic}")
        exit(1)

    tp_list = [TopicPartition(topic, p) for p in partitions]    # 改topic 2/2
    offsets = consumer.offsets_for_times({tp: timestamp for tp in tp_list})

    consumer.assign(tp_list)
    # 为每个分区设置起始 offset
    for tp in tp_list:
        offset_and_timestamp = offsets[tp]
        if offset_and_timestamp is not None:
            consumer.seek(tp, offset_and_timestamp.offset)
        else:
            print(f"No messages after {dt} for partition {tp.partition}")

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
        amnt = 0
        for message in consumer:
            if message.timestamp >= 1000*time_str_to_int("2023-10-28 0:0:0"):
                continue
            print(datetime.fromtimestamp(message.timestamp/1000))
            
            one = json.loads(message.value)
            
            # if one['pod_id']=='7d587ae5e1a34a86bfaf788edd46cfd4':
            #     print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11")
            
            # if one['workflow_id']=='2176b0eb1b3e4108a5a72a4df60f6f74':
            #     print(one['update_time'])
            #     break
                
            # if one['update_time']>='2023-10-25':
            #     continue
            
            # if one['type']=='replay' and one['output_bag']!='' and one['group']=='LidarRB':  # and one['category']=='Evaluation System'
            #     metirc = one['metric']
            #     if 'bag_duration' in metirc:
            #         amnt += metirc['bag_duration']
            #     # data.append(one['result_id'])
            #     length += 1
            
            if one['metric']!='{}' and one['device']=='x86-2080' and one['category']=='APA':
                amnt+=one['metric']['bags_profile_summary']['total']['total']

    except KeyboardInterrupt:
        # 用户按了Ctrl+C，就退出程序
        # with open('LidarRB_1024_bag.txt', 'w') as f:
        #     for result_id in data:
        #         f.write(result_id+'\n')
        print(f'length of result_ids: {length}')
        print(f'amount:{amnt}')
        print("Stopping consumer")
        consumer.close()
