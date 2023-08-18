from kafka import KafkaConsumer, TopicPartition
import datetime
import time
import json


def time_str_to_int(time_str: str) -> int:
    time_object = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    time_int = int(time.mktime((time_object).timetuple()))
    return time_int


if __name__ == "__main__":
    # 创建一个Kafka消费者
    consumer = KafkaConsumer(
        # 订阅的主题名
        'ars_prod_pod_result',
        bootstrap_servers=
        '10.10.2.224:9092,10.10.2.81:9092,10.10.3.141:9092',  # Kafka集群的地址
        group_id='test-10a',  # 这个消费者所在的组
        value_deserializer=lambda x: x.decode('utf-8'), 
        auto_offset_reset='earliest'
    )

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
        workflow_list = []
        workflow_dict={}
        for message in consumer:
            workflow_one = json.loads(message.value)
            if time_str_to_int(workflow_one['update_time']) >= time_str_to_int(
                    '2023-08-14 00:00:00') and time_str_to_int(
                        workflow_one['update_time']) < time_str_to_int(
                            '2023-08-15 00:00:00'):
                print(workflow_one['update_time'])
                if workflow_one['workflow_id'] not in workflow_list:
                    workflow_list.append(workflow_one['workflow_id'])
                else:
                    if workflow_one['workflow_id'] in workflow_dict.keys():
                        workflow_dict[workflow_one['workflow_id']]+=1
                    else:
                        workflow_dict[workflow_one['workflow_id']]=2
            elif time_str_to_int(workflow_one['update_time'])>time_str_to_int(
                            '2023-08-16 13:00:00'):
                break
        print(len(workflow_list),len(workflow_dict.keys()))
        with open('/home/simon.feng/flink_demo/flink_demo/workflow_kafka_8_16.json',
                  'w') as f:
            json.dump(workflow_list, f, indent=4)
        with open('/home/simon.feng/flink_demo/flink_demo/workflow_kafka_8_16_retry_dict.json',
                  'w') as f:
            json.dump(workflow_dict, f, indent=4)

    except KeyboardInterrupt:
        # 用户按了Ctrl+C，就退出程序
        print("Stopping consumer")
        consumer.close()
