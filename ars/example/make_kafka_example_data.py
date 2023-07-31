"""用于生成kafka_topic做实验，仅留存，无需实际运行
"""
from kafka import KafkaProducer
import random
import time
import json
from datetime import datetime, timedelta
from pprint import pprint

# 创建KafkaProducer实例
producer = KafkaProducer(
    bootstrap_servers='10.10.2.224:9092,10.10.2.81:9092,10.10.3.141:9092')

# 发送消息到指定主题
topic = 'test_flink_with_kafka'


def generate_random_message():

    pod_id = random.randint(1, 100)
    status = random.choices(["FAILURE", "SUCCESS"], weights=[0.1, 0.9])[0]
    end_timestamp = int(time.time())
    start_timestamp = int(
        time.mktime((datetime.now() - timedelta(days=14)).timetuple()))

    job_finish_timestamp = random.randint(start_timestamp, end_timestamp)
    job_start_timestamp = job_finish_timestamp - random.randint(
        0,
        timedelta(days=1).total_seconds())
    write_timestamp = job_finish_timestamp + random.randint(
        0,
        timedelta(hours=2).total_seconds())
    device = random.choice(["3090", "2080"])
    group = random.choice(["suzhou", "beijing", "shanghai"])
    reason = random.choice(["crash", "unknown", "player"])
    log = ''.join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=10))

    message = {
        "pod_id": pod_id,
        "status": status,
        "job_start_timestamp": job_start_timestamp,
        "job_finish_timestamp": job_finish_timestamp,
        "write_timestamp": write_timestamp,
        "device": device,
        "group": group,
        "reason": reason,
        "log": log
    }

    #return json.dumps(message).encode('utf-8')
    return message


messages = []
for i in range(100):
    messages.append(generate_random_message())

messages = sorted(messages, key=lambda x: x['write_timestamp'])
pprint(messages)

for message in messages:
    # 使用send()方法发送消息
    producer.send(topic, value=json.dumps(message).encode('utf-8'))

# 关闭KafkaProducer实例
producer.close()