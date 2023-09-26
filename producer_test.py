from kafka import KafkaProducer
import json
from datetime import datetime
from lib.common.schema import POD_ERR_SCHEMA

# 初始化Kafka Producer
producer = KafkaProducer(
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            bootstrap_servers=['10.10.2.224:9092', '10.10.2.81:9092', '10.10.3.141:9092'],
            api_version=(0, 10),
            retries=3,
        )

# 发送一些数据
for i in range(5):
    producer.send(topic='martin_test',value={
        "cluster_name": "DDInfra",
        "node_name": "node1",
        "pod_name": f"3090-1-ars-002bacb68af748048e2b001e41836a8a-replay-bag-smgfqa{i}",
        "happened_at": datetime.now().timestamp(),
    })
for i in range(3):
    producer.send(topic='martin_test',value={
        "cluster_name": "DDInfra",
        "node_name": "node2",
        "pod_name": f"2080-1-ars-002bacb68af748048e2b001e41836a8b-replay-bag-smgfqa{i}",
        "happened_at": datetime.now().timestamp(),
    })

# 确保所有的消息都被发送出去
producer.flush()

# 关闭producer以确保所有剩余的消息都被发送。
producer.close()
