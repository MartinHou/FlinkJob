from kafka import KafkaProducer
import json
from datetime import datetime
from lib.common.settings import KAFKA_TOPIC_OF_JOB_MONITOR

# 初始化Kafka Producer
producer = KafkaProducer(
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            bootstrap_servers=['10.10.2.224:9092', '10.10.2.81:9092', '10.10.3.141:9092'],
            api_version=(0, 10),
            retries=3,
        )

# 发送一些数据
producer.send(topic=KAFKA_TOPIC_OF_JOB_MONITOR,value={
    "node_name": '10.9.1.26',
    "job_status": 'failed',
    "job_name": '3090-1-ars-ef199570b574453b82558f452b81136f-replay-bag-test',
    "timestamp": 1000*datetime.now().timestamp()
})

# 确保所有的消息都被发送出去
producer.flush()

# 关闭producer以确保所有剩余的消息都被发送。
producer.close()
