import time
import json
import os
from datetime import datetime
from kafka import KafkaProducer


def datetime_str_to_int(datetime_str: str) -> int:
    datetime_object = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    date_int = int(time.mktime((datetime_object).timetuple()))
    return date_int


if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers="10.10.2.224:9092,10.10.2.81:9092,10.10.3.141:9092")
    topic = 'test_ars_workflow_statistics'
    messages = []
    print(os.path.abspath(''))
    # data.json path copy from database
    with open('/home/simon.feng/flink_demo/flink_demo/data.json') as f:
        data = dict(json.load(f))
    messages = list(data['workflow'])
    messages1 = messages.copy()
    messages.extend(messages1)
    messages = sorted(
        messages, key=lambda x: datetime_str_to_int(x['create_time']))
    for message in messages:
        print(type(message['workflow_output']), message['workflow_output'])
        producer.send(topic, value=json.dumps(message).encode('utf-8'))
    producer.close()