import time
import json
import os
import copy
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
from time import sleep



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
    with open('/home/simon.feng/flink_demo/flink_demo/data_new.json') as f:
        data = dict(json.load(f))
    messages_ori = list(data['workflow'])
    messages=[]
    start = '2022-01-01 23:50:00'
    end = '2022-01-02 00:10:00'
    datetime_list = list(pd.date_range(start=start, end=end, freq='15S'))
    for datetime_one in datetime_list:
        for ori_one in messages_ori:
            ori_one_tmp=copy.deepcopy(ori_one)
            ori_one_tmp['create_time']=str(datetime_one)
            messages.append(ori_one_tmp)
    for message in messages:
        print(message['create_time'])
        sleep(0.1)
        producer.send(topic, value=json.dumps(message).encode('utf-8'))
    producer.close()