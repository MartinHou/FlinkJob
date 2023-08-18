import os
import time
import json
from datetime import datetime,timedelta

if __name__ == "__main__":
    date='_8_16'
    with open('/home/simon.feng/flink_demo/flink_demo/workflow_kafka'+date+'.json','r') as f:
        workflow_kafka=json.load(f)
    with open('/home/simon.feng/flink_demo/flink_demo/workflow'+date+'.json','r') as f:
        workflow=json.load(f)
    workflow_set=set(workflow)
    workflow_kafka_set=set(workflow_kafka)
    print(len(workflow_set),len(workflow_kafka_set),len(workflow_set-workflow_kafka_set),len(workflow_kafka_set-workflow_set))
    with open('/home/simon.feng/flink_demo/flink_demo/workflow_sql_diff_kafka'+date+'.json',
            'w') as f:
        json.dump(list(workflow_set-workflow_kafka_set), f, indent=4)
    with open('/home/simon.feng/flink_demo/flink_demo/workflow_kafka_diff_sql'+date+'.json',
            'w') as f:
        json.dump(list(workflow_kafka_set-workflow_set), f, indent=4)





