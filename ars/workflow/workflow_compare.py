import os
import time
import json
from datetime import datetime,timedelta
from common.urls import *

if __name__ == "__main__":
    with open(get_kafka_workflow_loc(),'r') as f:
        workflow_kafka=json.load(f)
    with open(get_sql_workflow_loc(),'r') as f:
        workflow_sql=json.load(f)
    with open(get_might_be_in_sql_workflow_loc(),'r') as f:
        workflow_might_be_in_sql=json.load(f)
    with open(get_might_be_in_kafka_workflow_loc(),'r') as f:
        workflow_might_be_in_kafka=json.load(f)
    workflow_sql_set=set(workflow_sql) - set(workflow_might_be_in_sql)
    workflow_kafka_set=set(workflow_kafka) - set(workflow_might_be_in_kafka)
    
    print(
        f'SQL len = {len(workflow_sql_set)}',
        f'Kafka len = {len(workflow_kafka_set)}',
        f'SQL-Kafka len = {len(workflow_sql_set-workflow_kafka_set)}',
        f'Kafka-SQL len = {len(workflow_kafka_set-workflow_sql_set)}', 
        f'Ruled out workflow in sql: {workflow_might_be_in_sql}',
        f'Ruled out workflow in kafka: {workflow_might_be_in_kafka}',
        sep='\n'
    )
    with open(get_kafka_diff_sql_workflow_loc(),'w') as f:
        json.dump(list(workflow_sql_set-workflow_kafka_set), f, indent=4)
    with open(get_sql_diff_kafka_workflow_loc(),'w') as f:
        json.dump(list(workflow_kafka_set-workflow_sql_set), f, indent=4)





