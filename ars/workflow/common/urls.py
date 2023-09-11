from common.settings import *
import os

def get_kafka_workflow_loc():
    return os.path.join(LOG_URL,f'wf_kafka_{START_MON}{START_DAY}.json')

def get_sql_workflow_loc():
    return os.path.join(LOG_URL,f'wf_sql_{START_MON}{START_DAY}.json')

def get_kafka_workflow_retry_loc():
    return os.path.join(LOG_URL,f'wf_kafka_{START_MON}{START_DAY}_retry.json')

def get_kafka_diff_sql_workflow_loc():
    return os.path.join(LOG_URL,f'wf_sql-kafka_{START_MON}{START_DAY}.json')

def get_sql_diff_kafka_workflow_loc():
    return os.path.join(LOG_URL,f'wf_kafka-sql_{START_MON}{START_DAY}.json')