from common.settings import *
import os


def get_kafka_workflow_loc():
    path = os.path.join(LOG_URL, START_MON + START_DAY)
    if not os.path.exists(path):
        os.makedirs(path)
    return os.path.join(path, f'wf_kfk_{START_MON}{START_DAY}.json')


def get_sql_workflow_loc():
    path = os.path.join(LOG_URL, START_MON + START_DAY)
    if not os.path.exists(path):
        os.makedirs(path)
    return os.path.join(path, f'wf_sql_{START_MON}{START_DAY}.json')


def get_kafka_workflow_retry_loc():
    path = os.path.join(LOG_URL, START_MON + START_DAY)
    if not os.path.exists(path):
        os.makedirs(path)
    return os.path.join(path, f'wf_kfk_{START_MON}{START_DAY}_retry.json')


def get_kafka_diff_sql_workflow_loc():
    path = os.path.join(LOG_URL, START_MON + START_DAY)
    if not os.path.exists(path):
        os.makedirs(path)
    return os.path.join(path, f'wf_sql-kfk_{START_MON}{START_DAY}.json')


def get_sql_diff_kafka_workflow_loc():
    path = os.path.join(LOG_URL, START_MON + START_DAY)
    if not os.path.exists(path):
        os.makedirs(path)
    return os.path.join(path, f'wf_kfk-sql_{START_MON}{START_DAY}.json')


def get_might_be_in_sql_workflow_loc():
    path = os.path.join(LOG_URL, START_MON + START_DAY)
    if not os.path.exists(path):
        os.makedirs(path)
    return os.path.join(path,
                        f'wf_might_be_in_sql_{START_MON}{START_DAY}.json')


def get_might_be_in_kafka_workflow_loc():
    path = os.path.join(LOG_URL, START_MON + START_DAY)
    if not os.path.exists(path):
        os.makedirs(path)
    return os.path.join(path,
                        f'wf_might_be_in_kafka_{START_MON}{START_DAY}.json')
