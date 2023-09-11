from datetime import datetime, timedelta

LOG_URL = '/mnt/data/martin.hou/flink-jobs/ars/workflow/log'
FLINK_SQL_CONNECTOR_KAFKA_LOC = '/mnt/data/martin.hou/flink-sql-connector-kafka-1.15.4.jar'

START_TIME = datetime(2023, 9, 8, 0, 0, 0)
END_TIME = START_TIME + timedelta(days=1)
KILL_TIME = END_TIME + timedelta(hours=13)
START_MON, START_DAY = START_TIME.strftime("%m-%d").split('-')
START_YMD = START_TIME.strftime("%Y-%m-%d")

MYSQL_USER = 'ars_prod'
MYSQL_PASSWORD = '01234567'
MYSQL_HOST = '10.10.2.244'
MYSQL_DATABASE = 'ars_prod'

KAFKA_SERVERS = '10.10.2.224:9092,10.10.2.81:9092,10.10.3.141:9092'
KAFKA_TOPIC_OF_ARS_BAG= 'ars_prod_bag_result'
KAFKA_TOPIC_OF_ARS_BAG_CRASH='ars_prod_bag_crash_result'
KAFKA_CONSUMUER_GOURP_ID = "flink_get_crash"