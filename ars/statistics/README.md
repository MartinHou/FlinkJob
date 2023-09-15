所有代码配置好与flink相关的jar文件路径直接运行即可；

test_ars_workflow_statistics.py为dashboard界面数据库生成流处理；
test_ars_workflow_statistics_consuming.py为statistic界面数据库生成流处理；
bag_level_crash.py 为bag_level_crash的数据生成；
count_sql_workflow.py为每日sql workflow数据统计；
count_kafka_workflow.py为每日kafka workflow数据统计；
workflow_compare.py为两种数据来源差异化对比；

wf_sql_date.json为当日sql计数的workflow_id结果；
wf_kafka_date.json为当日kafka计数的workflow_id结果；
wf_kafka-sql_date.json为kafka有，sql无的计数结果；
wf_sql-kafka_date.json为sql有，kafka无的计数结果；
wf_kafka_date_retry.json为当日重试次数统计；