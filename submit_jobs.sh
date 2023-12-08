flink run -d -py ars/warning/pod_err_monitor.py
flink run -d -py ars/warning/node_monitor.py
flink run -d -py ars/mxf/filter_bag_level_crash.py

flink run -d -py ars/statistics/pod_stat.py -pyfs /mnt/data/martin.hou/flink-jobs
flink run -d -py ars/statistics/bag_stat.py -pyfs /mnt/data/martin.hou/flink-jobs

flink run -d -py ars/data_migration/migrate_workflow.py
flink run -d -py ars/data_migration/migrate_result.py