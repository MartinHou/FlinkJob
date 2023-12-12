flink run -d -py ars/warning/pod_err_monitor.py
flink run -d -py ars/warning/node_monitor.py
flink run -d -py ars/mxf/filter_bag_level_crash.py

flink run -d -py ars/statistics/pod_stat.py -pyfs /mnt/data/martin.hou/flink-jobs
flink run -d -py ars/statistics/bag_stat.py -pyfs /mnt/data/martin.hou/flink-jobs

flink run -d -py ars/cdc/migrate_workflow.py
flink run -d -py ars/cdc/migrate_result.py