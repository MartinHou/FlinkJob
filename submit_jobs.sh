flink run -py ars/statistics/gen_bag_statistics.py -pyfs /mnt/data/martin.hou/flink-jobs &
flink run -py ars/statistics/gen_consuming_statistics.py -pyfs /mnt/data/martin.hou/flink-jobs &
flink run -py ars/statistics/gen_pod_statistics.py -pyfs /mnt/data/martin.hou/flink-jobs &

flink run -py ars/warning/pod_err_monitor.py -pyfs /mnt/data/martin.hou/flink-jobs &
flink run -py ars/warning/node_monitor.py -pyfs /mnt/data/martin.hou/flink-jobs &
