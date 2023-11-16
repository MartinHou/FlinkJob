# flink run -py ars/statistics/gen_bag_statistics.py -pyfs /home/martin.hou/flink-jobs &
# flink run -py ars/statistics/gen_consuming_statistics.py -pyfs /home/martin.hou/flink-jobs &
# flink run -py ars/statistics/gen_pod_statistics.py -pyfs /home/martin.hou/flink-jobs &

flink run -py ars/warning/pod_err_monitor.py &
flink run -py ars/warning/node_monitor.py -pyfs . &
flink run -py ars/mxf/filter_bag_level_crash.py &
