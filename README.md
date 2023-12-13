
# Introduction

The Flink job contains 5 jobs:

- Filter bag level crash (ars/mxf/filter_bag_level_crash.py)
- Dashboard statistic (ars/statistics)
- Node monitor (ars/warning/node_monitor.py)
- GPU monitor (ars/warning/pod_err_monitor.py)
- Flink CDC (ars/cdc)

More info can be found:
https://momenta.feishu.cn/wiki/Xba1w1hVXiNVGykkUxucWEGDnfh

# Getting Started

Check setup/setup.sh and put jars under flink-1.x.x/lib.

```pip install -r requirements.txt```

# Run

```sh submit_jobs.sh```

or

```flink run -py xx.py [-pyfs abs_path_to_proj]```
