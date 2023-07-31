set -ex

# Configuration 和 service 的定义
kubectl create -f flink-configuration-configmap.yaml
kubectl create -f jobmanager-service.yaml
# 为集群创建 deployment
kubectl create -f jobmanager-session-deployment.yaml
kubectl create -f taskmanager-session-deployment.yaml

kubectl port-forward flink-jobmanager-697dc9ffc9-ntqq2 8765:8081 --address 0.0.0.0
