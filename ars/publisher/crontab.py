import paramiko
from kafka import KafkaProducer
import schedule
import time
import json 

# Kafka producer 配置
producer = KafkaProducer(
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=str.encode,
    bootstrap_servers=['10.10.2.224:9092', '10.10.2.81:9092', '10.10.3.141:9092'],
    api_version=(0, 10),
    retries=3,
)
topic_name = 'ars_prod_node_monitor'
# 机器IP列表
machines_ip_list =  ['10.9.1.26', '10.9.2.29', '10.9.2.32', '10.9.2.35', '10.9.2.36', '10.9.2.37', '10.9.2.39', '10.9.2.42', '10.9.2.46', '10.9.24.15', '10.9.25.38', '10.9.25.41', '10.9.25.42', '10.9.25.43', '10.9.25.44', '10.9.25.45', '10.9.25.47', '10.9.25.48', '10.9.25.49', '10.9.25.50', '10.9.26.38', '10.9.3.55', '10.9.3.56', '10.9.3.57', '10.9.3.64', '10.9.3.67', '10.9.3.72', '10.9.3.73', '10.9.3.77', '10.9.33.4', '10.9.33.5', '10.9.33.6', '10.9.33.7', '10.9.33.8', '10.9.33.9', '10.9.35.14', '10.9.35.25', '10.9.35.31', '10.9.35.32', '10.9.35.36', '10.9.35.37', '10.9.35.38', '10.9.35.50', '10.9.35.54', '10.9.35.59', '10.9.5.63', '10.9.5.66', '10.9.8.61', '10.9.8.65', '10.9.8.66', '10.9.8.68', '10.9.8.69', '10.9.8.71', '10.9.8.72', '10.9.8.73', '10.9.24.28', '10.9.25.13', '10.9.25.23', '10.9.26.23', '10.9.28.18', '10.9.28.31', '10.9.28.36', '10.9.28.38', '10.9.28.4', '10.9.28.43', '10.9.28.7', '10.9.29.17', '10.9.29.24', '10.9.29.35', '10.9.29.4', '10.9.29.56', '10.9.30.25', '10.9.30.39', '10.9.30.7', '10.9.31.25', '10.9.31.32', '10.9.31.45', '10.9.31.47', '10.9.32.25', '10.9.32.27', '10.9.32.4', '10.9.32.46', '10.9.33.24', '10.9.33.25', '10.9.35.51', '10.9.31.61', '10.9.32.61', '10.9.32.62', '10.9.32.63', '10.9.29.12', '10.9.29.44', '10.9.29.51', '10.9.29.57', '10.9.29.6',

"10.9.26.49",  
"10.9.5.15",   
"10.9.5.16",   
"10.9.5.18",   
"10.9.5.21",   
"10.9.5.24",   
"10.9.5.28",   
"10.9.5.32",   
"10.9.5.37",   
"10.9.5.40",   
"10.9.5.56",   
"10.9.5.60",   
"10.9.5.72",   
"10.9.5.73",   
"10.9.5.76",   
"10.9.5.77",   
"10.9.5.78",   
"10.9.5.80",   
"10.9.5.83",   
"10.9.5.84",
 
]
def task():
    for machine_ip in machines_ip_list:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(machine_ip, username='ddinfra', password='ddinfra')  # replace with your SSH credentials

            stdin, stdout, stderr = ssh.exec_command('nvidia-smi')
            output = stdout.read().decode('utf-8')
            msg = dict(
                cluster_name='ddinfra-prod',
                node_name=machine_ip,
                msg='',
                msg_level='health',
                timestamp=time.time(),
                fault_type='GPUFanError',
            )
            if 'err!' in output.lower():
                msg = dict(
                    cluster_name='ddinfra-prod',
                    node_name=machine_ip,
                    msg='`ERR!` in nvidia-smi output',
                    msg_level='fault',
                    timestamp=time.time(),
                    fault_type='GPUFanError',
                )
            print(msg)
            producer.send(
                topic_name,
                key=machine_ip,
                value=msg,
            )
            producer.flush()
        except Exception as e:
            print(f"Error occurred on {machine_ip}: {str(e)}")

        finally:
            ssh.close()



while True:
    task()
    time.sleep(60)
    