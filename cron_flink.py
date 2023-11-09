from kafka import KafkaConsumer, TopicPartition
from datetime import datetime, timedelta
import time
import json
from lib.common.urls import *
from lib.utils.dates import *
from lib.utils.sql import StatisticsActions
from lib.utils.utils import add_value_to_dict, merge_dicts

def stat_pod():
    # stat_dt = datetime(2023,10,27)
    # dt = datetime(2023,10,26)
    stat_dt = datetime.now()
    dt = stat_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    topic = 'ars_prod_pod_result'
    timestamp = int(dt.timestamp() * 1000)
    consumer = KafkaConsumer(
        # topic,
        bootstrap_servers=['10.10.2.224:9092','10.10.2.81:9092','10.10.3.141:9092'],
        group_id='stat_pod',
        value_deserializer=lambda x: x.decode('utf-8'),
        auto_offset_reset='earliest')
    partitions = consumer.partitions_for_topic(topic)
    tp_list = [TopicPartition(topic, p) for p in partitions]
    offsets = consumer.offsets_for_times({tp: timestamp for tp in tp_list})
    consumer.assign(tp_list)
    for tp in tp_list:
        offset_and_timestamp = offsets[tp]
        if offset_and_timestamp is not None:
            consumer.seek(tp, offset_and_timestamp.offset)
    latest_timestamps = {}
    
    res = {
        'stat_success_pod_group_by_type':{},
        'stat_failure_pod_group_by_type':{},
        
        'stat_replay_success_bag_group_by_category':{},
        'stat_replay_failure_bag_group_by_category':{},
        'stat_replay_success_bag_group_by_mode':{},
        'stat_replay_failure_bag_group_by_mode':{},
        
        'stat_success_bag_group_by_type':{},
        'stat_failure_bag_group_by_type':{},
        
        'stat_replay_time_consuming_group_by_category':{}
    }
    leng=0
    
    for message in consumer:
        latest_timestamps[message.partition] = message.timestamp
        if latest_timestamps and all(ts >= stat_dt.timestamp()*1000 for ts in latest_timestamps.values()):
            break
        one = json.loads(message.value)
        # update_time = one['update_time']
        # if update_time > '2023-10-27':
        #     continue
        # print(update_time)
        
        leng+=1
        workflow_type=one['workflow_type']
        category=one['category']
        if category == 'cp':
            category = 'CP'
        bag_nums=one['bag_nums']
        workflow_status=one['workflow_status']
        device = one['device']
        
        mode = None
        if 'extra_args' in one['workflow_input'] and 'mode' in one['workflow_input']['extra_args']:
            mode = one['workflow_input']['extra_args']['mode']
            
        bag_replay_list = []
        if 'bag_replayed_list' in one['workflow_output']:
            bag_replay_list = one['workflow_output']['bag_replayed_list']
        
        bags_profile_summary = None
        if 'bags_profile_summary' in one['metric']:
            bags_profile_summary = one['metric']['bags_profile_summary']
            
        
        # cal
        if workflow_status=='SUCCESS':
            add_value_to_dict(res,1,'stat_success_pod_group_by_type',workflow_type)
            succ, fail = 0,0
            for bag in bag_replay_list:
                if bag:
                    succ += 1
                else:
                    fail += 1
            if succ:
                add_value_to_dict(res,succ, 'stat_replay_success_bag_group_by_category',category)
                if mode is not None:
                    add_value_to_dict(res,succ, 'stat_replay_success_bag_group_by_mode',mode)
                if workflow_type!='probe_detect':
                    add_value_to_dict(res,succ,'stat_success_bag_group_by_type',workflow_type)
            if fail:
                add_value_to_dict(res,fail, 'stat_replay_failure_bag_group_by_category',category)
                if mode is not None:
                    add_value_to_dict(res,fail, 'stat_replay_failure_bag_group_by_mode',mode)
                if workflow_type!='probe_detect':
                    add_value_to_dict(res,fail,'stat_failure_bag_group_by_type',workflow_type)
                
        elif workflow_status=='FAILURE':
            add_value_to_dict(res,1,'stat_failure_pod_group_by_type',workflow_type)
            add_value_to_dict(res,bag_nums,'stat_replay_failure_bag_group_by_category',category)
            if mode is not None:
                add_value_to_dict(res,bag_nums,'stat_replay_failure_bag_group_by_mode',mode)
            if workflow_type!='probe_detect':
                add_value_to_dict(res,bag_nums,'stat_failure_bag_group_by_type',workflow_type)
            
        # consuming
        if bags_profile_summary is not None:
            res['stat_replay_time_consuming_group_by_category'] = merge_dicts({device:{category:bags_profile_summary}},res['stat_replay_time_consuming_group_by_category'])

    consumer.close()
    
    print('pod',stat_dt,leng)
    
    stat_action = StatisticsActions()
    for name, info in res.items():
        stat_action.add_or_update_statistics(name=name,period='daily',stat_date=dt,info=info)

def stat_bag():
    # stat_dt = datetime(2023,10,27)
    # dt = datetime(2023,10,26)
    stat_dt = datetime.now()
    dt = stat_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    topic = 'ars_prod_bag_result'
    timestamp = int(dt.timestamp() * 1000)
    consumer = KafkaConsumer(
        # topic,
        bootstrap_servers=['10.10.2.224:9092','10.10.2.81:9092','10.10.3.141:9092'],
        group_id='stat_bag',
        value_deserializer=lambda x: x.decode('utf-8'),
        auto_offset_reset='earliest')
    partitions = consumer.partitions_for_topic(topic)
    tp_list = [TopicPartition(topic, p) for p in partitions]
    offsets = consumer.offsets_for_times({tp: timestamp for tp in tp_list})
    consumer.assign(tp_list)
    for tp in tp_list:
        offset_and_timestamp = offsets[tp]
        if offset_and_timestamp is not None:
            consumer.seek(tp, offset_and_timestamp.offset)
    latest_timestamps = {}
    
    res = {
        'stat_replay_success_bag_duration_group_by_mode':{},
        'stat_replay_success_bag_duration_group_by_category':{},
        'stat_replay_error_bag_count_group_by_category':{},
    }
    leng = 0
    
    for message in consumer:
        ts = message.timestamp
        latest_timestamps[message.partition] = ts
        if latest_timestamps and all(ts >= stat_dt.timestamp()*1000 for ts in latest_timestamps.values()):
            break
        # if ts > 1000*datetime_to_timestamp(datetime(2023,10,27)):
        #     continue
        # print(datetime.fromtimestamp(ts/1000))
        
        one = json.loads(message.value)
        leng += 1
        
        # init
        workflow_type = one['type']
        group = one['group']
        if group == 'cp':
            group = 'CP'
        error_stage = one['error_stage']
        error_type = one['error_type']
        mode = None
        if 'extra_args' in one['config'] and 'mode' in one['config']['extra_args']:
            mode = one['config']['extra_args']['mode']
        output_bag = one['output_bag']
        duration = None
        if 'bag_duration' in one['metric']:
            duration = one['metric']['bag_duration']
            
        # cal
        if workflow_type=='replay' and output_bag!='' and duration is not None:
            if mode:
                add_value_to_dict(res,duration,'stat_replay_success_bag_duration_group_by_mode',mode)
            add_value_to_dict(res,duration,'stat_replay_success_bag_duration_group_by_category',group)
            
        if workflow_type == 'replay' and error_type!='':
            add_value_to_dict(res,1,'stat_replay_error_bag_count_group_by_category',group,error_stage,error_type)
            
    consumer.close()
    
    print('bag',stat_dt,leng)
    stat_action = StatisticsActions()
    for name, info in res.items():
        stat_action.add_or_update_statistics(name=name,period='daily',stat_date=dt,info=info)
        
        
def run():
    stat_pod()
    stat_bag()
    
if __name__ == "__main__":
    run()

