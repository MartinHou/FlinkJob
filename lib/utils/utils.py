import requests
from typing import List
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential
from typing import Optional, List
from collections import defaultdict


# Network IO
@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=1, max=5))
def http_request(
        method: str,
        url: str,
        data: Optional[dict] = None,
        json: Optional[dict] = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
        skip: Optional[List[int]] = None,
):

    r = requests.request(
        method=method,
        url=url,
        data=data,
        json=json,
        params=params,
        headers=headers,
        timeout=timeout,
    )

    if skip and r.status_code in skip:
        return r

    if r.status_code >= 400:
        print(
            f"Failed to {method.upper()} -> '{url}', status code: {r.status_code}, error: {r.text}"
        )

    r.raise_for_status()

    print(
        f"Success {method.upper()} -> '{url}' with status code: {r.status_code}"
    )

    return r


def defaultdict2dict(obj: any):
    """
    Transform defaultdict to dict
    
    params:
        obj: any object (defaultdict if called)
        
    return:
        dict: a python dict
    """
    if isinstance(obj, defaultdict):
        obj = {k: defaultdict2dict(v) for k, v in obj.items()}
    return obj


def merge_dicts(dict1, dict2):
    """Merge two dictionaries with possibly nested keys with int/float endpoint."""
    if not dict1:
        return dict2
    if not dict2:
        return dict1
    merged = {}
    for key in set(dict1.keys()).union(dict2.keys()):
        if key in dict1 and key in dict2:
            # 如果key对应的值是int或float，则直接相加
            if isinstance(dict1[key], (int, float)) and isinstance(dict2[key], (int, float)):
                merged[key] = dict1[key] + dict2[key]
            # 如果key对应的值仍然是字典，则递归处理
            elif all(isinstance(dict_[key], dict) for dict_ in [dict1, dict2]):
                merged[key] = merge_dicts(dict1[key], dict2[key])
            else:
                # 保留有该key的那个dict的值
                merged[key] = dict1[key] if key in dict1 else dict2[key]
        else:
            merged[key] = dict1[key] if key in dict1 else dict2[key]
    return merged

def add_value_to_dict(dictionary, value, *keys):
    temp = dictionary
    for key in keys[:-1]:
        if key not in temp or not isinstance(temp[key], dict):
            temp[key] = {}
        temp = temp[key]
    if keys[-1] not in temp:
        temp[keys[-1]] = value
    else:
        temp[keys[-1]] += value
        
def overwrite_value_to_dict(dictionary, value, *keys):
    temp = dictionary
    for key in keys[:-1]:
        if key not in temp or not isinstance(temp[key], dict):
            temp[key] = {}
        temp = temp[key]
    temp[keys[-1]] = value
    
if __name__=='__main__':
    a = {'a':{'b':3}}
    overwrite_value_to_dict(a,1,'a','b')
    print(a)