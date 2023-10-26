def merge_dicts(dict1, dict2):
    """Merge two dictionaries with possibly nested keys."""
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

# 使用示例：
dict1 = {'a': {'b': 1.0}, 'c': 2}
dict2 = {'a': {'b': 3, 'd': 4.0}, 'e': 5}
merged = merge_dicts(dict1, dict2)
print(merged)