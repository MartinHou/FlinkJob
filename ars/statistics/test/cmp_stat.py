from lib.db import get_engine
import pandas as pd
import json
from collections import defaultdict
from beautifultable import BeautifulTable
"""
Compare statistics between prod and local
"""

START, END = '2023-11-20', '2023-11-21'

prod_sql = f"""
SELECT name,info FROM statistics
where name not like 'REALTIME%' and stat_date >= '{START}' and stat_date < '{END}' and period="daily";
"""
local_sql = f"""
SELECT name,info FROM statistics
where stat_date >= '{START}' and stat_date < '{END}' and period="daily";
"""

prod_engine = get_engine('./etc/prod_mysql.conf')
local_engine = get_engine('./etc/local_mysql.conf')


def get_cnt(engine, sql):
    with engine.connect() as connection:
        prod_doc = pd.read_sql(
            sql, con=connection).astype({
                "name": str,
                "info": str
            })
    res = defaultdict(lambda: defaultdict(float))
    for _, row in prod_doc.iterrows():
        info = json.loads(row['info'])
        if row['name'] == 'stat_replay_error_bag_count_group_by_category':
            continue
        elif row['name'] == 'stat_replay_time_consuming_group_by_category':
            for device, ele in info.items():
                for group, ele1 in ele.items():
                    if 'total' not in ele1 or 'total' not in ele1['total']:
                        continue
                    res[device][group] += ele1['total']['total']
        else:
            for k, v in info.items():
                if not isinstance(v, int) and not isinstance(v, float):
                    break
                res[row['name']][k] += v
    res = pd.concat({k: pd.Series(v) for k, v in res.items()}).reset_index()
    res.columns = ['Name', 'Key', 'Value']
    return res


res_local, res_prod = pd.DataFrame(get_cnt(local_engine,
                                           local_sql)), pd.DataFrame(
                                               get_cnt(prod_engine, prod_sql))
# print(res_local,res_prod)
cmp = pd.merge(
    res_local,
    res_prod,
    on=['Name', 'Key'],
    how='outer',
    suffixes=('_local', '_prod'))
cmp['Value_prod'] = cmp['Value_prod'].fillna(0).astype(int)
cmp['Value_local'] = cmp['Value_local'].fillna(0).astype(int)
cmp['diff'] = cmp['Value_local'] - cmp['Value_prod']
cmp['diff(%)'] = (cmp['diff'] / cmp['Value_prod'] * 100).round(2)
cmp = cmp.sort_values(by='diff(%)')
table = BeautifulTable()
table.column_headers = list(cmp.columns)
for i in range(len(cmp)):
    table.append_row(cmp.iloc[i])
print(table)
# cmp.to_csv('./cmp.csv',index=False)
