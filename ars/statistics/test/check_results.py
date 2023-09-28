from lib.utils.db import get_engine
import pandas as pd
import json
from collections import defaultdict
from beautifultable import BeautifulTable

sql = """
SELECT name,info FROM statistics
where stat_date between '2023-9-18' and '2023-9-24' and period = 'daily';
"""

prod_engine = get_engine('./etc/prod_mysql.conf')
local_engine = get_engine('./etc/local_mysql.conf')


def get_cnt(engine):
    with engine.connect() as connection:
        prod_doc = pd.read_sql(
            sql, con=connection).astype({
                "name": str,
                "info": str
            })
    res = defaultdict(lambda: defaultdict(int))
    consuming = dict()
    for _, row in prod_doc.iterrows():
        info = json.loads(row['info'])
        match row['name']:
            case 'stat_replay_time_consuming_group_by_category':
                continue
            case _:
                for k,v in info.items():
                    if not isinstance(v,int):
                        break
                    res[row['name']][k]+=v
    res = pd.concat({k: pd.Series(v) for k, v in res.items()}).reset_index()
    res.columns = ['Name', 'Key', 'Value']
    return res


res_local, res_prod = pd.DataFrame(get_cnt(local_engine)), pd.DataFrame(
    get_cnt(prod_engine))
# print(res_local,res_prod)
cmp = pd.merge(
    res_local,
    res_prod,
    on=['Name', 'Key'],
    how='outer',
    suffixes=('_local', '_prod'))
cmp['Value_prod'] = cmp['Value_prod'].fillna(0).astype(int)
table = BeautifulTable()
table.column_headers = list(cmp.columns)
for i in range(len(cmp)):
    table.append_row(cmp.iloc[i])
print(table)
# cmp.to_csv('./cmp.csv',index=False)
