from lib.utils.db import get_engine
import pandas as pd

engine = get_engine('./etc/prod_mysql.conf')
date = 'LidarRB_1024_bag'

with open(f'mq_{date}.txt', 'r') as f:
    result_ids = [line.strip() for line in f]

with open(f'db_{date}.txt', 'r') as f:
    result_ids_copy = [line.strip() for line in f]

result_ids_diff = list(set(result_ids_copy) - set(result_ids))
print(result_ids_diff)
exit()
sql = """
    SELECT update_time from workflow where workflow_id='%s';
"""

df = pd.DataFrame(columns=['id', 'update_time'])
with engine.connect() as connection:
    
    for result_id in result_ids_diff:
        upd = connection.execute(sql % result_id).fetchone()[0]
        new_df = pd.DataFrame({'id': [result_id], 'update_time': [upd]})
        df = pd.concat([df,new_df], ignore_index=True)
df = df.sort_values('update_time')
df.to_csv(f'diff_{date}.csv', index=False)
