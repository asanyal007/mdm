
import pandas as pd

import json
from util import load_config_file
from data_source.database.postgres_db import PostgresDb
import requests


web_hook = 'https://hooks.slack.com/services/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'

data = {"text": "*python*"}
t = requests.post(url=web_hook, json=data)
print(t.text)

# config_details = load_config_file('config/config.json')
# db = PostgresDb(config_details['postgres_db'])
#
# df = pd.read_csv('us_1.csv')
# df.drop(['test'], axis=1, inplace=True)
# db.put_data(df, 'state_city_zip')
#
# print()


# df = pd.read_json('D:/data/address_MDM/data/us_details.json', orient='records')
#
# db.put_data(df, 'state_city_zip', load_type='replace')






# from multiprocessing import Pool
#
# def f(x):
#     return x*x
#
# if __name__ == '__main__':
#     import datetime
#
#     start_time = datetime.datetime.now()
#     for i in range(10000):
#         print(f(i))
#     print("--- %s seconds ---" % (datetime.datetime.now() - start_time).total_seconds())
#     start_time = datetime.datetime.now()
#     with Pool(10) as p:
#         print(p.apply(f, [i for i in range(10000)]))
#     print("--- %s seconds ---" % (datetime.datetime.now() - start_time).total_seconds())
