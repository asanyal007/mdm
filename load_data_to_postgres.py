from util import load_config_file
from manager.excel_reader import sheet_merging
from util import slack_web_hook
from data_source.database.postgres_db import PostgresDb
import numpy as np
import logging
from datetime import datetime
import warnings
import pandas as pd
warnings.filterwarnings("ignore")


start_time = datetime.now()
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logging.info("Data Reading Started")
slack_web_hook("Data Reading Started")

config_details = load_config_file('config/config.json')
excel_detail = config_details['excel_detail']

df = pd.read_csv(excel_detail["path"])

error = df[pd.isnull(df.postal_code)]
error.dropna(subset=excel_detail['primary_columns_error'], inplace=True)
df.dropna(subset=excel_detail['primary_columns_res'], inplace=True)

df['postal_code'] = df['postal_code'].astype('int64')

db = PostgresDb(config_details['postgres_db'])

error.reset_index(drop=True, inplace=True)
df.reset_index(drop=True, inplace=True)

logging.info("Memory Usage {time} in MB".format(time=np.around(df.memory_usage(index=True, deep=True).sum()/1024/1024, 2)))
slack_web_hook("Memory Usage {time} in MB".format(time=np.around(df.memory_usage(index=True, deep=True).sum()/1024/1024, 2)))

db.put_data(df, config_details['postgres_tables']['stage_table'], load_type='replace')
db.put_data(error, config_details['postgres_tables']['error_table'], load_type='replace')
end_time = datetime.now()
logging.info("data insertion completed")
logging.info('total time taken : {total} sec'.format(total=int((end_time - start_time).total_seconds())))
slack_web_hook("data insertion completed")
slack_web_hook('total time taken : {total} sec'.format(total=int((end_time - start_time).total_seconds())))

