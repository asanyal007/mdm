import pandas as pd
from pandas_profiling import ProfileReport
from data_source.database.postgres_db import PostgresDb
from util import load_config_file
import os
cwd = os.getcwd()  # Get the current working directory (cwd)
print(cwd)
files = os.listdir(cwd)  # Get all the files in that directory
config_details = load_config_file('config/config.json')

class stats:
    def __init__(self, table_data):
        self.table_data = table_data

    def get_count(self):
        return len(self.table_data)

    def get_dataprofile(self, file_name):
        profile = ProfileReport(self.table_data, title="Report", minimal=True)
        profile.to_file("data_stats/{}.html".format(file_name))
        profile.to_file("data_stats/{}.csv".format(file_name))



if __name__ == '__main__':

    db = PostgresDb(config_details['postgres_db'])

    df_source = db.get_data("SELECT * FROM {0} ;".format(config_details['postgres_tables']['stage_table']))
    df_golden = db.get_data("SELECT * FROM {0} ;".format(config_details['postgres_tables']['mdm_table']))
    df_error = db.get_data("SELECT * FROM {0} ;".format(config_details['postgres_tables']['error_table']))
    df_unresolved = db.get_data("SELECT * FROM {0} ;".format(config_details['postgres_tables']['unresolved_cluster_table']))

    golden_to_source_ratio = (stats(df_golden).get_count()/stats(df_source).get_count())*100
    error_to_source_ratio = (stats(df_error).get_count()/stats(df_source).get_count())*100
    unresolved_to_golden_ratio = (stats(df_unresolved).get_count()/stats(df_golden).get_count())*100
    cols = ['Metrics','golden_to_source_ratio', 'source_to_error_ratio', 'unresolved_to_golden_ratio']
    data = pd.DataFrame([['Ratio',golden_to_source_ratio,error_to_source_ratio,unresolved_to_golden_ratio]], columns=cols )
    data.to_excel("data_stats/Ratio.xlsx")








