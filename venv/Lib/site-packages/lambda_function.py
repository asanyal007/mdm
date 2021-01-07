import load_data_to_postgres
import mdm_ranking_generation
import save_to_s3
import get_stats
from data_source.database.postgres_db import PostgresDb
from util import load_config_file
config_details = load_config_file('config/config.json')
db = PostgresDb(config_details['postgres_db'])





def lambda_handler(event, context):
    # Use a breakpoint in the code line below to debug your script.

    if __name__ == '__main__':
        load_data_to_postgres.load_data()
        mdm_ranking_generation.ranking_gen()
        ''' Saving golden records to s3'''
        df_golden = db.get_data("SELECT * FROM {0} ;".format(config_details['postgres_tables']['mdm_table']))
        save_to_s3.save_s3(bucket_name="wns-eiq-outputdocs", df=df_golden, file_name="Golden_Record",
                           cols=config_details['final_output_cols'])
        ''' Saving the stats to s3'''
        data = get_stats.stats()
        save_to_s3.save_s3(bucket_name="wns-eiq-outputdocs", df=data, file_name="Stats",
                           cols=config_details['final_output_cols'])


lambda_handler(0,0)

