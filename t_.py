from data_source.database.postgres_db import PostgresDb
from util import load_config_file
from util import slack_web_hook
from py2neo.data import Node
from manager.string_preprocess import tokenized_word, word_correction
from manager.text_mdm_ratio import mdm_text
from data_source.database.cypher_db import CypherDb
from manager.text_mdm_ratio import levenshtein_ratio

from jaro import jaro_winkler_metric
import re

import numpy as np
import pandas as pd
import uuid
import logging
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

config_details = load_config_file('config/config.json')
db = PostgresDb(config_details['postgres_db'])

db_g = CypherDb(config_details['neo4j_details'])


def bad_cluster_selection(cluster_df):
    selection_res = False
    uni_pid_zip = cluster_df[['presc_id', 'zip']].drop_duplicates().values.tolist()
    for item in uni_pid_zip:
        res = cluster_df.loc[(cluster_df['presc_id'] == item[0]) & (cluster_df['zip'] == item[1]), 'uid']
        if res.unique().size > 1:
            selection_res = True
    return selection_res


def jaro_similarity_ratio(str_1, str_2):
    return np.round(jaro_winkler_metric(str_1, str_2), decimals=3) * 100


def avg_scores(a, b, c):
    return (a + b + c) / 3


def get_city(zip_code):
    zip_code = str(zip_code).zfill((5 - len(str(zip_code))) + len(str(zip_code)))
    query_str = "SELECT city FROM \"{}\" where zip='{}';".format(config_details['postgres_tables']['location_details'],
                                                                 zip_code)
    return db.get_data(query_str).to_dict(orient='records')[0]['city']


def line_correction(str_1, str_2):
    from collections import Counter
    if len(str_2) > 0:
        str_2 = str_2.split(" ")
        str_1 = str_1.split(" ")
        res = list((Counter(str_1) - Counter(str_2)).elements())
        return ' '.join(res)
    else:
        return str_1


def mdm_transformation(raw_df, config):
    columns = ['matrix_1', 'matrix_2', 'master_record_2', 'master_record_1', 'master_city', 'raw_data', 'len_raw_data',
               'label']
    raw_df.replace(np.NAN, '', inplace=True)
    raw_df['master_record_1'] = ' '.join(mdm_text(raw_df, 'matrix_1', config['abbreviation']))
    raw_df['master_record_2'] = ' '.join(mdm_text(raw_df, 'matrix_2', config['abbreviation']))
    raw_df['master_record_1'] = raw_df.apply(lambda x: line_correction(x['master_record_1'], x['master_record_2']),
                                             axis=1)
    raw_df['master_city'] = raw_df.apply(lambda x: get_city(x['zip']), axis=1)
    raw_df['scores'] = raw_df.apply(lambda x: avg_scores(levenshtein_ratio(x['addr_line_1'], x['master_record_1']),
                                                         levenshtein_ratio(x['addr_line_2'], x['master_record_2']),
                                                         levenshtein_ratio(x['city'], x['master_city'])),
                                    axis=1)
    raw_df.drop(columns=columns, inplace=True)
    return raw_df


def main():

    df = db.get_data(
        'SELECT * FROM {0};'.format(config_details['postgres_tables']['stage_table']))

    logging.info(
        "Time Taken for get data from db : {time} sec".format(time=int((datetime.now() - start_time).total_seconds())))
    logging.info(
        "Time Taken for get data from db : {time} sec".format(time=int((datetime.now() - start_time).total_seconds())))
    logging.info("Memory Usage {time} in MB".format(
        time=np.around(df.memory_usage(index=True, deep=True).sum() / 1024 / 1024, 2)))
    logging.info("Data Pre-processing")

    slack_web_hook(
        "Time Taken for get data from db : {time} sec".format(time=int((datetime.now() - start_time).total_seconds())))
    slack_web_hook(
        "Time Taken for get data from db : {time} sec".format(time=int((datetime.now() - start_time).total_seconds())))
    slack_web_hook("Memory Usage {time} in MB".format(
        time=np.around(df.memory_usage(index=True, deep=True).sum() / 1024 / 1024, 2)))
    slack_web_hook("Data Pre-processing")

    res_df = []
    for key, value_df in df.groupby(['presc_id']):
        if len(value_df) > 1:
            value_df.replace(np.NAN, '', inplace=True)
            value_df['matrix_1'] = value_df['addr_line_1'].apply(lambda x: tokenized_word(x))
            value_df['matrix_1'] = value_df['matrix_1'].apply(
                lambda x: word_correction(x, config_details['abbreviation']))
            value_df['matrix_1'] = value_df['matrix_1'].apply(lambda x: ' '.join(x))
            value_df['matrix_2'] = value_df['addr_line_2'].apply(lambda x: tokenized_word(x))
            value_df['matrix_2'] = value_df['matrix_2'].apply(
                lambda x: word_correction(x, config_details['abbreviation']))
            value_df['matrix_2'] = value_df['matrix_2'].apply(lambda x: ' '.join(x))
            value_df['raw_data'] = value_df[['matrix_1', 'matrix_2']].apply(lambda x: ' '.join(map(str, x)), axis=1)
            value_df['raw_data'] = value_df['raw_data'].apply(lambda x: x.strip())
            value_df['len_raw_data'] = value_df['raw_data'].apply(lambda x: len(x))
            value_df['house_no'] = value_df['addr_line_1'].apply(lambda x: x.split(" ")[0])
            value_df.sort_values('len_raw_data', ascending=False, inplace=True)
            count = 0
            value_df['label'] = ''
            for i in range(len(value_df)):
                mapping_df = value_df.loc[value_df['label'] == '']['raw_data']
                if len(mapping_df.values) > 0:
                    mapping_text = mapping_df.values.tolist()
                    mapping_text = mapping_text[0]
                    for index, value in value_df['raw_data'].iteritems():
                        if value_df.loc[index, 'label'] == '' and jaro_similarity_ratio(mapping_text, value) > 80:
                            value_df.loc[index, 'label'] = count
                    count = count + 1
            for cluster_key, cluster_value in value_df.groupby(['label']):
                cluster_value['uid'] = uuid.uuid4()
                unique_house_no = [tokenized_word(res)[0] for res in cluster_value['house_no'].unique() if
                                   len(re.findall(r'\d+[A-Z 0-9]', res)) > 0]
                unique_house_no = list(set(unique_house_no))
                if len(unique_house_no) > 1:
                    for house_no in unique_house_no:
                        res = cluster_value.loc[cluster_value['house_no'] == house_no]
                        res = mdm_transformation(res, config_details)
                        res['rank'] = res['scores'].rank(ascending=0)
                        res['weight'] = res['scores'].rank(ascending=1)
                        res['uid'] = uuid.uuid4()
                        res_df.append(res)
                else:
                    cluster_value = mdm_transformation(cluster_value, config_details)
                    cluster_value['rank'] = cluster_value['scores'].rank(ascending=0)
                    cluster_value['weight'] = cluster_value['scores'].rank(ascending=1)
                    cluster_value['uid'] = uuid.uuid4()
                    res_df.append(cluster_value)
        else:
            value_df['scores'] = 100
            value_df['rank'] = 1
            value_df['weight'] = 1
            value_df['uid'] = uuid.uuid4()
            res_df.append(value_df)
    res_df = pd.concat(res_df, sort=True)
    res_df.reset_index(drop=True, inplace=True)
    res_df.drop_duplicates(inplace=True)
    logging.info("Clustering and Ranking phase completed")
    logging.info("Memory Usage {time} in MB".format(
        time=np.around(res_df.memory_usage(index=True, deep=True).sum() / 1024 / 1024, 2)))

    slack_web_hook("Clustering and Ranking phase completed")
    slack_web_hook("Memory Usage {time} in MB".format(
        time=np.around(res_df.memory_usage(index=True, deep=True).sum() / 1024 / 1024, 2)))

    db.put_data(res_df, config_details['postgres_tables']['processed_table'], load_type='append')
    logging.info("Cluster and Rank allocated data load to DB completed")
    logging.info('total taken to cluster and Rank allocation  {total} sec'.format(
        total=int((datetime.now() - start_time).total_seconds())))

    logging.info("resolved and unresolved dataframe creation")

    slack_web_hook("Cluster and Rank allocated data load to DB completed")
    slack_web_hook('total taken to cluster and Rank allocation  {total} sec'.format(
        total=int((datetime.now() - start_time).total_seconds())))

    slack_web_hook("resolved and unresolved dataframe creation")

    res_df['status'] = 'unresolved'
    mdm_df = []
    cut_of_scores = int(config_details['cut_of_scores'])
    for _, p_df in res_df.groupby(['presc_id']):
        res = bad_cluster_selection(p_df)
        if res:
            mdm_df.append(p_df)
            continue

        for key, value in p_df.groupby(['uid']):
            if (value['scores'] > cut_of_scores).any():
                value = value.loc[(value['scores'] > cut_of_scores)]
                value = value.loc[value['rank'] == value['rank'].min()]
                value.reset_index(drop=True, inplace=True)
                value = value.head(1)
                value['status'] = 'system resolved'
            mdm_df.append(value)
    mdm_df = pd.concat(mdm_df)
    if 'house_no' in mdm_df.columns:
        mdm_df.drop(columns='house_no', inplace=True)
    unresolved_mdm_df = mdm_df[mdm_df['status'] == 'unresolved']
    resolved_mdm_df = mdm_df[mdm_df['status'] == 'system resolved']
    resolved_mdm_df.reset_index(drop=True, inplace=True)
    unresolved_mdm_df.reset_index(drop=True, inplace=True)
    mdm_df.reset_index(drop=True, inplace=True)

    logging.info("load resolved and unresolved dataframe to DB")
    slack_web_hook("load resolved and unresolved dataframe to DB")
    db.put_data(resolved_mdm_df, config_details['postgres_tables']['mdm_table'], load_type='append')
    db.put_data(unresolved_mdm_df, config_details['postgres_tables']['unresolved_cluster_table'], load_type='append')

    df_to_gdb(mdm_df)

    logging.info("load Completed")
    logging.info(
        'total Process time taken  : {total} sec'.format(total=int((datetime.now() - start_time).total_seconds())))

    slack_web_hook("load Completed")
    slack_web_hook(
        'total Process time taken  : {total} sec'.format(total=int((datetime.now() - start_time).total_seconds())))


def df_to_gdb(mdm_df):
    logging.info("load data to Graph Database")
    slack_web_hook("load data to Graph Database")

    mdm_df['uid'] = mdm_df['uid'].apply(lambda x: str(x))
    for index, row in mdm_df.iterrows():
        row['name'] = 'Address : ' + str(row['addr_id'])
        row['community'] = 9
        row_dict = row.to_dict()
        if row_dict['addr_line_2'] is None or row_dict['addr_line_2'] == "":
            del row_dict['addr_line_2']
        graph = db_g.connection()
        node = Node('address_details', **row)
        graph.create(node)


def graph_relation_map():
    logging.info("Run Cypher Query for Relation mapping")
    slack_web_hook("Run Cypher Query for Relation mapping")
    query_list = [
        'match (n:address_details) '
        'with distinct(n.presc_id) as presc '
        'create (:prescribers {presc_id: presc, name: \'Prescriber : \' + toString(presc), community: 10})',

        'match (n:address_details), (m:prescribers) '
        'where n.presc_id = m.presc_id '
        'create (m)-[:prescribe {weight: n.weight}]->(n)',

        'match (n:address_details) '
        'with distinct(n.state) as state '
        'create (:states {state_name: state, name: \'State : \' + toString(state), community: 12})',

        'match (n:address_details), (m: states) '
        'where n.state = m.state_name '
        'create (m)-[:state {weight: 0.05}]->(n)',

        'match (n:address_details) '
        'with distinct(n.city) as city '
        'create (:citys {city_name: city, name: \'City : \' + toString(city), community: 14})',

        'match (n:address_details), (m: citys) '
        'where n.city = m.city_name '
        'create (m)-[:city {weight: 0.05}]->(n)',

        'match (n:address_details) '
        'with distinct (n.zip) as zip '
        'create (:zipcodes {zip_no: zip, name: \'Zip : \' + toString(zip), community: 17})',

        'match (n:address_details), (m: zipcodes) '
        'where n.zip = m.zip_no '
        'create (m)-[:zipcode {weight: 0.05}]->(n)'
    ]

    for query in query_list:
        graph = db_g.connection()
        graph.run(query)

    logging.info("load data to Graph Database completed")


def get_presc_id():
    df = db.get_data(
        'SELECT presc_id FROM {0};'.format(config_details['postgres_tables']['stage_table']))
    df.drop_duplicates(inplace=True)
    return df['presc_id'].tolist()


result_list = []


def log_result(result):
    result_list.append(result)


if __name__ == '__main__':
    logging.info('=' * 120)
    start_time = datetime.now()
    main()
    graph_relation_map()
    logging.info(
        "Total Time taken: {time} sec".format(time=int((datetime.now() - start_time).total_seconds())))
    logging.info('=' * 120)

