from data_source.database.postgres_db import PostgresDb
from util import load_config_file
from py2neo.data import Node
from manager.string_preprocess import tokenized_word, word_correction
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from manager.text_mdm_ratio import mdm_text
from data_source.database.cypher_db import CypherDb
from manager.text_mdm_ratio import levenshtein_ratio

from jaro import jaro_winkler_metric

import numpy as np
import pandas as pd
import uuid

pd.options.mode.chained_assignment = None

config_details = load_config_file('config/config.json')
db = PostgresDb(config_details['postgres_db'])



def jaro_similarity_ratio(str_1, str_2):
    return np.round(jaro_winkler_metric(str_1, str_2), decimals=3) * 100


def mean_round(value):
    value_floor = np.floor(value)
    if value_floor < value:
        res = value_floor + 1
    else:
        res = value_floor
    return np.round(res, decimals=0)


def boolean_matrix_rank(matrix):
    unique, count = np.unique(matrix, return_counts=True)
    frequencies = np.asarray((unique, count)).T
    if len(count) > 1:
        if count.max() == count.min():
            rank = (len(matrix) / 2) if len(matrix) > 2 else (len(matrix) / 2) + 1
        elif (frequencies[0][1] - frequencies[1][1]) == -1:
            rank = np.round(count.max() / count.min(), decimals=0) + 1
        # elif frequencies[0][1] == len(matrix):
        #     rank = np.round(count.max() / count.min(), decimals=0) - 1
        else:
            rank = np.round(count.max() / count.min(), decimals=0)
    else:
        rank = len(count)
    if len(matrix) < rank:
        rank = (len(matrix) * 2) - rank
    return int(rank)


def similarity_calculation(df):
    df.replace(np.NAN, '', inplace=True)
    df['matrix_1'] = df['addr_line_1'].apply(lambda x: tokenized_word(x))
    df['matrix_1'] = df['matrix_1'].apply(lambda x: word_correction(x, config_details['abbreviation']))
    df['matrix_1'] = df['matrix_1'].apply(lambda x: ' '.join(x))
    df['matrix_2'] = df['addr_line_2'].apply(lambda x: tokenized_word(x))
    df['matrix_2'] = df['matrix_2'].apply(lambda x: word_correction(x, config_details['abbreviation']))
    df['matrix_2'] = df['matrix_2'].apply(lambda x: ' '.join(x))
    df['matrix_1'] = df.apply(lambda x: line_correction(x['matrix_1'], x['matrix_2']),
                                             axis=1)
    df['row_data'] = df[['matrix_1', 'matrix_2']].apply(lambda x: ' '.join(map(str, x)), axis=1)
    input_df = df.copy()
    input_df.drop_duplicates(subset='matrix_1', inplace=True)

    text = input_df['matrix_1'].values
    tidf_vectorized = TfidfVectorizer()
    X = tidf_vectorized.fit_transform(input_df['matrix_1'])
    tf_matrix = cosine_similarity(X, X)

    tf_matrix = np.where(tf_matrix > 0.5, 1, 0)
    n_clusters = boolean_matrix_rank(tf_matrix)
    print(text, n_clusters)
    # unique, count = np.unique(tf_matrix[0], return_counts=True)
    #
    # n_clusters = int(np.floor((tf_matrix.mean() / (len(tf_matrix) * 2)) + 1))

    # tf_matrix = np.where(tf_matrix > 0.3, 0, tf_matrix)
    # X_df = pd.DataFrame(tf_matrix)
    # X_df['mean'] = X_df.apply(lambda x: sum(x) / len(tf_matrix), axis=1)
    # X_df['mean'] = np.round(X_df['mean'], decimals=0)
    # tf_m_unique_grp = len(np.unique(np.around(tf_matrix, decimals=3)))
    # if tf_m_unique_grp == 2:
    #     n_clusters = tf_m_unique_grp
    # else:
    #     n_clusters = X_df['mean'].nunique()
    max_iter = len(tf_matrix) * len(tf_matrix)
    model = KMeans(n_clusters=n_clusters, init='k-means++', max_iter=max_iter, n_init=1)
    model.fit(X)

    for i in set(model.labels_):
        current_cluster = [text[x] for x in np.where(model.labels_ == i)[0]]
        for value in current_cluster:
            df.loc[df['matrix_1'] == value, 'cluster_label'] = i

    return df


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
    columns = ['addr_1_tokenize', 'addr_2_tokenize', 'master_record_2', 'master_record_1', 'master_city']
    raw_df.replace(np.NAN, '', inplace=True)
    raw_df['addr_1_tokenize'] = raw_df['addr_line_1'].apply(lambda x: tokenized_word(x))
    raw_df['addr_2_tokenize'] = raw_df['addr_line_2'].apply(lambda x: tokenized_word(x))
    raw_df['addr_1_tokenize'] = raw_df['addr_1_tokenize'].apply(lambda x: word_correction(x, config['abbreviation']))
    raw_df['addr_2_tokenize'] = raw_df['addr_2_tokenize'].apply(lambda x: word_correction(x, config['abbreviation']))
    raw_df['master_record_1'] = ' '.join(mdm_text(raw_df, 'addr_1_tokenize', config['abbreviation']))
    raw_df['master_record_2'] = ' '.join(mdm_text(raw_df, 'addr_2_tokenize', config['abbreviation']))
    raw_df['master_record_1'] = raw_df.apply(lambda x: line_correction(x['master_record_1'], x['master_record_2']),
                                             axis=1)
    raw_df['master_city'] = raw_df.apply(lambda x: get_city(x['zip']), axis=1)
    raw_df['scores'] = raw_df.apply(lambda x: avg_scores(levenshtein_ratio(x['addr_line_1'], x['master_record_1']),
                                                         levenshtein_ratio(x['addr_line_2'], x['master_record_2']),
                                                         levenshtein_ratio(x['city'], x['master_city'])),
                                    axis=1)
    raw_df.drop(columns=columns, inplace=True)
    return raw_df


df = db.get_data('SELECT * FROM {};'.format(config_details['postgres_tables']['stage_table']))

print("Pulled data")

# df = df[df['presc_id'] == 1000]

res_df = []
for key, value_df in df.groupby(['presc_id']):
    if len(value_df) > 1:
        value_df.replace(np.NAN, '', inplace=True)
        value_df['matrix_1'] = value_df['addr_line_1'].apply(lambda x: tokenized_word(x))
        value_df['matrix_1'] = value_df['matrix_1'].apply(lambda x: word_correction(x, config_details['abbreviation']))
        value_df['matrix_1'] = value_df['matrix_1'].apply(lambda x: ' '.join(x))
        value_df['matrix_2'] = value_df['addr_line_2'].apply(lambda x: tokenized_word(x))
        value_df['matrix_2'] = value_df['matrix_2'].apply(lambda x: word_correction(x, config_details['abbreviation']))
        value_df['matrix_2'] = value_df['matrix_2'].apply(lambda x: ' '.join(x))
        value_df['row_data'] = value_df[['matrix_1', 'matrix_2']].apply(lambda x: ' '.join(map(str, x)), axis=1)
        value_df['row_data'] = value_df['row_data'].apply(lambda x: x.strip())
        count = 0
        value_df['label'] = ''
        for i in range(len(value_df)):
            mapping_df = value_df.loc[value_df['label'] == '']['row_data']
            if len(mapping_df.values) > 0:
                mapping_text = mapping_df.values.tolist()
                mapping_text.sort(reverse=True)
                mapping_text = mapping_text[0]
                print(mapping_text)
                for index, value in value_df['row_data'].iteritems():
                    value_df.loc[index, 'score_jaro'] = jaro_similarity_ratio(mapping_text, value)

                value_df.loc[value_df['score_jaro'] > 80, 'label'] = count
                count = count + 1
        #value = similarity_calculation(value)
        # value['door_num'] = value['addr_line_1'].apply(lambda x: x.split(" ")[0])
        for cluster_key, cluster_value in value_df.groupby(['label']):
            # df = mdm_transformation(cluster_value, config_details)
            # df['rank'] = df['scores'].rank(ascending=0)
            # df['weight'] = df['scores'].rank(ascending=1)
            cluster_value['uid'] = uuid.uuid4()
            # df.drop(columns=['matrix'], inplace=True)
            res_df.append(cluster_value)
    else:
        # df = mdm_transformation(value, config_details)
        # df['rank'] = df['scores'].rank(ascending=0)
        # df['weight'] = df['scores'].rank(ascending=1)
        value_df['uid'] = uuid.uuid4()
        res_df.append(value_df)
res_df = pd.concat(res_df, sort=True)
res_df.reset_index(drop=True, inplace=True)
res_df.to_csv('unsuper_1_1_1_.csv')

print("data transform completed")

db.put_data(res_df, config_details['postgres_tables']['processed_table'], load_type='replace')

res_df['status'] = 'unresolved'
mdm_df = []
cut_of_scores = int(config_details['cut_of_scores'])
for key, value in res_df.groupby(['uid']):
    if (value['scores'] > cut_of_scores).any():
        value = value.loc[(value['scores'] > cut_of_scores)]
        value = value.loc[value['rank'] == value['rank'].min()]
        value.reset_index(drop=True, inplace=True)
        value = value.head(1)
        value['status'] = 'system resolved'
    mdm_df.append(value)
mdm_df = pd.concat(mdm_df)
unresolved_mdm_df = mdm_df[mdm_df['status'] == 'unresolved']
resolved_mdm_df = mdm_df[mdm_df['status'] == 'system resolved']
resolved_mdm_df.reset_index(drop=True, inplace=True)
unresolved_mdm_df.reset_index(drop=True, inplace=True)
mdm_df.reset_index(drop=True, inplace=True)

db.put_data(resolved_mdm_df, config_details['postgres_tables']['mdm_table'], load_type='replace')
db.put_data(unresolved_mdm_df, config_details['postgres_tables']['unresolved_cluster_table'], load_type='replace')

db_g = CypherDb(config_details['neo4j_details'])

mdm_df['uid'] = mdm_df['uid'].apply(lambda x: str(x))

for index, row in mdm_df.iterrows():
    row['name'] = str(row['addr_id'])
    row['community'] = 9
    row_dict = row.to_dict()
    if row_dict['addr_line_2'] is None or row_dict['addr_line_2'] == "":
        del row_dict['addr_line_2']
    graph = db_g.connection()
    node = Node('address_details', **row)
    graph.create(node)

graph = db_g.connection()
query_list = [
    'match (n:address_details) '
    'with distinct(n.presc_id) as presc '
    'create (:prescribers {presc_id: presc, name: toString(presc), community: 10})',

    'match (n:address_details), (m:prescribers) '
    'where n.presc_id = m.presc_id '
    'create (m)-[:prescribe {weight: n.weight}]->(n)',

    'match (n:address_details) '
    'with distinct(n.state) as state '
    'create (:states {state_name: state, name: toString(state), community: 12})',

    'match (n:address_details), (m: states) '
    'where n.state = m.state_name '
    'create (m)-[:state {weight: 0.05}]->(n)',

    'match (n:address_details) '
    'with distinct(n.city) as city '
    'create (:citys {city_name: city, name: toString(city), community: 14})',

    'match (n:address_details), (m: citys) '
    'where n.city = m.city_name '
    'create (m)-[:city {weight: 0.05}]->(n)',

    'match (n:address_details) '
    'with distinct (n.zip) as zip '
    'create (:zipcodes {zip_no: zip, name: toString(zip), community: 17})',

    'match (n:address_details), (m: zipcodes) '
    'where n.zip = m.zip_no '
    'create (m)-[:zipcode {weight: 0.05}]->(n)'
]

for query in query_list:
    graph = db_g.connection()
    graph.run(query)

print("Completed")
