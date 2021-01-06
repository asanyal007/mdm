from data_source.database.postgres_db import PostgresDb
from util import load_config_file
import re
from sklearn.cluster import KMeans
from manager.string_preprocess import tokenized_word
from manager.string_preprocess import tokenized_word, word_correction
from manager.text_mdm_ratio import mdm_text
import math
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import collections
import numpy as np
from graphframes import *


import sys

print(sys.path)

config_details = load_config_file('config/config.json')

db = PostgresDb(config_details['postgres_db'])
df = db.get_data('SELECT * FROM {};'.format(config_details['postgres_tables']['unresolved_cluster_table']))
df_split = np.array_split(df, 2, axis=0)
print()
#df.to_csv('unresolved_cluster.csv')

print("ee")
# config_details = load_config_file('config/config.json')
#
# def calculate_position(values):
#     x = []
#     for pos, matrix in enumerate(values):
#         if pos > 0:
#             x.append(matrix)
#     return x
#
#
# def jaccard_similarity(query, document):
#     intersection = set(query).intersection(set(document))
#     union = set(query).union(set(document))
#     return len(intersection)/len(union)
#
#
# te = '4400 S 700 E'
# te_1 = '4400 S 700'
# a_te = tokenized_word(te)
# w_te = word_correction(a_te, config_details['abbreviation'])
#
# a_te_1 = tokenized_word(te_1)
# w_te_1 = word_correction(a_te_1, config_details['abbreviation'])
# re_ = jaccard_similarity(w_te, w_te_1)
#
# print(re_)
#
#
#
# def idf(n,df):
#     result = math.log((n+1.0)/(df+1.0)) + 1
#     return result
#
#
# import pandas as pd
#
# re = ['1320 CORPORATE DR.',
# '115 DARROW RD 1320',
# 'YES 1320',
# 'NO'
# ]

# df_1 = pd.DataFrame(data={"A": [
# 'HIGHWAY 68 #2243RINC',
# 'ST RD 68',
# '2243 NM-68',
# 'ST RD 68 2243 RINCONADA',
# ]})

# df_1 = pd.DataFrame(data={"name": ['84 HIGHLAND AVE',
# '84 HIGHLAND AVE STE',
# '84 HIGHLAND AVE STE 304',
# '162 FEDERAL ST',
# '1990 WISCONSIN AVE']})

df_1 = pd.DataFrame(data={"name": ['1320 CORPORATE DR.',
'5111 DARROW RD',
'9318 STATE ROUTE 14',
'9318 STATE ROUTE 14',
'9318 STATE ROUTE 14 2ND FL',
'9318 ST RT 14',
'9318 STATE ROUTE 14',
'9318 STATE ROUTE 14 FL 2'
]})

df_1['name_1'] = df_1['name'].apply(lambda x: tokenized_word(x))
df_1['name_1'] = df_1['name_1'].apply(lambda x: word_correction(x, config_details['abbreviation']))
df_1['name'] = df_1['name_1'].apply(lambda x: ' '.join(x))

# df_2 = pd.DataFrame(data={"src": [0, 0, 0, 1, 1, 1, 2, 2, 2,], "dst" : [1, 2, 4, 0, 2, 4, 0, 1, 4]})
#
# re_ = pd.merge(df_1, df_2)
#
# d = re_[~(re_['src']==re_['dst'])]
# d.drop(d.loc[d['src'] > d['dst']].index.tolist(), inplace=True)

# df_1['A_1'] = df_1['A'].apply(lambda x: x.split(" "))


value__text = df_1['name'].values

re = [
'1990 WISCONSIN AVENUE',
'1990 WISCONSIN AVE',
'1990 WISCONSIN AVE',
'1990 WISCONSIN AVE',
'1990 WISCONSIN AVE STOP 2'
]

# re = ['84 HIGHLAND AVE',
# '84 HIGHLAND AVE STE',
# '84 HIGHLAND AVE STE 304',
# '162 FEDERAL ST',
# '1990 WISCONSIN AVE']

tfidf_vectorizer = TfidfVectorizer(binary=True)
X = tfidf_vectorizer.fit_transform(df_1['name'])
t = cosine_similarity(X, X)
t_ = tfidf_vectorizer.get_feature_names()
tf_matrix = (X * X.T).A
# model = tfidf_vectorizer.fit(tf_matrix)


#tf_matrix = cosine_similarity(X).fit()
# tf_matrix = tfidf_vectorizer.transform(re).toarray()
#tf_matrix = np.where(1 > tf_matrix > 0, -1, 1)
unique, counts = np.unique(tf_matrix, return_counts=True)
frequencies = np.asarray((unique, counts)).T
df = pd.DataFrame(t)
df['mean'] = df.sum(axis=0)
df['mean'] = np.floor(df['mean'])
df['mean'] = df['mean'].round(decimals=0)
n_clusters = 0

n_clusters += df[df['mean'] == 1][['mean']].count()[0]
df = df[df['mean'] != 1]
for value in df.groupby(['mean']):
    n_clusters += 1

n_clusters = n_clusters
model = KMeans(n_clusters=n_clusters,init='k-means++', max_iter=16, n_init=1)
model.fit(X)
s = model.score(X)

t = model.predict(tfidf_vectorizer.transform(['84 HIGHLAND AVE STE']))
# t_1 = model.predict(tfidf_vectorizer.transform(['1320 CORPORATE DR.']))
# t_2 = model.predict(tfidf_vectorizer.transform(['115 DARROW RD 1320']))
# t_3 = model.predict(tfidf_vectorizer.transform(['YES 1320']))


cluster_assignments_dict = {}

# for i in set(model.labels_):
#     current_cluster = [value__text[x] for x in np.where(model.labels_ == i)[0]]
#     for values in current_cluster:
#         df_1.loc[df_1['A_1'] == values, 'cluster_label'] = i

for i in set(model.labels_):
    current_cluster_bills = [value__text[x] for x in np.where(model.labels_ == i)[0]]
    cluster_assignments_dict[i] = current_cluster_bills

# print("Top terms per cluster:")
# order_centroids = model.cluster_centers_.argsort()[:, ::-1]
# terms = tfidf_vectorizer.get_feature_names()
# for i in range(2):
#     print("Cluster %d:" % i),
#     for ind in order_centroids[i, :10]:
#         print(' %s' % terms[ind])
#
# print("\n")
# print("Prediction")


clusters = collections.defaultdict(list)
for i, label in enumerate(model.labels_):
    clusters[label].append(i)
clusters = dict(clusters)

for cluster in range(3):
    print ("cluster ",cluster,":")
    for i,sentence in enumerate(clusters[cluster]):
        print("\tsentence ",i,": ",re[sentence])



def door_no(x):
    value = x['addr_line_1'].split(" ")[0].replace("-", "")
    if value[:-1].isdigit() or value.isdigit():
        return x['addr_line_1'].split(" ")[0]
    elif x['addr_line_2'] is not None:
        value = x['addr_line_2']
        return value.split(" ")[0]
    else:
        res = re.sub(r'[a-z]+', '', x['addr_line_1'], re.I)
        return res



config_details = load_config_file('config/config.json')

db = PostgresDb(config_details['postgres_db'])
df = db.get_data('SELECT * FROM {};'.format(config_details['postgres_tables']['stage_table']))

df['door_no'] = df[['addr_line_1', 'addr_line_2']].apply(lambda x:  door_no(x), axis=1)
df


df = pd.read_json('us_details_1_1.json')
db.put_data(df, 'state_city_zip', load_type='replace')




df = db.get_data('SELECT * FROM mdm_selection where status <> \'unresolved\';')
df_1 = db.get_data('SELECT * FROM mdm_selection where status = \'unresolved\';')
df_2 = db.get_data('SELECT * FROM prescribed_address;')
df_3 = db.get_data('SELECT * FROM address_ranking;')
df.to_excel('mdm_result_.xlsx', sheet_name='resolved', engine='xlsxwriter')
df_1.to_excel('mdm_result_1.xlsx', sheet_name='unresolved', engine='xlsxwriter')
df_2.to_excel('mdm_result_11.xlsx', sheet_name='clusters', engine='xlsxwriter')
df_3.to_excel('mdm_result_11_.xlsx', sheet_name='clusters', engine='xlsxwriter')
df['data'] = df[['state_id', 'state_name', 'city']].apply(lambda x:'/'.join(x), axis=1)
res = []
for key, value in df.groupby('data'):
    data = {}
    data['state_id'] = value['state_id'].values[0]
    data['city'] = value['city'].values[0]
    data['state_name'] = value['state_name'].values[0]
    data['zips'] = list(value['zip'].values)
    data_copy = data.copy()
    res.append(data_copy)

import json
r = json.load(list(res))

print()
# df.set_index(['state_id', 'state_name', 'city'], inplace=True)
# df_1 = pd.DataFrame(data=df, index="/".join(df.index.values))
# df.reset_index()
# df['data'] = df.index.values
# df['data'] = list(df.index)
# res = []
# data = {'zips': []}
# for key, value in df.groupby('data'):
#
#     data['state_id'] = value['state_id']
#     data['city'] = value['city']
#     data['state_name'] = value['state_name']
#     data['zips'].append(value['zip'])




# import pandas as pd
# import json
#
# df = db.get_data('select * from prescribed_address')
# df = pd.read_json('us_details_1_1.json', orient='records')
# df.drop_duplicates(inplace=True)
#
# with open('us_details_1_1.json', 'w') as write:
#     json.dump(df.to_dict(orient='records'), write)
#
# db.put_data(df, 'state_city_zip', load_type='replace')
#
#
# query = "SELECT city FROM \"{}\" where zip={};".format("state_city_zip", 8009)
# res = db.get_data(query).to_dict(orient='records')[0]['city']
#
# query = "SELECT state, city, zip FROM \"{table_name}\";".format(table_name='address_ranking')
#
# res = db.get_data(query)
#
# res.drop_duplicates(inplace=True)
# res_1 = []
# for cluster_id, value in res.groupby('uid'):
#     res_3 = value.loc[(value['scores'] > 60) & value['rank'] == value['rank'].min()]
#     print()
#
# print()
# import pandas as pd
# import json
#
# df = pd.read_csv('us_details.csv')
# print(list(df.columns))
# df = df.loc[:,['state_id', 'state_name', 'city', 'zip']]
# df.drop_duplicates(inplace=True)
#
# df.fillna(0, inplace=True)
# df['zip'] = df['zip'].astype('int64')
# df.to_csv('us_details_1.csv', index=False)
#
# # db.put_data(df, 'state_city_zip', load_type='replace')
#
# # data = {
# #     "state": "NJ",
# #     "city": "COLLINGSWOOD",
# #     "zip": "8108"
# # }
# #
# # query = "SELECT * FROM \"MDM_Ranked\" Where state=\'{state}\' and city=\'{city}\' and zip=\'{zip}\';".format(**data)
# #
# # res = db.get_data(query)
# #
# # test = res['uid'].drop_duplicates().to_dict()
# #
# # for value in test:
# #     query = "DELETE FROM \"{table_name}\" WHERE uid = \'{value}\' AND addr_id = \'{addr_id}\';".format(
# #         table_name=table_name, value=value['uid'], addr_id=value['addr_id'])
# #     print(query)
# #     db.run_query(query)
# #
# # print()
#
# # import json
# # import pandas as pd
# #
# # with open('D:/data/us_details.json', 'r') as file:
# #     data = json.load(file)
# #
# # df = pd.read_json('D:/data/us_details.json', orient='records')
# # print()
#
#
# # import pandas as pd
# # import json
# # import numpy as np
# #
# # df = pd.read_csv('d:/data/us.csv')
# # df.fillna(0)
# # #df.replace(np.nan, " ",  inplace=True)
# # final = []
# # res = df.to_dict(orient='')
# # for index, row in df.iterrows():
# #     df_1 = {}
# #     df_1['state_id'] = row['state_id']
# #     df_1['state_name'] = row['state_name']
# #     df_1['city'] = row['city']    #df_1['city'] = row['city']
# #     for zip in row['zips'].split(" "):
# #         df_1['zip'] = zip
# #         df_2 = df_1.copy()
# #         final.append(df_2)
# #
# # address_df = pd.DataFrame(final)
# #
# # address_df.to_csv('us_details.csv')
# #
# #
#
#
#
# # from py2neo import Graph
# # from py2neo.data import Node
# # from util import load_config_file
# #
# # details = load_config_file('config/config.json')
# # details = details['neo4j_details']
# # db = Graph(uri=details['url'], user=details['user'], password=details['password'], secure=True)
# #
# # data = {"name" : "shashwath"}
# # node = Node('address_details',**data)
# #
# # db.create(node)
#
#
#
# print()