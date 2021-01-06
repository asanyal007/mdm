from flask import render_template, Flask, request, jsonify, redirect, url_for
from flask_cors import cross_origin
from data_source.database.postgres_db import PostgresDb
from data_source.database.cypher_db import CypherDb
from util import load_config_file, unique_id
from sqlalchemy import exc
import pandas as pd
import xmltodict
import base64
import json

config_details = load_config_file('config/config.json')
app = Flask(__name__)


@app.route('/graph-view')
def graph_view():
    return render_template('index.html')


@app.route('/api/login', methods=['POST'])
@cross_origin()
def log_in():
    """
    Get the State city and zip details for US.

    Request: Json data keys are email and password
    Returns:
        log in authorization
    error:
        400, 401
    """
    data = request.json
    if 'email' not in data:
        response = jsonify({"msg": "email not found"})
        response.status_code = 400
        return response
    if 'password' not in data:
        response = jsonify({"msg": "password not found"})
        response.status_code = 400
        return response
    table_name = 'account'
    query = "SELECT * FROM \"{table_name}\" WHERE email=\'{email}\' AND password=\'{password}\';".format(
        table_name=table_name,
        email=data['email'],
        password=data[
            'password']
    )
    db = PostgresDb(config_details['postgres_db'])
    res = db.get_data(query)
    if len(res) > 0:
        return jsonify({"msg": "login Successfully", "username": str(res['username'].values[0])})
    else:
        response = jsonify({"msg": "login not Successfully"})
        response.status_code = 401
        return response


@app.route('/api/get-location-details', methods=['POST'])
@cross_origin()
def get_location_details():
    """
    Get the State city and zip details for US.

    Request: None
    Returns:
        complete data

    """
    table_name = config_details['postgres_tables']['location_details']
    query = "SELECT * FROM \"{table_name}\";".format(table_name=table_name)
    db = PostgresDb(config_details['postgres_db'])
    res = db.get_data(query)
    res.drop_duplicates(inplace=True)
    result = []
    for key, value in res.groupby(['state_id', 'state_name', 'city']):
        data = dict()
        data['state_id'] = value['state_id'].values[0]
        data['city'] = value['city'].values[0]
        data['state_name'] = value['state_name'].values[0]
        data['zips'] = list(value['zip'].values)
        data_copy = data.copy()
        result.append(data_copy)
    return jsonify(result)


@app.route('/api/get-data-unresolved', methods=['POST'])
@cross_origin()
def get_data_unresolved():
    """
    Get the data as per the state, city and zip filters which are unresolved

    request: dictionary
        ex:
            {
                "state": "NJ",
                "city": "BERLIN",
                "zip":"8009"
            }

    Returns:
        filter data set
    """
    data = request.json
    list_query = []
    for key, value in data.items():
        list_query.append(f'{key}=\'{value}\'')
    data['where_str'] = ' AND '.join(list_query)
    data['table_name'] = config_details['postgres_tables']['unresolved_cluster_table']
    query = "SELECT * FROM \"{table_name}\" Where {where_str} AND status='unresolved';".format(
        **data)
    db = PostgresDb(config_details['postgres_db'])
    res = db.get_data(query)
    return jsonify(res.to_dict(orient='records'))


@app.route('/api/get-data-resolved', methods=['POST'])
@cross_origin()
def get_data_resolved():
    """
    Get the data as per the state, city and zip filters which resolved

    request: dictionary
        ex:
            {
                "state": "NJ",
                "city": "BERLIN",
                "zip":"8009"
            }

    Returns:
        filter data set
    """
    data = request.json
    list_query = []
    for key, value in data.items():
        list_query.append(f'{key}=\'{value}\'')
    data['where_str'] = ' AND '.join(list_query)
    data['table_name'] = config_details['postgres_tables']['mdm_table']
    query = "SELECT * FROM \"{table_name}\" Where {where_str};".format(
        **data)
    db = PostgresDb(config_details['postgres_db'])
    res = db.get_data(query)
    return jsonify(res.to_dict(orient='records'))


@app.route('/api/update-data', methods=['POST'])
@cross_origin()
def update_date():
    """
    update database table as per the user selection

    Request: List of dictionary
        ex:
            [
              {
                "addr_id": 86488,
                "uid": "6741dac9-a674-4e6c-8889-8834c3f3b700",
              },
              ------------------------------
              ------------------------------
              {
                "addr_id": 43719,
                "uid": "c00ce488-2629-47f0-8327-0022d5fc39ff",
              }
            ]

    Returns:
        {"msg": "successfully updated"}
    """
    from py2neo import Node
    data = request.json
    sql_db = PostgresDb(config_details['postgres_db'])
    gdb = CypherDb(config_details['neo4j_details'])
    for value in data:
        selected_mdm = sql_db.get_data(
            "SELECT * FROM \"{table_name}\" WHERE uid = \'{uid}\' AND addr_id = \'{addr_id}\';".format(
                table_name=config_details['postgres_tables']['unresolved_cluster_table'], uid=value['uid'],
                addr_id=value['addr_id']))
        selected_mdm['status'] = 'user resolved'
        sql_db.put_data(selected_mdm, config_details['postgres_tables']['mdm_table'], load_type='append')
        # master_pre_address table update code
        master_pre = selected_mdm[['addr_id', 'addr_line_1', 'addr_line_2', 'city', 'state', 'zip']].head()
        try:
            sql_db.put_data(master_pre, config_details['postgres_tables']['master_pre_table'], load_type='append')
        except exc.IntegrityError:
            pass
        # End here
        query = "DELETE FROM \"{table_name}\" WHERE uid = \'{value}\';".format(
            table_name=config_details['postgres_tables']['unresolved_cluster_table'], value=value['uid'],
            addr_id=value['addr_id'])
        sql_db.run_query(query)
        node = str(Node('address_details', **{'uid': value['uid']})).replace("(:", "(n:")
        g_query = "MATCH p={node} where n.addr_id <> {addr_id} DETACH Delete p".format(node=node,
                                                                                       addr_id=value['addr_id'])
        update_gquery = "MATCH p={node} where n.addr_id = {addr_id} SET n.status =\'User resolved\'".format(node=node,
                                                                                                            addr_id=
                                                                                                            value[
                                                                                                                'addr_id'])
        gdb_conn = gdb.connection()
        gdb_conn.run(g_query)
        gdb_conn.run(update_gquery)

    return jsonify({"msg": "successfully updated"})


@app.route('/api/get-userId', methods=['POST'])
@cross_origin()
def get_user_list():
    """
    Get the user all pres_id
    request: dictionary
        ex:
            {"field": "pres_id"}
    Returns:
        User data set as a list
    """
    data = {}
    data = request.json
    if data['field'] in ["presc_id"]:
        data['table_name'] = config_details['postgres_tables']['processed_table']
        query = "SELECT DISTINCT(\"{field}\") FROM \"{table_name}\" ;".format(**data)
        try:
            db = PostgresDb(config_details['postgres_db'])
            results = db.get_data(query)
            output = [row for row in results.get(data['field'])]
            output.sort()
            return jsonify({str(data['field']): output})
        except:
            return jsonify({"msg": "Check Database Connection"})
    return jsonify({"msg": "Invalid field"})


@app.route('/api/get-user-locations', methods=['POST'])
@cross_origin()
def get_user_location_details():
    """
    Get the user data state_name\state_id\city\zip --> {key}
    request: dictionary
        ex:
            {"field": "Key"}
    Returns:
        specific location deatail as a list
    """
    data = {}
    data = request.json
    if data['field'].lower() in ["zip", "state_name", "state_id", "city"]:
        data['table_name'] = config_details['postgres_tables']['location_details']
        query = "SELECT DISTINCT(\"{field}\") FROM \"{table_name}\" ;".format(**data)
        try:
            db = PostgresDb(config_details['postgres_db'])
            results = db.get_data(query)
            output = [row for row in results.get(data['field'])]
            return jsonify({str(data['field']): output})
        except:
            return jsonify({"msg": "Check Database Connection"})
    return jsonify({"msg": "Invalid field"})


@app.route('/api/get-state-id-byName', methods=['POST'])
@cross_origin()
def get_state_id_by_name():
    """
    Get the user  state_id --> {key}
    request: dictionary
        ex:
            {"field": "Key"}
    Returns:
        dictionary :  state_name as key and state_id value
    """
    data = {}
    data = request.json
    if data['field'].lower() in ["state_id"]:
        data['table_name'] = config_details['postgres_tables']['location_details']
        query = "SELECT Distinct(state_name) ,state_id FROM \"{table_name}\";".format(**data)
        try:
            db = PostgresDb(config_details['postgres_db'])
            results = db.get_data(query)
            output = results.to_dict(orient='records')
            output = {record['state_name']: record['state_id'] for record in output}
            return jsonify(output)
        except:
            return jsonify({"msg": "Check Database Connection"})
    return jsonify({"msg": "Invalid field"})


@app.route('/api/advance-search', methods=['POST'])
@cross_origin()
def advance_query_search():
    """
    Get the data as per the pres_id , state, city and zip advance search
    request: dictionary
        ex:
            {'query':"state:'CA' AND presc_id:'1095' OR city:'NEWPORT'"}
    Returns:
        filter data set as list of record
    """
    data = request.json
    text = data['query']
    for ch in ["':", "' :", '":', '" :', ":"]:
        text = text.replace(ch, "=")
    for ch in ['"']:
        text = text.replace(ch, "'")
    data['where_stm'] = text
    try:
        data['table_name'] = config_details['postgres_tables']['unresolved_cluster_table']
        query = "SELECT * FROM \"{table_name}\" where {where_stm};".format(**data)
        db = PostgresDb(config_details['postgres_db'])
        results = db.get_data(query)
        output = results.to_dict(orient='records')
        return jsonify({'result': output})
    except:
        return jsonify({"msg": "Check Database Connection"})
    return jsonify({"msg": "Invalid field"})


@app.route('/api/advance-search-resolved', methods=['POST'])
@cross_origin()
def advance_resolved_query_search():
    """
    Get the data as per the pres_id , state, city and zip advance search
    request: dictionary
        ex:
            {'query':"state:'CA' AND presc_id:'1095' OR city:'NEWPORT'"}
    Returns:
        filter data set as list of record
    """
    data = request.json
    text = data['query']
    for ch in ["':", "' :", '":', '" :', ":"]:
        text = text.replace(ch, "=")
    for ch in ['"']:
        text = text.replace(ch, "'")
    data['where_stm'] = text
    try:
        data['table_name'] = config_details['postgres_tables']['mdm_table']
        query = "SELECT * FROM \"{table_name}\" where {where_stm};".format(**data)
        db = PostgresDb(config_details['postgres_db'])
        results = db.get_data(query)
        output = results.to_dict(orient='records')
        return jsonify({'result': output})
    except:
        return jsonify({"msg": "Check Database Connection"})
    return jsonify({"msg": "Invalid field"})


@app.route('/api/saml-login', methods=['POST'])
@cross_origin()
def saml_log_in():
    """
    Get SAML login authentication

    Request: Json data keys are SAMLResponse
    Returns:
        log in authorization
    error:
        400, 401
    """
    if len(request.form['SAMLResponse']) > 0:
        base64_bytes = str(request.form['SAMLResponse']).encode('ascii')
        message_bytes = base64.b64decode(base64_bytes)
        message = message_bytes.decode('ascii')
        m_dict = json.loads(json.dumps(xmltodict.parse(message)))
        email = m_dict['samlp:Response']['Assertion']['AttributeStatement']['Attribute']['AttributeValue'].lower()
        table_name = 'account'
        query = "SELECT * FROM \"{table_name}\" WHERE email=\'{email}\';".format(
            table_name=table_name,
            email=email
        )
        db = PostgresDb(config_details['postgres_db'])
        sql_db = PostgresDb(config_details['postgres_db'])
        res = db.get_data(query)
        if len(res) > 0:
            query = "UPDATE \"{table_name}\"  SET flage = \'1\' WHERE email = \'{email}\';".format(
                table_name=table_name,
                email=email)
            sql_db.run_query(query)
            return redirect("https://mdmfe.wnsagilius.com/authenticate")
        else:
            return redirect("https://mdmfe.wnsagilius.com/authenticate")
    else:
        response = jsonify({"msg": "SAMLResponse data is not found"})
        response.status_code = 400
        return response


@app.route('/api/patch-login', methods=['POST'])
@cross_origin()
def patch_log_in():
    table_name = 'account'
    query = "SELECT * FROM \"{table_name}\" WHERE flage = \'1\';".format(
        table_name=table_name
    )
    db = PostgresDb(config_details['postgres_db'])
    res = db.get_data(query)
    if len(res) > 0:
        sql_db = PostgresDb(config_details['postgres_db'])
        query = "UPDATE \"{table_name}\"  SET flage = \'0\';".format(
            table_name=table_name)
        sql_db.run_query(query)
        return jsonify({"msg": "login Successfully", "username": str(res['username'].values[0])})
    else:
        response = jsonify({"msg": "login not Successfully"})
        response.status_code = 200
        return response


@app.route('/api/create-cluster', methods=['POST'])
def new_cluster():
    from mdm_ranking_generation import df_text_transformation
    from mdm_ranking_generation import df_text_ranking
    from mdm_ranking_generation import auto_resolve

    data = request.json
    cut_of_scores = int(config_details['cut_of_scores'])
    data = pd.DataFrame(data).to_dict(orient='list')
    data['presc_id'] = 'presc_id = {}'.format(set(data['presc_id']).pop())
    data['addr_id'] = 'addr_id IN {}'.format((*data['addr_id'],)) if len(data['addr_id']) > 1 \
        else 'addr_id = {}'.format(data['addr_id'][0])
    data['uid'] = 'uid = \'{}\''.format(set(data['uid']).pop())

    db = PostgresDb(config_details['postgres_db'])
    query = "SELECT * FROM \"{table_name}\" WHERE {clause};".format(table_name=config_details['postgres_tables']
    ['unresolved_cluster_table'],
                                                                    clause=' AND '.join(list(data.values())))
    res = db.get_data(query)
    for name in [config_details['postgres_tables']['processed_table'],
                 config_details['postgres_tables']['unresolved_cluster_table']]:
        clause = data.copy()
        clause['table_name'] = name
        delete_rows(clause)

    if len(res) > 1:
        res = df_text_transformation(res)
        res['label'] = ''  # mdm_transformation function drop the columns 'label'
        res = df_text_ranking(res,)
    else:
        res = df_text_ranking(res, default=True)
    if 'house_no' in res.columns:
        res.drop(columns='house_no', inplace=True)
    res = auto_resolve(res, cut_of_scores)
    unresolved_mdm_df = res[res['status'] == 'unresolved']
    resolved_mdm_df = res[res['status'] == 'system resolved']
    resolved_mdm_df.reset_index(drop=True, inplace=True)
    unresolved_mdm_df.reset_index(drop=True, inplace=True)

    db.put_data(resolved_mdm_df, config_details['postgres_tables']['mdm_table'], load_type='append')
    db.put_data(unresolved_mdm_df, config_details['postgres_tables']['unresolved_cluster_table'], load_type='append')

    res.drop(columns='status', inplace=True)

    db.put_data(res, config_details['postgres_tables']['processed_table'], load_type='append')

    return jsonify({"msg": "successfully New cluster created"})


@app.route('/api/move-cluster', methods=['POST'])
def move_cluster():
    from mdm_ranking_generation import df_text_transformation
    from mdm_ranking_generation import df_text_ranking
    from mdm_ranking_generation import auto_resolve

    data = request.json
    cut_of_scores = int(config_details['cut_of_scores'])
    db = PostgresDb(config_details['postgres_db'])

    dst_uid = data['dst_uid']
    select_data = pd.DataFrame(data['selected_data']).to_dict(orient='list')
    src_uid = select_data['uid'][0]
    if str(dst_uid) == str(src_uid):
        return jsonify({"msg": "source and destination UID same"})
    else:
        if "master_data" not in data:
            query = 'SELECT * FROM ' \
                    '(SELECT * FROM \"{table_name}\" ' \
                    'WHERE uid = \'{dst_uid}\' UNION ALL ' \
                    'SELECT * FROM \"{table_name}\" ' \
                    'WHERE addr_id IN {addr_id} AND uid = \'{src_uid}\') alis'.format(
                table_name=config_details['postgres_tables']['unresolved_cluster_table'],
                dst_uid=dst_uid,
                addr_id=(*select_data['addr_id'],) if len(select_data['addr_id']) > 1 else '({})'.format(
                    select_data['addr_id'][0]),
                src_uid=src_uid)
            res = db.get_data(query)
            del_data = res[['presc_id', 'addr_id', 'uid']].to_dict(orient='list')
            del_data['presc_id'] = 'presc_id = {}'.format(set(del_data['presc_id']).pop())
            del_data['addr_id'] = 'addr_id IN {}'.format((*del_data['addr_id'],)) if len(del_data['addr_id']) > 1 \
                else 'addr_id = {}'.format(del_data['addr_id'][0])
            del_data['uid'] = 'uid IN {}'.format((*del_data['uid'],)) if len(
                del_data['uid']) > 1 else 'uid = \'{}\''.format(
                list(set(del_data['uid']))[0])
            for name in [config_details['postgres_tables']['processed_table'],
                         config_details['postgres_tables']['unresolved_cluster_table']]:
                clause = del_data.copy()
                clause['table_name'] = name
                delete_rows(clause)
        else:
            col_list = "\"addr_id\", \"addr_line_1\", \"addr_line_2\", \"city\", \"presc_id\", " \
                       "\"rank\", \"scores\", \"state\", \"uid\", \"weight\", \"zip\""
            query = 'SELECT * FROM ' \
                    '(SELECT {col_list} FROM \"{table_name_1}\" ' \
                    'WHERE uid = \'{dst_uid}\' UNION ALL ' \
                    'SELECT {col_list} FROM \"{table_name_2}\" ' \
                    'WHERE addr_id IN {addr_id} AND uid = \'{src_uid}\') alis'.format(
                        table_name_1=config_details['postgres_tables']['processed_table'],
                        table_name_2=config_details['postgres_tables']['unresolved_cluster_table'],
                        dst_uid=dst_uid,
                        addr_id=(*select_data['addr_id'],) if len(select_data['addr_id']) > 1 else '({})'.format(
                            select_data['addr_id'][0]),
                        src_uid=src_uid,
                        col_list=col_list)
            res = db.get_data(query)
            res['status'] = 'unresolved'
            del_data = res[['presc_id', 'addr_id', 'uid']].to_dict(orient='list')
            del_data['presc_id'] = 'presc_id = {}'.format(set(del_data['presc_id']).pop())
            del_data['addr_id'] = 'addr_id IN {}'.format((*del_data['addr_id'],)) if len(del_data['addr_id']) > 1 \
                else 'addr_id = {}'.format(del_data['addr_id'][0])
            del_data['uid'] = 'uid IN {}'.format((*del_data['uid'],)) if len(
                del_data['uid']) > 1 else 'uid = \'{}\''.format(
                list(set(del_data['uid']))[0])
            for name in [config_details['postgres_tables']['mdm_table'],
                         config_details['postgres_tables']['unresolved_cluster_table'],
                         config_details['postgres_tables']['processed_table']]:
                clause = del_data.copy()
                clause['table_name'] = name
                delete_rows(clause)
        res['uid'] = dst_uid
        if len(res) > 1:
            res = df_text_transformation(res)
            res['label'] = ''  # mdm_transformation function drop the columns 'label'
            res = df_text_ranking(res, gen_uid=False)
        else:
            res = df_text_ranking(res, default=True, gen_uid=False)
        if 'house_no' in res.columns:
            res.drop(columns='house_no', inplace=True)

        if len(res) > 0:
            db.put_data(res.drop(columns='status'), config_details['postgres_tables']['processed_table'], load_type='append')

        res = auto_resolve(res, cut_of_scores)
        if 'unresolved' in res['status'].tolist():
            unresolved_mdm_df = res[res['status'] == 'unresolved']
            unresolved_mdm_df.reset_index(drop=True, inplace=True)
            db.put_data(unresolved_mdm_df, config_details['postgres_tables']['unresolved_cluster_table'],
                        load_type='append')
        if 'system resolved' in res['status'].tolist():
            resolved_mdm_df = res[res['status'] == 'system resolved']
            resolved_mdm_df.reset_index(drop=True, inplace=True)
            db.put_data(resolved_mdm_df, config_details['postgres_tables']['mdm_table'], load_type='append')

        return jsonify({"msg": "successfully merged the cluster"})


@app.route('/api/get-uid-list', methods=['POST'])
def get_presc_uid_list():
    data = request.json
    db = PostgresDb(config_details['postgres_db'])
    table = config_details['postgres_tables']['processed_table']
    query = 'SELECT Distinct(uid) FROM \"{table}\" WHERE presc_id = {presc_id}'.format(table=table, **data)
    res = db.get_data(query)
    return jsonify(res.to_dict(orient='list'))


@app.route('/api/get-cluster-df', methods=['POST'])
def get_cluster_df():
    data = request.json
    db = PostgresDb(config_details['postgres_db'])
    table1 = config_details['postgres_tables']['unresolved_cluster_table']
    table2 = config_details['postgres_tables']['mdm_table']
    query = 'select * from \
            (select * from {table1} union all \
            select * from {table2}) alis where uid = \'{uid}\';'.format(table1=table1, table2=table2, **data)
    res = db.get_data(query)
    res = res.sort_values('rank', ascending=False)
    res = res.head(1)
    return jsonify(res.to_dict(orient='records'))


def delete_rows(clause):
    sql_db = PostgresDb(config_details['postgres_db'])
    query = 'delete from \"{table_name}\" WHERE {clause};'.format(table_name=clause['table_name'],
                                                                  clause=' AND '.join(list(clause.values())[0:3]))
    sql_db.run_query(query)


if __name__ == '__main__':
    app.run(port=8080, debug=True)
