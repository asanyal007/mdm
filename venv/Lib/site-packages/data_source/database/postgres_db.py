from data_source.database_interface import DatabaseInterface
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.types import Text


class PostgresDb(DatabaseInterface):

    def __init__(self, dbinfo):
        super().__init__(dbinfo)

    def get_data(self, query_str):
        self.query_str = query_str
        conn = psycopg2.connect(
            "dbname={dbname} user={user} host={host} port={port} password={password}".format(**self.dbinfo)
        )
        data = pd.read_sql(self.query_str, conn)
        conn.close()
        return data

    def put_data(self, df, table_name, load_type="fail"):
        db_path = "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**self.dbinfo)
        con = create_engine(db_path)
        df.to_sql(name=table_name, con=con, if_exists=load_type, index=False, method='multi',
                  dtype={"fax": Text(), 'credit_amt': Text(), 'phone': Text() })
        #con.dispose()

    def run_query(self, query):
        conn = psycopg2.connect(
            "dbname={dbname} user={user} host={host} port={port} password={password}".format(**self.dbinfo)
        )
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

