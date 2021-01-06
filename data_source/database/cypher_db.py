from data_source.database_interface import DatabaseInterface
from py2neo import Graph


class CypherDb(DatabaseInterface):

    def __init__(self, dbinfo):
        self.parameters = None
        super().__init__(dbinfo)

    def get_data(self, query_str, parameters=None):
        conn = self.connection()
        if self.parameters:
            res = conn.run(query_str, parameters=parameters).to_data_frame()
        else:
            res = conn.run(query_str).to_data_frame()

        return res

    def connection(self):
        return Graph(
            uri=self.dbinfo['url'],
            user=self.dbinfo['user'],
            password=self.dbinfo['password'],
            secure=True
        )
