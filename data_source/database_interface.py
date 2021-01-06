from data_source.data_source_interface import DataSourceInterface


class DatabaseInterface(DataSourceInterface):
    def __init__(self, dbinfo, query_str=None):
        super(DatabaseInterface).__init__()
        self.dbinfo = dbinfo
        self.query_str = query_str
