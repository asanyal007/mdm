class DataSourceInterface(object):

    def __init__(self):
        pass

    def get_data(self, query_str):
        pass

    def __repr__(self):
        my_repr = ""
        key_val_str = "\n{}\n------------\n{}\n"
        for key, val in self.__dict__.items():
            my_repr += key_val_str.format(key, val)
        return my_repr