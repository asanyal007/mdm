import pandas as pd


def read_sheet_df(path, sheet_name):
    """ This method helps reading execl data with sheet name specific
    Args:
        path: file path
        sheet_name: excel sheet name
    returns:
        dataframe
    """
    return pd.read_excel(path, sheet_name=sheet_name, engine='openpyxl')


def sheet_merging(excel_detail):
    """
    This method will read the list of excel sheet and merge them on right join, also drop the NaN on subset base.
    Args:
        excel_detail: excel_details dict values

    return: joined dataframe
    """
    sheets_df_dict = {}

    for name in excel_detail['sheet_names']:
        sheets_df_dict[name] = read_sheet_df(path=excel_detail['path'], sheet_name=name)

    res = pd.merge(*sheets_df_dict.values(), how='right')

    return res
