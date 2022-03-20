import pandas as pd

jhp_data_url = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv?opt_id=oeu1600111930493r0.051025759046968044'
nyt_data_url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'

def load_csv(nyt_url=nyt_data_url,jhp_url=jhp_data_url) -> pd.DataFrame:
    """loads data from csv into a pandas dataframe

    Args:
        url (string): url fata from repository

    Returns:
        pd.DataFrame: returns the dataframe
    """
    nyt_data = pd.read_csv(nyt_url, index_col=None)
    jhp_data = pd.read_csv(jhp_url,index_col=None)
    return {
        'nyt_data': nyt_data,
        'jhp_data': jhp_data
    }
