import pandas as pd
import boto3

from transform import transform_dsfi_data
from transform import transform_jhp_data

s3 = boto3.resource('s3')

dsfis_data_url = 'https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_provincial_cumulative_timeline_confirmed.csv'
jhp_data_url = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv?opt_id=oeu1600111930493r0.051025759046968044'

def load_csv(url) -> pd.DataFrame:
    """loads data from csv into a pandas dataframe

    Args:
        url (string): url fata from repository

    Returns:
        pd.DataFrame: returns the dataframe
    """
    df = pd.read_csv(url,index_col=None)
    return df


