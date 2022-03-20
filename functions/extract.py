import pandas as pd
import boto3

from transform import transform_nyt_data
from transform import transform_jhp_data
from transform import transofrm_join_data

s3 = boto3.resource('s3')

jhp_data_url = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv?opt_id=oeu1600111930493r0.051025759046968044'
nyt_data_url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'

def load_csv(url) -> pd.DataFrame:
    """loads data from csv into a pandas dataframe

    Args:
        url (string): url fata from repository

    Returns:
        pd.DataFrame: returns the dataframe
    """
    df = pd.read_csv(url, index_col=None)
    return df

def extraction():
    """transform data and join data to form one dataset
    and save it to an S3 bucket
    """
    nyt_data_extraction = load_csv(nyt_data_url)
    jhp_data_extraction = load_csv(jhp_data_url)

    nyt_data = transform_nyt_data(nyt_data_extraction)
    jhp_data = transform_jhp_data(jhp_data_extraction)

    transformed_data = transofrm_join_data(nyt_data,jhp_data)

    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print(transformed_data)

    return "DONE"

extraction()
