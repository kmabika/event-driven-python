import pandas as pd
import logging
import alert as notify

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def transform_jhp_data(data) -> pd.DataFrame:
    """Converts and sorts data

    Args:
        data (pd.DataFrame): john hopkins csv data

    Returns:
        jhp_data: sorted john hopkins dataframe
    """
    try:
        df = data
        df = df.fillna(0)
        sa_data = df.loc[df['Country/Region'] == 'US']
        new_data = sa_data[['Date', 'Country/Region', 'Recovered']]
        sorted_data = new_data.sort_index(ascending=False)
        sorted_data['datetime'] = pd.to_datetime(sorted_data['Date'], format='%Y-%m-%d')
        sorted_data['recovered'] = pd.to_numeric(sorted_data['Recovered'], downcast='integer')
        jhp_data = sorted_data[['datetime', 'recovered']]
    except (Exception) as err:
        logger.error('Error in the transforming jhp process', err)
    return jhp_data


def transform_nyt_data(data) -> pd.DataFrame:
    """Converts & sorts data

    Args:
        data (pd.Dataframe): dsfi csv data

    Returns:
        nyt_data: sorted data nyt dataframe
    """
    try:
        df = data
        df = df.fillna(0)
        sorted_data = df.sort_index(ascending=False)
        sorted_data['datetime'] = pd.to_datetime(sorted_data['date'], format='%Y-%m-%d')
        sorted_data['deaths'] = pd.to_numeric(sorted_data['deaths'], downcast='integer')
        sorted_data['cases'] = pd.to_numeric(sorted_data['cases'], downcast='integer')
        nyt_data = sorted_data[['datetime', 'deaths','cases']]
    except (Exception) as err:
        logger.error('Error in the transforming dsfi process', err)
    return nyt_data

def transform(data) -> pd.DataFrame:
    """merge nyt and jhp data

    Args:
        data['nyt_data'] (pd.DataFrame): _description_
        data['jhp_data'] (pd.DataFrame): _description_

    Returns:
        covid_df: combined covid 19 dataframe
    """
    try:
        logger.info('Starting transformation')

        nyt_data = transform_nyt_data(data['nyt_data'])
        jhp_data = transform_jhp_data(data['jhp_data'])
        covid_df = pd.merge(nyt_data, jhp_data, on='datetime', how='inner')
        
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
            print(covid_df)

    except (Exception) as err:
        notify.alert("Error in Transforming jhp data")
        logger.error('Error in the transforming dsfi process', err)
    logger.info('transformation process completed')
    return covid_df
