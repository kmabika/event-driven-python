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
        pd.DataFrame: sorted john hopkins data
    """
    try:
        logger.info('Starting nyt transformation')

        df = data
        df = df.fillna(0)
        sa_data = df.loc[df['Country/Region'] == 'US']
        new_data = sa_data[['Date', 'Country/Region', 'Recovered']]
        sorted_data = new_data.sort_index(ascending=False)
        sorted_data['datetime'] = pd.to_datetime(sorted_data['Date'], format='%Y-%m-%d')
        sorted_data['recovered'] = pd.to_numeric(sorted_data['Recovered'], downcast='integer')
        jhp_data = sorted_data[['datetime', 'recovered']]
    except (Exception) as err:
        notify.alert("Error in Transforming jhp data")
        logger.error('Error in the transforming jhp process', err)
    logger.info('jhp transformation completed completed')
    return jhp_data


def transform_nyt_data(data) -> pd.DataFrame:
    """Converts & sorts data

    Args:
        data (pd.Dataframe): dsfi csv data

    Returns:
        pd.DataFrame: sorted data dsfi data
    """
    try:
        logger.info('Starting dsfi transformation')

        df = data
        df = df.fillna(0)
        sorted_data = df.sort_index(ascending=False)
        sorted_data['datetime'] = pd.to_datetime(sorted_data['date'], format='%Y-%m-%d')
        sorted_data['deaths'] = pd.to_numeric(sorted_data['deaths'], downcast='integer')
        sorted_data['cases'] = pd.to_numeric(sorted_data['cases'], downcast='integer')
        nyt_data = sorted_data[['datetime', 'deaths','cases']]
    except (Exception) as err:
        # notify.alert("Error in Transforming dsfi data")
        logger.error('Error in the transforming dsfi process', err)
    logger.info('dsfi transformation completed completed')
    return nyt_data

def transofrm_join_data(nyt_data,jhp_data) -> pd.DataFrame:
    """_summary_

    Args:
        nyt_data (_type_): _description_
        jhp_data (_type_): _description_

    Returns:
        pd.DataFrame: _description_
    """
    nyt_data = nyt_data
    jhp_data = jhp_data
    covid_df = pd.merge(nyt_data, jhp_data, on='datetime', how='inner')
    return covid_df