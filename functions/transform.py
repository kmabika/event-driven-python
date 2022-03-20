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
        df = data
        df = df.fillna(0)
        sa_data = df.loc[df['Country/Region'] == 'South Africa']
        new_data = sa_data[['Date', 'Country/Region', 'Recovered']]
        sorted_data = new_data.sort_index(ascending=False)
        sorted_data['datetime'] = pd.to_datetime(sorted_data['Date'], format='%Y-%m-%d')
        sorted_data['Recovered'] = pd.to_numeric(sorted_data['Recovered'], downcast='integer')
        jhp_data = sorted_data[['datetime', 'Recovered']]
    except (Exception) as err:
        notify.alert("Error in Transforming jhp data")
        logger.error('Error in the transforming dsfi process', err)
    logger.info('jhp transformation completed completed')
    return jhp_data


def transform_dsfi_data(data) -> pd.DataFrame:
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
        sorted_data['datetime'] = pd.to_datetime(sorted_data['date'], format='%d-%m-%Y')
        sorted_data['total'] = pd.to_numeric(sorted_data['total'], downcast='integer')
        dsfi_data = sorted_data[['datetime', 'total']]
    except (Exception) as err:
        notify.alert("Error in Transforming dsfi data")
        logger.error('Error in the transforming dsfi process', err)
    logger.info('dsfi transformation completed completed')
    return dsfi_data