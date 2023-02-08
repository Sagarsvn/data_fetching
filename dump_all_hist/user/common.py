from pandas import DataFrame

from config.constant_an import CUSTOMER_ID, VIEW_FREQUENCY, UBD_GROUP_BY


def get_view_counts(
        ubd: DataFrame
) -> DataFrame:
    """
    Calculate view history attribute
    :param ubd: dataframe object pandas
    :param key: paytv status of content
    :return: dataframe object pandas
    """
    ubd = ubd.groupby(by=UBD_GROUP_BY).size().reset_index()
    return ubd.rename(columns={0: VIEW_FREQUENCY})


def get_duration(
        ubd: DataFrame,
) -> DataFrame:
    """
    Calculate view history attribute
    :param ubd: dataframe object pandas
    :param key: paytv status of content
    :return: dataframe object pandas
    """

    ubd_duration = ubd.groupby(UBD_GROUP_BY)['watch_duration'].sum().reset_index()
    return ubd_duration


def get_created_on(
        ubd: DataFrame,
) -> DataFrame:
    """
           Calculate created_on  attribute
           :param ubd: dataframe object panda
           :return: dataframe object pandas
           """

    ubd_created_on = ubd.groupby(UBD_GROUP_BY

                                 )['created_on'].first().reset_index()

    return ubd_created_on
