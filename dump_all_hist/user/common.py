from pandas import DataFrame

from config.constant_an import CUSTOMER_ID, VIEW_FREQUENCY, UBD_GROUP_BY


def get_view_counts(
        ubd: DataFrame,
        group_by,
        on,
) -> DataFrame:
    """
    Calculate view history attribute
    :param ubd: dataframe object pandas
    :param key: paytv status of content
    :return: dataframe object pandas
    """
    ubd = ubd.groupby(by=group_by).size().reset_index()
    return ubd.rename(columns={0: on})


def get_duration(
        ubd: DataFrame,
        group_by,
        on,
) -> DataFrame:
    """
    Calculate view history attribute
    :param ubd: dataframe object pandas
    :param key: paytv status of content
    :return: dataframe object pandas
    """

    ubd_duration = ubd.groupby(group_by
                               )[on].sum().reset_index()
    return ubd_duration


def get_created_on(
        ubd: DataFrame,
        group_by,
        on,
) -> DataFrame:
    """
           Calculate created_on  attribute
           :param ubd: dataframe object panda
           :return: dataframe object pandas
           """

    ubd_created_on = ubd.groupby(group_by
                                 )[on].first().reset_index()

    return ubd_created_on


def get_groupby_implict_rating(
        ubd: DataFrame,
        groupby,
        on ,
) -> DataFrame:
    """Calculate created_on  attribute
    :param ubd: dataframe object panda
    :return: dataframe object pandas
    """

    ubd_program_rating = ubd.groupby(groupby
                                     )[on].mean().round(1).astype(object).reset_index()

    return ubd_program_rating


