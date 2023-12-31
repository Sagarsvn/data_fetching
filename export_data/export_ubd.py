
from urllib.parse import quote_plus

import pymongo
from pandas import DataFrame, concat
import re

from config.config import Config, registered_ubd_start_month, registered_ubd_end_month, ubd_path, anonymous_ubd_end_date, \
    anonymous_ubd_start_date
from config.constant import UBD_RENAME, UBD_DROP
from utils.logger import Logging
from utils.s3_service import S3Service


DB_URI = '''mongodb://{user}:{password}@localhost:27018/conviva?authSource=conviva&directConnection=true'''.format(
    user=Config().mongodb_user,
    password=quote_plus(Config().mongodb_password)
)
cls = S3Service.from_connection()


def create_connection():
    """Initialize mongodb connection
    :return: connection pool
    """
    client = pymongo.MongoClient(DB_URI)
    db_conn = client["conviva"]
    return db_conn


def yield_rows(cursor, chunk_size):
    """
            Generator to yield chunks from cursor
            :param cursor:
            :param chunk_size:
            :return:
            """
    chunk = []
    for i, row in enumerate(cursor):
        if i % chunk_size == 0 and i > 0:
            yield chunk
        chunk.append(row)
    yield chunk


def genreate_record(cursor, chunk_size):
    """
    call generator function
    """
    chunks = yield_rows(cursor, chunk_size=chunk_size)
    temp = DataFrame()
    c = 1
    for chunk in chunks:
        Logging.info(f"record : {chunk_size * c} ")
        df_temp = DataFrame(chunk)
        temp = concat([temp, df_temp])
        c = c + 1

    return temp


def get_registered_user_ubd(chunk_size):
    """
     :return registered_user ubd
    """
    try:
        db_conn = create_connection()
        db_conn = db_conn["prod-conviva-data"]
        for t1, t2 in zip(registered_ubd_start_month, registered_ubd_end_month):
            cursor = db_conn.find({
                "Viewerid": {"$ne": None},
                "ContentId": {"$nin": [None, "N/A", "", "NA"]},
                "IsLogin": {"$in": ["yes", "true", "login"]},
                "PlayingTime": {"$nin": [None, "N/A", "", "NA"]},
                "StartTime": {"$nin": [None, "N/A", "", "NA"]},
                "ContentType": {
                    "$in": [re.compile("episode"), re.compile("clip"), re.compile("extra")]},
                "PercentageComplete": {"$gte": 25},
                "DataDate": {
                    "$lte": t2,
                    "$gte": t1,
                }
            }, {"Viewerid": 1, "StartTime": 1, "PlayingTime": 1, "ContentType": 1, 'ContentId': 1, '_id': False,},
                batch_size=chunk_size
            )

            # call genreator function
            resp = genreate_record(cursor=cursor, chunk_size=chunk_size)

            # rename column name
            resp = resp.rename(UBD_RENAME, axis=1)

            # reset index
            resp.reset_index(inplace=True, drop=True)

            # drop duplicates based same column
            resp = resp.drop_duplicates(
                subset=UBD_DROP, ignore_index=True
            )
            Logging.info(f"total records in {t1} {len(resp)}")

            # save to s3
            cls.write_df_pkl_to_s3(data=resp,
                                   object_name=
                                   ubd_path + "registered_ubd_{}.pkl"
                                   .format(t1.replace("-", ""))
                                   )
    except Exception as e:
        Logging.info(f"Error while user behaviour data, Exception: {e}")


def get_anonymous_user_ubd(chunk_size):
    """
    :return anonymous_user ubd
    """
    try:
        conn = create_connection()
        db_conn = conn["prod-conviva-data"]
        cursor = db_conn.find(
            {
                "Viewerid": {"$ne": None},
                "ContentId": {"$nin": [None, "N/A", "", "NA"]},
                "IsLogin": {"$in": ["no", "false", "not login"]},
                "PlayingTime": {"$nin": [None, "N/A", "", "NA"]},
                "StartTime": {"$nin": [None, "N/A", "", "NA"]},
                "ContentType": {
                    "$in": [re.compile("episode"), re.compile("clip"), re.compile("extra")]},
                "PercentageComplete": {"$gte": 25},
                "DataDate": {
                    "$lte":anonymous_ubd_end_date,
                    "$gte": anonymous_ubd_start_date,
                }
            },
            {"Viewerid": 1, "StartTime": 1, "PlayingTime": 1, "ContentType": 1, 'ContentId': 1, '_id': False},
            batch_size=chunk_size)

        resp = genreate_record(cursor=cursor, chunk_size=chunk_size)
        # rename cloumn
        resp = resp.rename(UBD_RENAME, axis=1)

        # reset index
        resp.reset_index(inplace=True, drop=True)

        # drop duplicates based same column
        resp = resp.drop_duplicates(
            subset=UBD_DROP, ignore_index=True
        )

        Logging.info(f"total records in {len(resp)}")

        cls.write_df_pkl_to_s3(data=resp,
                               object_name=
                               ubd_path + "anonymous_ubd_{}.pkl"
                               .format(anonymous_ubd_start_date.replace("-", ""))
                               )
    except Exception as e:
        Logging.info(f"Error while user behaviour data, Exception: {e}")


def all_ubd_record():
    # get registered user
    Logging.info("exporting  user behaviour data for registered user".center(100, "*"))
    get_registered_user_ubd(chunk_size=50000)

    # get anonymous user
    Logging.info("exporting  user behaviour data for anonymous user".center(100, "*"))
    get_anonymous_user_ubd(chunk_size=50000)
