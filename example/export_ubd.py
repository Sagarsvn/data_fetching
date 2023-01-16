from urllib.parse import quote_plus
from typing import Union, Any, Mapping
import pandas
import pymongo
from pandas import DataFrame
from pymongo.database import Database
import re

from config.config import Config, ubd_start_month, ubd_end_month, ubd_path
from config.constant import UBD_RENAME
from utils.s3_service import S3Service

DB_URI = '''mongodb://{user}:{password}@localhost:27018/conviva?authSource=conviva&directConnection=true'''.format(
    user=Config().mongodb_user,
    password=quote_plus(Config().mongodb_password)
)
cls = S3Service.from_connection()


def create_connection() -> Database[Union[Mapping[str, Any], Any]]:
    """Initialize mongodb connection
    :return: connection pool
    """
    client = pymongo.MongoClient(DB_URI)
    db_conn = client["conviva"]
    return db_conn


def get_registered_user_ubd():
    """
     :return registered_user ubd
    """
    try:
        conn = create_connection()
        db_conn = conn["prod-conviva-data"]
        tmp = []
        for rec in db_conn.find(
                {
                    "Viewerid": {"$ne": None},
                    "ContentId": {"$nin": [None, "N/A", "", "NA"]},
                    "IsLogin": {"$nin": [None, "N/A", "", "NA"], "$in": ["yes", "true", "login"]},
                    "PlayingTime": {"$nin": [None, "N/A", "", "NA"]},
                    "StartTime": {"$nin": [None, "N/A", "", "NA"]},
                    "ContentType": {
                        "$in": [re.compile("episode"), re.compile("clip"), re.compile("extra")]},
                    "DataDate": {
                        "$lte": ubd_end_month,
                        "$gte": ubd_start_month,
                    }
                },
                {"Viewerid": 1, "StartTime": 1, "PlayingTime": 1, "ContentType": 1, 'IsLogin': 1, 'ContentId': 1,
                 '_id': False}
        ):
            print(rec)
            tmp.append(rec)

        df = DataFrame(tmp).rename(UBD_RENAME, axis=1)

        print("save to s3")
        cls.write_df_pkl_to_s3(data=df,
                               object_name=ubd_path + "registered_ubd.pkl")
    except Exception as e:
        print(f"Error while user behaviour data, Exception: {e}")


def get_anonymous_user_ubd():
    """
    :return anonymous_user ubd
    """
    try:
        conn = create_connection()
        db_conn = conn["prod-conviva-data"]
        tmp = []
        for rec in db_conn.find(
                {
                    "Viewerid": {"$ne": None},
                    "ContentId": {"$nin": [None, "N/A", "", "NA"]},
                    "IsLogin": {"$nin": [None, "N/A", "", "NA"], "$in": ["no", "false", "not login"]},
                    "PlayingTime": {"$nin": [None, "N/A", "", "NA"]},
                    "StartTime": {"$nin": [None, "N/A", "", "NA"]},
                    "ContentType": {
                        "$in": [re.compile("episode"), re.compile("clip"), re.compile("extra")]},
                    "DataDate": {
                        "$lte": ubd_end_month,
                        "$gte": ubd_start_month,
                    }
                },
                {"Viewerid": 1, "StartTime": 1, "PlayingTime": 1, "ContentType": 1, 'IsLogin': 1, 'ContentId': 1,
                 '_id': False}
        ):
            print(rec)
            tmp.append(rec)

        df = DataFrame(tmp).rename(UBD_RENAME, axis=1)

        print("save to s3")
        cls.write_df_pkl_to_s3(data=df,
                               object_name=ubd_path + "anonymous_ubd.pkl")

    except Exception as e:
        print(f"Error while user behaviour data, Exception: {e}")


def all_ubd_record():
    # get registered user
    print("exporting  user behaviour data for registered user".center(100, "*"))
    get_registered_user_ubd()
    # get anonymous user
    print("exporting  user behaviour data for anonymous user".center(100, "*"))
    get_anonymous_user_ubd()


if __name__ == "__main__":
    all_ubd_record()
