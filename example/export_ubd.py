from urllib.parse import quote_plus
from typing import Union, Any, Mapping
import pandas
import pymongo
from pymongo.database import Database
import re

DB_URI = "mongodb://aiml:{password}@localhost:27017/conviva?authSource=conviva&directConnection=true".format(
    password=quote_plus("rctiplus2022@@")
)


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
                    "$lte": "2022-12-31",
                    "$gte": "2022-07-01",
                }
            },
            {"Viewerid": 1, "StartTime": 1, "PlayingTime": 1, "ContentType": 1, 'IsLogin': 1, 'ContentId': 1,
             '_id': False}
    ):
        print(rec)
        tmp.append(rec)

    pandas.DataFrame(tmp).to_pickle("registerd.pkl", compression='gzip')


def get_anonymous_user_ubd():
    """
    :return anonymous_user ubd
    """
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
                    "$lte": "2022-12-31",
                    "$gte": "2022-07-01",
                }
            },
            {"Viewerid": 1, "StartTime": 1, "PlayingTime": 1, "ContentType": 1, 'IsLogin': 1, 'ContentId': 1,
             '_id': False}
    ):
        print(rec)
        tmp.append(rec)

    pandas.DataFrame(tmp).to_pickle("anonymous_user.pkl", compression='gzip')


if __name__ == "__main__":
    # get registered user
    get_registered_user_ubd()
    # get anonymous user
    get_anonymous_user_ubd()
