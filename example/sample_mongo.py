from urllib.parse import quote_plus
from typing import Union, Any, Mapping
import pandas
import pymongo
from pymongo.database import Database

from config.config import Config


DB_URI = '''mongodb://{user}:{password}@localhost:27018/conviva?authSource=conviva&directConnection=true'''.format(user =Config().mongodb_user,
    password=quote_plus(Config().mongodb_password)
)


def create_connection() -> Database[Union[Mapping[str, Any], Any]]:
    """Initialize mongodb connection
    :return: connection pool
    """
    client = pymongo.MongoClient(DB_URI)
    db_conn = client["conviva"]
    return db_conn


def get_all_data():
    conn = create_connection()
    db_conn = conn["prod-conviva-data"]
    for rec in db_conn.find({}).limit(10):
        print(rec)


def get_all_data_by_parameters():
    conn = create_connection()
    db_conn = conn["prod-conviva-data"]
    for rec in db_conn.find({"DeviceOs": "Android", "Country": "indonesia"}).limit(10):
        print(rec)


def get_all_data_convert_to_dataframe():
    conn = create_connection()
    db_conn = conn["prod-conviva-data"]
    resp = [o for o in db_conn.find({}).limit(10)]
    df = pandas.DataFrame(resp)
    print(df.head(10))


if __name__ == "__main__":
    get_all_data_convert_to_dataframe()
