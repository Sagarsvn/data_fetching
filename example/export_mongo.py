from typing import Union, Any, Mapping
import pandas
import pymongo
from pymongo.database import Database

DB_URI = "mongodb://aiml:rctiplus2022%40%40@localhost:27017/conviva?authSource=conviva&directConnection=true"


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
    tmp = []
    for rec in db_conn.find(
        {
            "DataDate": {
                "$lte": "2022-11-17",
                "$gte": "2022-05-01",
            }
        }
    ):
        tmp.append(rec)

    pandas.DataFrame(tmp).to_csv("ubd.csv", index=False)


if __name__ == "__main__":
    get_all_data()
