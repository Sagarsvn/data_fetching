from pandas import DataFrame
import redis
from copy import deepcopy
from typing import List, Dict, Any

from config.config import user_path, Config
from config.constant import CUSTOMER_REQUIRED, CUSTOMER_RENAME
from utils.logger import Logging
from utils.s3_service import S3Service


def create_connection(db: int) -> redis.StrictRedis:
    """Initialize redis connection
    :param db: selected db
    :return: connection pool
    """
    pool = redis.ConnectionPool(
        host=Config().redis_localhost,
        port=Config().redis_port,
        db=db,
        username=Config().redis_user_name,
        password=Config().redis_password,
    )
    conn = redis.StrictRedis(
        connection_pool=pool,
        retry_on_timeout=True,
        socket_timeout=10_0000,
        socket_connect_timeout=10_0000,
    )
    return conn


def chunk_array_keys(
        keys: List[str],
        chunk_size: int = 10000,
) -> List[List[str]]:
    """Split array of keys into chunks
    :param keys: list of keys
    :param chunk_size: size of chunk
    :return: list of chunks
    """
    return [keys[i: i + chunk_size] for i in range(0, len(keys), chunk_size)]


def fetch_all_keys() -> List[str]:
    """Fetch all keys only from redis
    :return: list of all keys in redis
    """
    # open connection
    conn = create_connection(0)
    # get all keys
    keys = deepcopy(conn.keys())
    # close connection
    conn.close()
    return keys


def fetch_value_from_keys(
        keys: List[str],
) -> List[Dict[str, Any]]:
    """Fetch value from keys
    :param keys: list of keys
    """
    q = []
    for keys in chunk_array_keys(keys):
            n = len(keys)
            Logging.info("fetch value from : {} keys".format(n))
            # open connection
            conn = create_connection(0)
            # setup pipeline
            p = conn.pipeline()
            # get value from keys
            _ = [p.hgetall(key) for key in keys]
            # execute pipeline
            q += [result for result in p.execute()]

            len_q = len(q)
            Logging.info("q now is: {} records".format(len_q))
            # close connection
            conn.close()
    return q


def remove_binary_object(values):
    """
    remove binary values
    """
    final_resp = []
    for resp in values:
        temp = {}
        for k, v in resp.items():
            temp[k.decode('utf-8')] = v.decode('utf-8')
        final_resp.append(temp)
    return final_resp


def export_all_customer():
    """Export data from db 0"""
    Logging.info("fetching all customer data".center(100, "*"))
    # get all keys
    keys = fetch_all_keys()
    Logging.info(keys)
    # fetch value from keys
    values = fetch_value_from_keys(keys)
    # remove binary value from record
    final_resp = remove_binary_object(values)
    # convert to dataframe
    df = DataFrame(final_resp)
    # rename column
    df = df[CUSTOMER_REQUIRED]
    # required column
    final_df = df.rename(columns=CUSTOMER_RENAME)
    # get len dataframe
    n = len(final_df)
    Logging.info("total data: {} records".format(n))
    # export to csv
    Logging.info("exporting to csv")
    cls = S3Service.from_connection()
    cls.write_df_pkl_to_s3(data=final_df, object_name=
    user_path + "customer.pkl")

    return df




