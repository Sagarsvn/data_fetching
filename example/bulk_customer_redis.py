import ast

from pandas import DataFrame
import redis
from copy import deepcopy
from typing import List, Dict, Any


def create_connection(db: int) -> redis.StrictRedis:
    """Initialize redis connection
    :param db: selected db
    :return: connection pool
    """
    pool = redis.ConnectionPool(
        host="localhost",
        port=63791,
        db=db,
        username="aiml",
        password="UJ9Gbhh9uEJTpAZf",
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
        chunk_size: int = 1000,
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
    final_resp = []
    for keys in chunk_array_keys(keys):
            n = len(keys)
            print("fetch value from : {} keys".format(n))
            # open connection
            conn = create_connection(0)
            # setup pipeline
            p = conn.pipeline()
            # get value from keys
            _ = [p.hgetall(key) for key in keys]
            # execute pipeline
            q += [result for result in p.execute()]
            for resp in q:
                temp = {}
                for k, v in resp.items():
                    temp[k.decode('utf-8')] = v.decode('utf-8')
                final_resp.append(temp)

            len_q = len(final_resp)
            print("q now is: {} records".format(len_q))
            # close connection
            conn.close()
    return final_resp


def export_data_db_0():
    """Export data from db 0"""
    # get all keys
    keys = fetch_all_keys()
    # fetch value from keys
    values = fetch_value_from_keys(keys)
    # convert to dataframe
    df = DataFrame(values)
    # get len dataframe
    n = len(df)
    print("total data: {} records".format(n))
    print("exporting to csv")
    # export to compressed pickle
    df.to_pickle("customer.pkl", compression='gzip')


if __name__ == "__main__":
    export_data_db_0()
