import ast
from itertools import chain

from pandas import DataFrame
import redis
from copy import deepcopy
from typing import List, Dict, Any

from config.config import Config, static_path, content_path
from utils.s3_service import S3Service

cls = S3Service.from_connection()


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
        chunk_size: int = 1000,
) -> List[List[str]]:
    """Split array of keys into chunks
    :param keys: list of keys
    :param chunk_size: size of chunk
    :return: list of chunks
    """
    return [keys[i: i + chunk_size] for i in range(0, len(keys), chunk_size)]


def fetch_all_keys(master_key) -> List[str]:
    """Fetch all keys only from redis
    :return: list of all keys in redis
    """
    # open connection
    conn = create_connection(1)
    # get all keys
    keys = deepcopy(conn.keys(master_key))
    # close connection
    print(keys)
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
        print("fetch value from : {} keys".format(n))
        # open connection
        conn = create_connection(1)
        # setup pipeline
        p = conn.pipeline()
        # get value from keys
        _ = [p.get(key) for key in keys]
        # execute pipeline
        q += [result.decode("utf-8") for result in p.execute()]

        len_q = len(q)
        print("q now is: {} records".format(len_q))
        # close connection
        conn.close()
    q = [i.replace('null', 'None') for i in q]
    q = [ast.literal_eval(i) for i in q]
    return q


def fetch_actor():
    """ fetch actor from redis
     store into s3 """
    keys = fetch_all_keys(b'master:starring:*')
    # fetch value from keys
    values = fetch_value_from_keys(keys)
    # convert to dataframe
    df = DataFrame(list(chain.from_iterable(values)))
    # get len dataframe
    n = len(df)
    print("total data: {} records".format(n))
    # export to csv
    print("save to s3")
    cls.write_df_pkl_to_s3(data=df, object_name=
    static_path + "actor.pkl")


def fetch_genre():
    """ fetch gernes from redis
                store into s3
               """
    keys = fetch_all_keys(b'master:genre:*')
    # fetch value from keys
    values = fetch_value_from_keys(keys)
    # convert to dataframe
    df = DataFrame(list(chain.from_iterable(values)))
    # get len dataframe
    n = len(df)
    print("total data: {} records".format(n))
    # export to csv
    print("save to s3")
    cls.write_df_pkl_to_s3(data=df, object_name=
    static_path + "genre.pkl")


def fetch_writers():
    """ fetch writers from redis
              store into s3
             """
    keys = fetch_all_keys(b'master:writers:*')
    # fetch value from keys
    values = fetch_value_from_keys(keys)
    # convert to dataframe
    df = DataFrame(list(chain.from_iterable(values)))
    # get len dataframe
    n = len(df)
    print("total data: {} records".format(n))
    # export to csv
    print("save to s3")
    cls.write_df_pkl_to_s3(data=df, object_name=
    static_path + "writers.pkl")


def fetch_directors():
    """ fetch clip from directors
              store into s3
             """
    keys = fetch_all_keys(b'master:directors:*')
    # fetch value from keys
    values = fetch_value_from_keys(keys)
    # convert to dataframe
    df = DataFrame(list(chain.from_iterable(values)))
    # get len dataframe
    n = len(df)
    print("total data: {} records".format(n))
    # export to csv
    print("save to s3")
    cls.write_df_pkl_to_s3(data=df, object_name=
    static_path + "directors.pkl")


def fetch_tv():
    """ fetch clip from tv
              store into s3
             """
    keys = fetch_all_keys(b'master:tv:*')
    # fetch value from keys
    values = fetch_value_from_keys(keys)
    # convert to dataframe
    df = DataFrame(list(chain.from_iterable(values)))
    # get len dataframe
    n = len(df)
    print("total data: {} records".format(n))
    # export to csv
    print("save to s3")
    cls.write_df_pkl_to_s3(data=df, object_name=
    static_path + "tv.pkl")


def fetch_clip():
    """ fetch clip from redis
          store into s3
         """
    keys = fetch_all_keys(b'master:clip:*')
    # fetch value from keys
    values = fetch_value_from_keys(keys)
    # convert to dataframe
    df = DataFrame(list(chain.from_iterable(values)))
    # get len dataframe
    n = len(df)
    print("total data: {} records".format(n))
    # export to csv
    print("save to s3")
    cls.write_df_pkl_to_s3(data=df, object_name=
    static_path + "clip.pkl")


def fetch_extra():
    """
          fetch extra  from redis
          store into s3
         """
    keys = fetch_all_keys(b'master:extra:*')
    # fetch value from keys
    values = fetch_value_from_keys(keys)
    # convert to dataframe
    df = DataFrame(list(chain.from_iterable(values)))
    # get len dataframe
    n = len(df)
    print("total data: {} records".format(n))
    # export to csv
    print("save to s3")
    cls.write_df_pkl_to_s3(data=df, object_name=
    static_path + "extra.pkl")


def fetch_episode():
    """
         fetch episode  from redis
         store into s3
        """
    keys = fetch_all_keys(b'master:episode:*')
    # fetch value from keys
    values = fetch_value_from_keys(keys)
    # convert to dataframe
    df = DataFrame(list(chain.from_iterable(values)))
    # get len dataframe
    n = len(df)
    print("total data: {} records".format(n))
    # export to csv
    print("save to s3")
    cls.write_df_pkl_to_s3(data=df, object_name=
    static_path + "episode.pkl")


def fetch_program_type():
    """
     fetch program type  from redis
     store into s3
    """
    keys = fetch_all_keys(b'master:program:*')
    # fetch value from keys
    values = fetch_value_from_keys(keys)
    # convert to dataframe
    df = DataFrame(list(chain.from_iterable(values)))
    # get len dataframe
    n = len(df)
    print("total data: {} records".format(n))
    # export to csv
    print("save to s3")
    cls.write_df_pkl_to_s3(data=df, object_name=
    content_path + "program.pkl")


def fetch_all_content_with_static():
    print("fetching actor".center(100, "*"))

    fetch_actor()

    print("fetching genre".center(100, "*"))
    fetch_genre()

    print("fetching writer".center(100, "*"))
    fetch_writers()

    print("fetching director".center(100, "*"))
    fetch_directors()

    print("fetching tv".center(100, "*"))
    fetch_tv()

    print("fetching clip".center(100, "*"))
    fetch_clip()

    print("fetching extra".center(100, "*"))
    fetch_extra()

    print("fetching episode".center(100, "*"))
    fetch_episode()

    print("fetching program_type".center(100, "*"))
    fetch_program_type()


if __name__ == "__main__":
    fetch_all_content_with_static()