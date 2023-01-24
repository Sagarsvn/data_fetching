import ast
from itertools import chain

from pandas import DataFrame
import redis
from copy import deepcopy
from typing import List, Dict, Any

from config.config import Config, static_path, content_path
from config.constant import ACTOR_REQUIRED, ACTOR_RENAME, GENRE_REQUIRED, GENRE_RENAME, DIRECTOR_RENAME, \
    DIRECTOR_REQUIRED, CLIP_RENAME, EXTRA_RENAME, WRITER_REQUIRED, WRITER_RENAME, PROGRAM_TYPE_REQUIRED, \
    PROGRAM_TYPE_RENAME, EPISODE_RENAME, CLIP_REQUIRED, EXTRA_REQUIRED, EPISODE_REQUIRED
from utils.logger import Logging
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
        Logging.info(f"fetch value from : {n} keys")
        # open connection
        conn = create_connection(1)
        # setup pipeline
        p = conn.pipeline()
        # get value from keys
        _ = [p.get(key) for key in keys]
        # execute pipeline
        q += [result.decode("utf-8") for result in p.execute()]

        len_q = len(q)
        Logging.info(f"q now is: {len_q} records")
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
    actor = DataFrame(list(chain.from_iterable(values)))
    # rename column
    actor = actor[ACTOR_REQUIRED]
    # required column
    actor = actor.rename(columns=ACTOR_RENAME)
    # get len dataframe
    n = len(actor)
    Logging.info(f"total data: {n} records")
    # save to s3
    cls.write_df_pkl_to_s3(data=actor, object_name=
    static_path + "actor.pkl")

    return actor


def fetch_genre():
    """ fetch gernes from redis
                store into s3
               """
    keys = fetch_all_keys(b'master:genre:*')
    # fetch value from keys
    values = fetch_value_from_keys(keys)
    # convert to dataframe
    genre = DataFrame(list(chain.from_iterable(values)))
    # rename column
    genre = genre[GENRE_REQUIRED]
    # required column
    genre = genre.rename(columns=GENRE_RENAME)
    # get len dataframe
    n = len(genre)
    Logging.info(f"total data: {n} records")
    # save to s3
    cls.write_df_pkl_to_s3(data=genre, object_name=
    static_path + "genre.pkl")

    return genre


def fetch_writers():
    """ fetch writers from redis
              store into s3
             """
    keys = fetch_all_keys(b'master:writers:*')
    # fetch value from keys
    values = fetch_value_from_keys(keys)
    # convert to dataframe
    writer = DataFrame(list(chain.from_iterable(values)))
    # rename column
    writer = writer[WRITER_REQUIRED]
    # required column
    writer = writer.rename(columns=WRITER_RENAME)
    # get len dataframe
    n = len(writer)
    Logging.info(f"total data: {n} records")
    # save to s3
    cls.write_df_pkl_to_s3(data=writer, object_name=
    static_path + "writer.pkl")

    return writer


def fetch_directors():
    """ fetch clip from directors
              store into s3
             """
    keys = fetch_all_keys(b'master:directors:*')
    # fetch value from keys
    values = fetch_value_from_keys(keys)
    # convert to dataframe
    director = DataFrame(list(chain.from_iterable(values)))
    # required column
    director = director[DIRECTOR_REQUIRED]
    # rename column
    director = director.rename(columns=DIRECTOR_RENAME)

    # get len dataframe
    n = len(director)
    Logging.info(f"total data: {n} records")
    # save to s3
    cls.write_df_pkl_to_s3(data=director, object_name=
    static_path + "director.pkl")

    return director

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
    Logging.info(f"total data: {n} records")
    # save to s3
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
    clip = DataFrame(list(chain.from_iterable(values)))
    # required column
    clip = clip[CLIP_REQUIRED]
    # rename column
    clip = clip.rename(columns=CLIP_RENAME)
    # get len dataframe
    n = len(clip)
    Logging.info(f"total data: {n} records")
    cls.write_df_pkl_to_s3(data=clip,
                           object_name=static_path + "clip.pkl")


def fetch_extra():
    """
          fetch extra  from redis
          store into s3
         """
    keys = fetch_all_keys(b'master:extra:*')
    # fetch value from keys
    values = fetch_value_from_keys(keys)
    # convert to dataframe
    extra = DataFrame(list(chain.from_iterable(values)))
    # required column
    extra = extra[EXTRA_REQUIRED]
    # rename column
    extra = extra.rename(columns=EXTRA_RENAME)
    # get len dataframe
    n = len(extra)
    Logging.info(f"total data: {n} records")
    # save to s3
    cls.write_df_pkl_to_s3(data=extra, object_name=
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
    episode = DataFrame(list(chain.from_iterable(values)))
    # required data
    episode = episode[EPISODE_REQUIRED]
    # rename column
    episode = episode.rename(columns=EPISODE_RENAME)
    # get len dataframe
    n = len(episode)
    Logging.info(f"total data: {n} records")
    # save to s3
    cls.write_df_pkl_to_s3(data=episode, object_name=
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
    program = DataFrame(list(chain.from_iterable(values)))
    # required column
    program = program[PROGRAM_TYPE_REQUIRED]
    # rename column
    program = program.rename(columns=PROGRAM_TYPE_RENAME)
    # get len dataframe
    n = len(program)
    Logging.info(f"total data: {n} records")
    # save to s3
    cls.write_df_pkl_to_s3(data=program, object_name=
    content_path + "program.pkl")


def fetch_all_content_with_static():
    Logging.info("fetching actor".center(100, "*"))

    fetch_actor()

    Logging.info("fetching genre".center(100, "*"))
    fetch_genre()

    Logging.info("fetching writer".center(100, "*"))
    fetch_writers()

    Logging.info("fetching director".center(100, "*"))
    fetch_directors()

    Logging.info("fetching tv".center(100, "*"))
    fetch_tv()

    Logging.info("fetching clip".center(100, "*"))
    fetch_clip()

    Logging.info("fetching extra".center(100, "*"))
    fetch_extra()

    Logging.info("fetching episode".center(100, "*"))
    fetch_episode()

    Logging.info("fetching program_type".center(100, "*"))
    fetch_program_type()
