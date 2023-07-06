import datetime

import pandas as pd
import pymongo
from pandas import DataFrame
from rplus_utils.preprocessing.convert_time_zone import convert_column_to_iso
from tqdm import tqdm

from config.config import Config, static_path, content_path, user_path
from config.constant import (
    ACTOR_REQUIRED,
    ACTOR_RENAME,
    GENRE_REQUIRED,
    GENRE_RENAME,
    DIRECTOR_RENAME,
    DIRECTOR_REQUIRED,
    CLIP_RENAME,
    EXTRA_RENAME,
    WRITER_REQUIRED,
    WRITER_RENAME,
    PROGRAM_TYPE_REQUIRED,
    PROGRAM_TYPE_RENAME,
    EPISODE_RENAME,
    CLIP_REQUIRED,
    EXTRA_REQUIRED,
    EPISODE_REQUIRED, CUSTOMER_REQUIRED, CUSTOMER_RENAME
)
from config.constant_an import UPDATED_AT, CREATED_AT

from utils.s3_service import S3Service


class S3Connector:
    def __init__(self):
        self.s3_service = S3Service.from_connection()

    def store_pickle_to_s3(self, data, object_name):
        self.s3_service.write_df_pkl_to_s3(data=data, object_name=object_name)

    def store_csv_to_s3(self, data, object_name):
        return self.s3_service.write_csv_to_s3(df_to_upload=data, object_name=object_name)

    def fetch_pickle_from_s3(self, object_name):
        return self.s3_service.read_pickles_from_s3(object_name=object_name)

    def fetch_csv_from_s3(self, object_name):
        return self.s3_service.read_csv_from_s3(object_name=object_name)



class MongoConnector:
    def __init__(self):
        self.config = Config()
        self.client = pymongo.MongoClient(self.config.rplus_mongodb_uri)

    def get_database_connection(self, database_name):
        return self.client[database_name]

    def fetch_from_collection(self, collection_name):
        collection = self.get_database_connection('conviva')[collection_name]
        cursor = collection.find({})
        total_docs = collection.count_documents({})
        docs = []
        with tqdm(total=total_docs) as pbar:
            for doc in cursor:
                docs.append(doc)
                pbar.update(1)
                pbar.set_description(f"Fetching {len(docs)} documents")
        return DataFrame.from_records(docs)

    def close_connection(self):
        self.client.close()


class DataFetcher:
    def __init__(self):
        self.s3_connector = S3Connector()
        self.mongo_connector = MongoConnector()

    def fetch_clip_data(self):
        clip = self.mongo_connector.fetch_from_collection('master:clip')
        self.mongo_connector.close_connection()
        clip = clip[CLIP_REQUIRED].rename(columns=CLIP_RENAME)
        self.s3_connector.store_pickle_to_s3(clip, content_path + "clip.pkl")
        return clip

    def fetch_extra_data(self):
        extra = self.mongo_connector.fetch_from_collection('master:extra')
        extra = extra[EXTRA_REQUIRED].rename(columns=EXTRA_RENAME)
        self.s3_connector.store_pickle_to_s3(extra, content_path + "extra.pkl")
        return extra

    def fetch_episode_data(self):
        episode = self.mongo_connector.fetch_from_collection('master:episode')
        episode = episode[EPISODE_REQUIRED].rename(columns=EPISODE_RENAME)
        self.s3_connector.store_pickle_to_s3(episode, content_path + "episode.pkl")
        return episode

    def fetch_program_data(self):
        program = self.mongo_connector.fetch_from_collection('master:program')
        program = program[PROGRAM_TYPE_REQUIRED].rename(columns=PROGRAM_TYPE_RENAME)
        self.s3_connector.store_pickle_to_s3(program, content_path + "program.pkl")
        return program

    def fetch_actor_data(self):
        actor = self.mongo_connector.fetch_from_collection('master:starring')
        actor = actor[ACTOR_REQUIRED].rename(columns=ACTOR_RENAME)
        self.s3_connector.store_pickle_to_s3(actor, static_path + "actor.pkl")
        return actor

    def fetch_genre_data(self):
        genre = self.mongo_connector.fetch_from_collection('master:genre')
        genre = genre[GENRE_REQUIRED].rename(columns=GENRE_RENAME)
        self.s3_connector.store_pickle_to_s3(genre, static_path + "genre.pkl")
        return genre

    def fetch_writer_data(self):
        writer = self.mongo_connector.fetch_from_collection('master:writers')
        writer = writer[WRITER_REQUIRED].rename(columns=WRITER_RENAME)
        self.s3_connector.store_pickle_to_s3(writer, static_path + "writer.pkl")
        return writer

    def fetch_director_data(self):
        director = self.mongo_connector.fetch_from_collection('master:directors')
        director = director[DIRECTOR_REQUIRED].rename(columns=DIRECTOR_RENAME)
        self.s3_connector.store_pickle_to_s3(director, static_path + "director.pkl")
        return director

    def fetch_customer_data(self):
        customer = self.mongo_connector.fetch_from_collection('master:user')
        customer = customer[CUSTOMER_REQUIRED].rename(columns=CUSTOMER_RENAME)
        self.s3_connector.store_pickle_to_s3(customer, user_path + "customer.pkl")
        return customer
