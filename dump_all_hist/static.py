import pandas as pd

from rplus_ingestor.static.preprocessing.static import static_data_preprocessing
from dump_all_hist.update_node import UpdateNode

from export_data.export_mongo import S3Connector, DataFetcher
from utils.logger import Logging

from config.config import (
    static_loader_path,
    static_path,
    genre_missing_id,
)
from config.constant_an import (
    ACTOR_AN_RENAME,
    ACTOR,
    WRITER,
    DIRECTOR,
    GENRE,
    WRITER_AN_RENAME,
    DIRECTOR_AN_RENAME,
    GENRE_AN_RENAME,
    PKL,
    CSV,
)


class DumpStatic:
    def __init__(self):
        self.s3_connector = S3Connector()
        self.graph_node = UpdateNode()
        self.fetch = DataFetcher()


    def fetch_and_dump(self, data_type, fetch_function):
        """
        Fetch data and dump into Neptune graph.
        """
        try:
            Logging.info(f"Fetching {data_type}".center(100, "*"))
            try:
                data = fetch_function()
            except:
                data = self.s3_connector.fetch_pickle_from_s3(
                    object_name=f"{static_path}{data_type}{PKL}"
                )

            data = static_data_preprocessing(data, data_type)

            if data_type != GENRE:
                data["~id"] = f"{data_type.lower()}:" + data[f"{data_type.lower()}_id"]
                data['~label'] = data_type

            else:
                missing_record = pd.DataFrame(genre_missing_id)
                data = data.append(missing_record, ignore_index=True)
                data['created_at'] = 'nan'
                data['updated_at'] = 'nan'
                data["~id"] = f"{data_type.lower()}:" + data[f"{data_type.lower()}_id"]
                data['~label'] = data_type

            rename_dict = {
                ACTOR: ACTOR_AN_RENAME,
                WRITER: WRITER_AN_RENAME,
                DIRECTOR: DIRECTOR_AN_RENAME,
                GENRE: GENRE_AN_RENAME,
            }
            data = data.rename(rename_dict[data_type], axis=1)

            Logging.info(f"Total record of {data_type}: {len(data)}")

            self.s3_connector.store_csv_to_s3(
                object_name=f"{static_loader_path}{data_type}{CSV}",
                data=data,
            )

            UpdateNode.update_node(key=f"{static_loader_path}{data_type}{CSV}")

        except Exception as e:
            Logging.error(
                f"Unable to dump the {data_type} data on graph network: {str(e)}"
            )

    def actor(self):
        self.fetch_and_dump(ACTOR, self.fetch.fetch_actor_data)

    def writer(self):
        self.fetch_and_dump(WRITER, self.fetch.fetch_writer_data)

    def director(self):
        self.fetch_and_dump(DIRECTOR,self.fetch.fetch_director_data)

    def genre(self):
        self.fetch_and_dump(GENRE, self.fetch.fetch_genre_data)

    @staticmethod
    def dump_static_node():
        """
        Dump all static node on graph.
        """
        DumpStatic().actor()
        DumpStatic().writer()
        DumpStatic().director()
        DumpStatic().genre()


