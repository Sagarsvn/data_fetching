import pandas as pd

from rplus_ingestor.content.preprocessing.episode import episode_data_preprocessing
from config.config import content_path, content_loader_path
from config.constant_an import EPISODE_AN_RENAME, EPISODE, PKL, CSV
from dump_all_hist.update_node import UpdateNode
from export_data.export_mongo import DataFetcher, S3Connector
from utils.logger import Logging


class DumpEpisode:
    def __init__(self):
        self.s3_connector = S3Connector()
        self.graph_node = UpdateNode()

    def fetch_raw_episode_data(self) -> pd.DataFrame:
        """
        Fetches raw episode data from the `DataFetcher` class, and if there's an error,
        it retrieves the data from an S3 bucket using the `fetch_from_s3()` method
        of the `S3Connector` object.
        """
        Logging.info(f"Fetching {EPISODE} raw data".center(100, "*"))
        try:
            episode_data = DataFetcher().fetch_episode_data()
            Logging.info(f"Successfully fetched {EPISODE} raw data".center(100, "*"))
        except Exception as e:
            Logging.error(f"Error fetching {EPISODE} raw data: {e}")
            episode_data = self.s3_connector.fetch_pickle_from_s3(object_name=f"{content_path}{EPISODE}{PKL}")
            Logging.info(f"Using previously fetched {EPISODE} raw data".center(100, "*"))
        return episode_data

    def dump_episode_on_graph(self):
        """
        Dumps episode data to the graph.
        """
        episode_data = self.fetch_raw_episode_data()

        Logging.info("Start preprocessing".center(100, "*"))
        episode_data = episode_data_preprocessing(episode_data)
        episode_data["~id"] = "episode:" + episode_data["episode_id"]
        episode_data["~label"] = EPISODE
        episode_data = episode_data.rename(EPISODE_AN_RENAME, axis=1)

        self.s3_connector.store_csv_to_s3(data=episode_data, object_name=f"{content_loader_path}{EPISODE}{CSV}")
        self.graph_node.update_node(key=f"{content_loader_path}{EPISODE}{CSV}")
        Logging.info(f"Successfully dumped {EPISODE} on the graph".center(100, "*"))


