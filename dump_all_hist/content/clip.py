from pandas import DataFrame
from rplus_ingestor.content.preprocessing.clip import clip_data_preprocessing
from config.config import content_path, content_loader_path
from config.constant_an import CLIP, CLIP_AN_RENAME, CSV, PKL

from dump_all_hist.update_node import UpdateNode
from export_data.export_mongo import DataFetcher, S3Connector
from utils.logger import Logging


class DumpClip:
    def __init__(self):
        self.s3_connector = S3Connector()
        self.graph_node = UpdateNode()

    def fetch_raw_data(self) -> DataFrame:
        Logging.info("Fetching clip raw data".center(100, "*"))
        try:
            clip_data = DataFetcher().fetch_clip_data()
            Logging.info("Successfully fetched clip raw data".center(100, "*"))
        except Exception as e:
            Logging.error(f"Error fetching clip raw data: {e}")
            clip_data = self.s3_connector.fetch_pickle_from_s3(object_name=f"{content_path}{CLIP}{PKL}")
            Logging.info("Using previously fetched clip raw data".center(100, "*"))
        return clip_data

    def dump_clip_on_graph(self):
        """
        Dumps clip data to the graph.
        """
        clip_data = self.fetch_raw_data()

        Logging.info("Start preprocessing".center(100, "*"))
        clip_data = clip_data_preprocessing(clip_data)
        clip_data["~id"] = "clip:" + clip_data["clip_id"]
        clip_data["~label"] = CLIP
        clip_data = clip_data.rename(CLIP_AN_RENAME, axis=1)

        self.s3_connector.store_csv_to_s3(data=clip_data, object_name=f"{content_loader_path}{CLIP}{CSV}")
        self.graph_node.update_node(key=f"{content_loader_path}{CLIP}{CSV}")
        Logging.info(f"Successfully dumped {CLIP} on the graph".center(100, "*"))

