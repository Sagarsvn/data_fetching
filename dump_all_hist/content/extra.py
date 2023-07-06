import pandas as pd

from rplus_ingestor.content.preprocessing.extra import extra_data_preprocessing
from config.config import content_path, content_loader_path
from config.constant_an import EXTRA, EXTRA_AN_RENAME, CSV, PKL
from dump_all_hist.update_node import UpdateNode
from export_data.export_mongo import S3Connector, DataFetcher
from utils.logger import Logging



class DumpExtra:
    def __init__(self):
        self.s3_connector = S3Connector()
        self.graph_node = UpdateNode()

    def fetch_raw_data(self) -> pd.DataFrame:
        """
        Fetches raw extra data from the `fetch_extra()` method of the `export_bulk_content` module,
        and if there's an error, it retrieves the data from an S3 bucket using the `read_pickles_from_s3()`
        method of the `S3Service` object.
        """
        Logging.info(f"Fetching {EXTRA} raw data".center(100, "*"))
        try:
            extra_data = DataFetcher().fetch_extra_data()
            Logging.info(f"Successfully fetched {EXTRA} raw data".center(100, "*"))
        except Exception as e:
            Logging.error(f"Error fetching {EXTRA} raw data: {e}")
            extra_data = self.s3_connector.store_csv_to_s3(object_name=f"{content_path}{EXTRA}{PKL}")
            Logging.info(f"Using previously fetched {EXTRA} raw data".center(100, "*"))
        return extra_data

    def dump_extra_on_graph(self):
        """
        Dumps extra data to the graph.
        """
        extra_data = self.fetch_raw_data()

        Logging.info("Start preprocessing".center(100, "*"))
        extra_data = extra_data_preprocessing(extra_data)
        extra_data["~id"] = "extra:" + extra_data["extra_id"]
        extra_data["~label"] = EXTRA
        extra_data = extra_data.rename(EXTRA_AN_RENAME, axis=1)

        self.s3_connector.store_csv_to_s3(data=extra_data, object_name=f"{content_loader_path}{EXTRA}{CSV}")
        self.graph_node.update_node(key=f"{content_loader_path}{EXTRA}{CSV}")
        Logging.info(f"Successfully dumped {EXTRA} on the graph".center(100, "*"))
