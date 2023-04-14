
import pandas as pd

from rplus_ingestor.content.preprocessing.program import program_data_preprocessing
from config.config import content_path, content_loader_path
from config.constant_an import PROGRAM, PROGRAM_AN_RENAME, CSV, PROGRAM_DEPENDENCIES, PROGRAM_DEPENDENCIES_DROP_COLUMN
from dump_all_hist.update_node import UpdateNode
from export_data.export_mongo import DataFetcher, S3Connector
from utils.logger import Logging


class DumpProgram:
    def __init__(self):
        self.s3_connector = S3Connector()
        self.graph_node = UpdateNode()

    def fetch_raw_program_data(self) -> pd.DataFrame:
        """
        Fetches raw program data from the `DataFetcher` class, and if there's an error,
        it retrieves the data from an S3 bucket using the `fetch_from_s3()` method
        of the `S3Connector` object.
        """
        Logging.info(f"Fetching {PROGRAM} raw data".center(100, "*"))
        try:
            program_data = DataFetcher().fetch_program_data()
            Logging.info(f"Successfully fetched {PROGRAM} raw data".center(100, "*"))
        except Exception as e:
            Logging.error(f"Error fetching {PROGRAM} raw data: {e}")
            program_data = self.s3_connector.fetch_pickle_from_s3(object_name=f"{content_path}{PROGRAM}{CSV}")
            Logging.info(f"Using previously fetched {PROGRAM} raw data".center(100, "*"))
        return program_data

    def dump_program_on_graph(self):
        """
        Dumps program data to the graph.
        """
        program_data = self.fetch_raw_program_data()

        Logging.info("Start preprocessing".center(100, "*"))
        program_data = program_data_preprocessing(program_data)
        program_data["~id"] = "program:" + program_data["program_id"]
        program_data["~label"] = PROGRAM

        program_dependencies = program_data[PROGRAM_DEPENDENCIES]

        self.s3_connector.store_csv_to_s3(data=program_dependencies, object_name=f"{content_loader_path}program_dependencies{CSV}")

        program_data = program_data.drop(PROGRAM_DEPENDENCIES_DROP_COLUMN, axis=1)
        program_data = program_data.rename(PROGRAM_AN_RENAME, axis=1)

        self.s3_connector.store_csv_to_s3(data=program_data, object_name=f"{content_loader_path}{PROGRAM}{CSV}")
        self.graph_node.update_node(key=f"{content_loader_path}{PROGRAM}{CSV}")

        Logging.info(f"Successfully dumped {PROGRAM} on the graph".center(100, "*"))



