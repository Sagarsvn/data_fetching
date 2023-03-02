import uuid

from rplus_ingestor.content.preprocessing.episode \
    import episode_data_preprocessing
from config.config import content_path, content_loader_path
from config.constant_an import EPISODE_AN_RENAME, EPISODE, PKL, CSV
from dump_all_hist.create_node import GenerateNode
from export_data.export_bulk_content import fetch_episode
from utils.logger import Logging
from utils.s3_service import S3Service


class DumpEpisode:
    def __init__(
            self
    ):

        self.cls = S3Service.from_connection()

    def fetch_raw_episode_data(
            self
    ):
        """
       fetch data from redis and s3
       :retun: Program raw data
        """
        Logging.info(f"Fetching {EPISODE} raw data  ".center(100, "*"))
        try:
            episode_data = \
                fetch_episode()
        except:
            episode_data = self.cls.read_pickles_from_s3(
                object_name=f'{content_path}{EPISODE}{PKL}')

        return episode_data

    def dump_episode_on_graph(
            self
    ):
        """
         dump episode
         data on graph
        """

        episode_data = DumpEpisode().\
            fetch_raw_episode_data()

        Logging.info(
            "Start preprocessing".center(
                100, "*"
            )
        )

        episode_data = episode_data_preprocessing(
            episode_data
        )

        episode_data["~id"] = "episode:" + episode_data['episode_id']

        episode_data['~label'] = EPISODE

        episode_data = episode_data.rename(
            EPISODE_AN_RENAME, axis=1
        )

        self.cls.write_csv_to_s3(
            object_name=f'{content_loader_path}{EPISODE}{CSV}',
            df_to_upload=episode_data)

        GenerateNode.create_node(
            key=f'{content_loader_path}{EPISODE}{CSV}'
        )

        Logging.info(
            f"Successfully dump {EPISODE} on the graph ".center(100, "*")
        )


