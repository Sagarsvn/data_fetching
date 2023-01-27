import uuid

from rplus_ingestor.content.preprocessing.clip\
    import clip_data_preprocessing
from config.config import content_path, content_loader_path
from config.constant_an import CLIP, CLIP_AN_RENAME, CSV, PKL
from dump_all_hist.create_node import GenerateNode
from export_data.export_bulk_content import  fetch_clip
from utils.logger import Logging
from utils.s3_service import S3Service


class DumpClip:
    def __init__(
            self
    ):

        self.cls = S3Service.from_connection()

    def fetch_raw_data(
            self
    ):
        """
       fetch data from redis and s3
       :retun: Program raw data
        """
        Logging.info("Fetching clip raw data  ".center(100, "*"))
        try:
            clip_data = \
                fetch_clip()
        except:
            clip_data = self.cls.read_pickles_from_s3(
                object_name=f'{content_path}{CLIP}{PKL}')

        return clip_data

    def dump_clip_on_graph(
            self
    ):
        """
         dump clip
          data on graph
        """

        clip_data = DumpClip().\
            fetch_raw_data()

        Logging.info(
            "Start preprocessing".center(
                100, "*"
            )
        )

        clip_data = clip_data_preprocessing(
            clip_data
        )

        clip_data = clip_data.rename(
            CLIP_AN_RENAME,
            axis=1)

        clip_data["~id"] = clip_data.apply(
            lambda _: str(uuid.uuid4()),
            axis=1)

        clip_data['~label'] = CLIP

        self.cls.write_csv_to_s3(
            object_name=f'{content_loader_path}{CLIP}{CSV}',
            df_to_upload=clip_data)

        GenerateNode.create_node(
            key=f'{content_loader_path}{CLIP}{CSV}'
        )

        Logging.info(
            f"Successfully dump {CLIP} on the graph ".center(100, "*")
        )


