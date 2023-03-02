from rplus_ingestor.content.preprocessing.extra \
    import extra_data_preprocessing
from config.config import content_path, content_loader_path
from config.constant_an import EXTRA, EXTRA_AN_RENAME, CSV, PKL
from dump_all_hist.create_node import GenerateNode
from export_data.export_bulk_content import fetch_extra
from utils.logger import Logging
from utils.s3_service import S3Service


class DumpExtra:
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

        try:
            clip_data = \
                fetch_extra()
        except:
            clip_data = self.cls.read_pickles_from_s3(
                object_name=f'{content_path}{EXTRA}{PKL}')

        return clip_data

    def dump_extra_on_graph(
            self
    ):
        """
         dump extra data
         on graph
        """

        extra_data = DumpExtra() \
            .fetch_raw_data()

        Logging.info(
            "Start preprocessing".center(
                100, "*"
            )
        )

        extra_data = extra_data_preprocessing(
            extra_data
        )

        extra_data["~id"] = "extra:" + extra_data["extra_id"]

        extra_data['~label'] = EXTRA

        extra_data = extra_data.rename(
            EXTRA_AN_RENAME, axis=1
        )

        self.cls.write_csv_to_s3(
            object_name=f'{content_loader_path}{EXTRA}{CSV}',
            df_to_upload=extra_data)

        GenerateNode.create_node(
            key=f'{content_loader_path}{EXTRA}{CSV}'
        )
        Logging.info(
            f"Successfully dump {EXTRA} on the graph ".center(100, "*")
        )


