import uuid

from rplus_ingestor.content.preprocessing.program import program_data_preprocessing

from config.config import content_path, content_loader_path
from config.constant_an import PROGRAM, PROGRAM_AN_RENAME, CSV, PROGRAM_DEPENDENCIES, PROGRAM_DEPENDENCIES_DROP_COLUMN
from dump_all_hist.create_node import GenerateNode
from export_data.export_bulk_content import fetch_program_type
from utils.logger import Logging
from utils.s3_service import S3Service


class DumpProgram:

    def __init__(
            self
    ):

        self.cls = S3Service.from_connection()

    def fetch_raw_program_data(
            self
    ):
        """
       fetch data from redis and s3
       :retun: Program raw data
        """

        try:
            program_data = \
                fetch_program_type()
        except:
            program_data = self.cls.read_pickles_from_s3(
                object_name=content_path + "program.pkl")

        return program_data

    def dump_program_on_graph(
            self
    ):
        """
        read program data
        dump on graph
        """
        program = DumpProgram().fetch_raw_program_data()

        Logging.info(
            f"Start preprocessing of {PROGRAM} ".center(
                100, "*"
            )
        )
        program = program_data_preprocessing(program)

        program["~id"] = "program:"+program["program_id"]

        program['~label'] = PROGRAM

        program_dependencies = program[PROGRAM_DEPENDENCIES]

        self.cls.write_csv_to_s3(
            object_name=f'{content_loader_path}program_dependencies{CSV}',
            df_to_upload=program_dependencies)

        program = program.drop(
            PROGRAM_DEPENDENCIES_DROP_COLUMN,
            axis=1)

        program = program.rename(
            PROGRAM_AN_RENAME, axis=1
        )

        self.cls.write_csv_to_s3(
            object_name=f'{content_loader_path}{PROGRAM}{CSV}',
            df_to_upload=program)

        GenerateNode.create_node(
            key=f'{content_loader_path}{PROGRAM}{CSV}'
        )
        Logging.info(
            f"Successfully dump {PROGRAM} on the graph ".center(
                100, "*"
            )
        )



