import uuid

from config.config import content_path
from config.constant_an import PROGRAM
from export_data.export_bulk_content import fetch_program_type
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
                object_name=content_path+"program.pkl")

        return program_data

    def program_on_graph(
            self
    ):
        """
        read program data
        dump on graph
        """

        program = DumpProgram.fetch_raw_program_data()

        program["~id"] = program.apply(lambda _: str(uuid.uuid4()), axis=1)

        program['~label'] = PROGRAM

