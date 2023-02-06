import uuid

import pandas as pd
from rplus_ingestor.user.preprocessing.user import PreprocessUser

from config.config import user_path, user_loader_path
from config.constant_an import USER_AN_RENAME, USER, USER_AN_REQUIRED, CUSTOMER_PREFERENCE, CSV
from dump_all_hist.create_node import GenerateNode
from export_data.export_bulk_user import export_all_customer
from utils.logger import Logging
from utils.s3_service import S3Service


class DumpUser:
    def __init__(
            self
    ):

        self.cls = S3Service.from_connection()

    def fetch_data(
            self
    ):
        Logging.info(
            "Fetching customer raw data  ".center(
                100, "*")
        )
        try:
            user = export_all_customer()

        except:

            user = self.cls.read_pickles_from_s3(
                object_name=f'{user_path}customer.pkl')

        return user

    def preprocessing_user(
            self, user
    ):
        """
        preprocessing
        of user
        """

        Logging.info(
            "Start preprocessing".center(
                100, "*"
            )
        )

        user = PreprocessUser().controller(user)

        user = user.rename(USER_AN_RENAME, axis=1).drop("birth_date",
                                                        axis="columns")

        return user

    def loader_csv(self,
                   user):
        """
        create loader csv and
         dump on the graph
        """
        Logging.info(
            f"Creating user loader csv ".center(100, "*")
        )

        user["~id"] = user.apply(
            lambda _: str(uuid.uuid4()),
            axis=1)

        user['~label'] = USER

        user_pref = user[USER_AN_REQUIRED]

        self.cls.write_csv_to_s3(
            object_name=f'{user_loader_path}{CUSTOMER_PREFERENCE}{CSV}',
            df_to_upload=user_pref)

        user = user.drop(CUSTOMER_PREFERENCE, axis="columns")

        self.cls.write_csv_to_s3(
            object_name=f'{user_loader_path}user_history.csv',
            df_to_upload=user)

    def dump_user_on_graph(
            self
    ):
        """
        fetch_data from redis or s3
        preprocessing of customer
        create loader csv
        dump on the Neptune Graph
        """

        user = self.fetch_data()

        user = self.preprocessing_user(user)

        self.loader_csv(user)

        GenerateNode.create_node(
            key=f'{user_loader_path}user_history.csv'
        )

        Logging.info(
            f"Successfully dump {USER} on the graph ".center(100, "*")
        )
