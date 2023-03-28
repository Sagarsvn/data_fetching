from pandas import DataFrame

from config.config import user_loader_path
from config.constant_an import USER_PROFILE_DROP, USER_PROFILE_PREFERENCE_RENAME, CUSTOMER_ID, USER_PROFILE_RENAME, USER
from dump_all_hist.update_node import UpdateNode
from utils.logger import Logging
from utils.s3_service import S3Service


class UpdateUserProfileCluster:

    def __init__(self):
        self.cls = S3Service.from_connection()

    def fetch_user_profile(self):
        """
        fetch user
        profile from s3
        """
        user_profile = self.cls.read_pickles_from_s3(
            object_name="user_data/users_profile.pkl"
        )
        return user_profile

    def convert_data_type(self,
                          user_profile:DataFrame):
        """
        convert data type for all
        preference and cluster
        """

        user_profile[CUSTOMER_ID] = user_profile[CUSTOMER_ID].astype(str)
        for column in user_profile.columns:
            if 'preference' in column or 'age' in column or 'cluster_id' in column:
                Logging.info("converting for {}".format(column))
                user_profile[column] = user_profile[column].astype(str)
                user_profile[column] = user_profile[column].apply(
                    lambda x: int(float(x)) if x != 'nan' else (-1 if 'preference' in column else -999)
                )
        return user_profile

    def update_user_profile(self) -> DataFrame:
        """
        fetch data from s3
        and covert datatype
        add label and ~id for
        all user
        """

        user_profile = self.fetch_user_profile()

        user_profile = user_profile.drop(USER_PROFILE_DROP,axis=1)

        user_profile = self.convert_data_type(user_profile)

        user_profile = user_profile.rename(USER_PROFILE_PREFERENCE_RENAME,axis=1)

        [user_profile.rename({col: col + ":String(single)"}, axis=1, inplace=True)

        for col in user_profile.columns if 'preference' in col]

        user_profile["~id"] = "user:" + user_profile[CUSTOMER_ID]

        user_profile['~label'] = USER

        user_profile = user_profile.rename(USER_PROFILE_RENAME,axis=1)

        return user_profile

    def update_in_graph(self) -> bool:
        """
        update on AWS Graph
        """

        update_user_profile = self.update_user_profile()
        self.cls.write_csv_to_s3(
            object_name=f'{user_loader_path}updated_user_profile.csv',
            df_to_upload=update_user_profile)

        UpdateNode.update_node(
            key=f'{user_loader_path}updated_user_profile.csv'
        )
        Logging.info(
            f"Successfully dump updated user profile on the graph ".center(100, "*")
        )

        return True







