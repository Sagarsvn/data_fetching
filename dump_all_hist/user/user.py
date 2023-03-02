from rplus_offline_result.user_clustering.main import ClusteringMain

from pandas import merge
from rplus_ingestor.user.preprocessing.user import PreprocessUser

from config.config import user_path, user_loader_path
from config.constant_an import USER_AN_RENAME, USER, CUSTOMER_PREFERENCE, CUSTOMER_ID, CUSTOMER_PREFERENCE_1, CUSTOMER_PREFERENCE_2, \
    CUSTOMER_PREFERENCE_3, REGEX_IN_COLUMN
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

        user = user.drop(columns="birth_date")

        user = self.generate_customer_preference(user)



        return user

    def generate_customer_preference(
            self, user

    ):
        """
        customer preference
        """
        user[CUSTOMER_PREFERENCE_1] = user[CUSTOMER_PREFERENCE].str[0]
        user[CUSTOMER_PREFERENCE_2] = user[CUSTOMER_PREFERENCE].str[1]
        user[CUSTOMER_PREFERENCE_3] = user[CUSTOMER_PREFERENCE].str[2]

        user = user.drop(columns="customer_preferences")

        user = self.genrate_cluster_preference(user)

        return user

    def genrate_cluster_preference(
            self, user
    ):
        """"
        genrate has preference actor,director,
        genere,cluster_id
        """
        data, mean_user, model = ClusteringMain.controller()

        self.cls.write_df_to_pickle(
            object_name=f'{user_loader_path}mean_user.pkl',
            df_to_upload=mean_user)

        cluster_preference = data.filter(
            regex=REGEX_IN_COLUMN
        )

        cluster_preference[CUSTOMER_ID] = cluster_preference[CUSTOMER_ID].apply(str)

        user[CUSTOMER_ID] = user[CUSTOMER_ID].apply(str)

        customer_cluster_preference = merge(
            cluster_preference,
            user,
            how="outer",
            on=CUSTOMER_ID
        )
        for column in customer_cluster_preference.columns:
            if 'preference' in column or 'age' in column or 'cluster_id' in column:
                Logging.info("converting for {}".format(column))
                customer_cluster_preference[column] = customer_cluster_preference[column].astype(str)
                customer_cluster_preference[column] = customer_cluster_preference[column].apply(
                    lambda x: int(float(x)) if x != 'nan' else (-1 if 'preference' in column else -999)
                )
        return customer_cluster_preference

    def loader_csv(
            self,user
    ):
        """
        create loader csv and
         dump on the graph
        """
        Logging.info(
            f"Creating user loader csv ".center(100, "*")
        )

        user["~id"] = "user:" + user[CUSTOMER_ID]

        user['~label'] = USER

        [user.rename({col: col + ":String"}, axis=1, inplace=True)
         for col in user.columns if 'preference' in col]

        user = user.rename(USER_AN_RENAME,axis=1)

        self.cls.write_csv_to_s3(
            object_name=f'{user_loader_path}user_history.csv',
            df_to_upload=user)

        GenerateNode.create_node(
            key=f'{user_loader_path}user_history.csv'
        )

        Logging.info(
            f"Successfully dump {USER} on the graph ".center(100, "*")
        )

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






