import pandas as pd
from rplus_ingestor.user.preprocessing.user import PreprocessUser
from rplus_offline_result.user_clustering.main import ClusteringMain
from config.config import user_path, user_loader_path
from config.constant_an import USER_AN_RENAME, USER, CUSTOMER_PREFERENCE, CUSTOMER_ID, CUSTOMER_PREFERENCE_1, CUSTOMER_PREFERENCE_2, \
    CUSTOMER_PREFERENCE_3, REGEX_IN_COLUMN
from dump_all_hist.update_node import UpdateNode

from export_data.export_mongo import S3Connector
from utils.logger import Logging



class DumpUser:
    def __init__(self):
        self.s3_connector = S3Connector()
        self.graph_node = UpdateNode()

    def fetch_data(self):
        """
        Fetch customer raw data either from Redis, S3 or local.
        """
        Logging.info("Fetching customer raw data".center(100, "*"))
        try:
            user = self.s3_connector.fetch_pickle_from_s3(
                object_name=f'{user_path}customer.pkl'
            )
        except:
            user = self.s3_connector.fetch_pickle_from_s3(
                object_name=f'{user_path}customer.pkl'
            )
        return user

    def preprocessing_user(self, user):
        """
        Preprocess user data by dropping unwanted columns, generating customer preference,
        and clustering them.
        """
        Logging.info("Start preprocessing".center(100, "*"))

        user = PreprocessUser().controller(user)
        user = user.drop(columns=["birth_date"])
        user = self.generate_customer_preference(user)
        return user

    def generate_customer_preference(self, user):
        """
        Generate customer preference columns by splitting the preference column
        into 3 separate columns and dropping the original preference column.
        """
        user[CUSTOMER_PREFERENCE_1] = user[CUSTOMER_PREFERENCE].str[0]
        user[CUSTOMER_PREFERENCE_2] = user[CUSTOMER_PREFERENCE].str[1]
        user[CUSTOMER_PREFERENCE_3] = user[CUSTOMER_PREFERENCE].str[2]

        user = user.drop(columns=["customer_preferences"])
        user = self.generate_cluster_preference(user)
        return user

    def generate_cluster_preference(self, user):
        """
        Generate customer preference columns by
        clustering and merging with user data.
        """
        data, mean_user, model = ClusteringMain.controller()

        self.s3_connector.store_pickle_to_s3(
            object_name=f'{user_loader_path}mean_user.pkl',
            data=mean_user
        )

        cluster_preference = data.filter(regex=REGEX_IN_COLUMN)
        cluster_preference[CUSTOMER_ID] = cluster_preference[CUSTOMER_ID].apply(str)
        user[CUSTOMER_ID] = user[CUSTOMER_ID].apply(str)

        customer_cluster_preference = pd.merge(
            cluster_preference,
            user,
            how="outer",
            on=CUSTOMER_ID
        )

        for column in customer_cluster_preference.columns:
            if 'preference' in column or 'age' in column or 'cluster_id' in column:
                Logging.info(f"Converting {column} to int")
                customer_cluster_preference[column] = customer_cluster_preference[column].astype(str)
                customer_cluster_preference[column] = customer_cluster_preference[column].apply(
                    lambda x: int(float(x)) if x != 'nan' else (-1 if 'preference' in column else -999)
                )
        return customer_cluster_preference

    def create_user_loader_csv(self, user):
        """
        Create a loader CSV file for the user and dump it on the graph.
        """

        Logging.info(f"Creating user loader CSV".center(100, "*"))

        # Add ID and label columns to the user DataFrame
        user["~id"] = "user:" + user[CUSTOMER_ID]
        user['~label'] = USER

        # Rename preference columns to include the ":String" suffix
        for col in user.columns:
            if 'preference' in col:
                user.rename({col: col + ":String"}, axis=1, inplace=True)

        # Rename the columns to the desired names
        user = user.rename(USER_AN_RENAME, axis=1)

        # Write the CSV file to S3
        object_name = f'{user_loader_path}user_history.csv'
        self.s3_connector.store_pickle_to_s3(object_name, user)

        # Update the node in the Neptune graph
        UpdateNode.update_node(key=object_name)

        Logging.info(f"Successfully dumped {USER} on the graph".center(100, "*"))

    def dump_user_on_graph(self):
        # Fetch the data
        user = self.fetch_data()

        # Preprocess the user data
        user = self.preprocessing_user(user)

        # Create the loader CSV file and dump it on the graph
        self.create_user_loader_csv(user)