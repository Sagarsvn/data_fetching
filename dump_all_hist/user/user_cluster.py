from config.config import user_path, user_loader_path
from config.constant_an import MEAN_USER, CLUSTER_ID, FROM, TO
from dump_all_hist.create_node import GenerateNode
from utils.logger import Logging
from utils.s3_service import S3Service


class UserCluster:

    def __init__(self):
        self.cls = S3Service.from_connection()

    def fetch_mean_user(self):
        mean_user = self.cls.read_pickles_from_s3(
            object_name=f'{user_path}mean_user.pkl')

        return mean_user

    def create_cluster(self):
        """
        create cluster
         node
        """
        cluster = self.fetch_mean_user()

        Logging.info(
            "Start clustering node".center(
                100, "*"
            )
        )

        cluster["~id"] = "cluster:" + cluster[CLUSTER_ID].astype(str)

        cluster['~label'] = "cluster"

        cluster = cluster.drop(columns=MEAN_USER).rename({CLUSTER_ID: 'cluster_id:Int'}, axis=1)

        self.cls.write_csv_to_s3(
            object_name=f'{user_loader_path}cluster.csv',
            df_to_upload=cluster)

        GenerateNode.create_node(
            key=f'{user_loader_path}cluster.csv'
        )

    def create_mean_user_relationship(self):
        """create mean user clustor node
         """

        user_cluster = self.fetch_mean_user()

        user_cluster[FROM] = "user:" + user_cluster[MEAN_USER].astype(str)

        user_cluster[TO] = "cluster:" + user_cluster[CLUSTER_ID].astype(str)

        user_cluster['~label'] = MEAN_USER.upper()

        user_cluster["~id"] = user_cluster[FROM] + '-' + user_cluster[TO]

        user_cluster = user_cluster.drop(columns=[MEAN_USER, CLUSTER_ID])

        self.cls.write_csv_to_s3(
            object_name=f'{user_loader_path}mean_cluster.csv',
            df_to_upload=user_cluster)

        GenerateNode.create_node(
            key=f'{user_loader_path}mean_cluster.csv'
        )

    def dump_user_cluster(self):
        Logging.info(
            "Start dumping cluster node".center(
                100, "*"
            )
        )
        self.create_cluster()

        Logging.info(
            "Start dumping user cluster node".center(
                100, "*"
            )
        )

        self.create_mean_user_relationship()

        Logging.info(
            "Suceessfully  update user cluster node".center(
                100, "*"
            )
        )
