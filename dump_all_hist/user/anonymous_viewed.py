from pandas import DataFrame, concat, merge

from rplus_ingestor.user.rating.implicit_rating import  RatingGenerator
from rplus_utils import fetch_all_ubd

from config.config import  content_loader_path, user_loader_path, ubd_loader_path
from config.constant_an import PKL, CUSTOMER_ID, EPISODE, CSV, CLIP, \
    EXTRA, INNER, EPISODE_GRAPH_REQUIRED, EPISODE_GRAPH_RENAME, \
    VIEWED_REQUIRED, VIEWED_RENAME, CLIP_GRAPH_REQUIRED, CLIP_GRAPH_RENAME, EXTRA_GRAPH_REQUIRED, EXTRA_GRAPH_RENAME, \
    VIEWED, UBD_GROUP_BY, CONTENT_ID, USER, DEFAULT_CLUSTER_ID, VIEW_FREQUENCY, WATCH_DURATION, CREATED_ON, \
    UBD_PROGRAM_GROUP_BY, PROGRAM, PROGRAM_GRAPH_RENAME, PROGRAM_GRAPH_REQUIRED, IMPLICIT_RATING, UBD_REQUIRED_COLUMN, \
    PERCENTAGE_COMPLETE
from dump_all_hist.create_node import GenerateNode
from dump_all_hist.update_node import UpdateNode
from dump_all_hist.user.common import get_view_counts, get_duration, get_created_on, get_groupby_implict_rating, \
    get_perecentage_complete
from export_data.export_mongo import S3Connector

from utils.logger import Logging



class AnonymousViewed:

    def __init__(self):
        self.s3_connector = S3Connector()


    def create_anonymous_user_node(
            self
    ):
        property = {"customer_id:String": "anonymous",
                    "age:Int": 30,
                    "customer_status:String": "activated",
                    "customer_created_on:String": "2023-01-01T00:00:00+07:00",
                    "customer_updated_on:String": "2023-01-01T00:00:00+07:00",
                    "gender:String": "nan",
                    "cluster_id:Int": DEFAULT_CLUSTER_ID
                    }

        anonymous_user = DataFrame([property])


        anonymous_user["~id"] = "user:" + anonymous_user["customer_id:String"]

        anonymous_user['~label'] = USER

        self.s3_connector.store_csv_to_s3(
            object_name=f'{user_loader_path}anonymous_user_history{CSV}',
            data=anonymous_user
        )

        GenerateNode.create_node(
            key=f'{user_loader_path}anonymous_user_history{CSV}'
        )

    def fetch_anonymous_ubd(self):
        ubd = fetch_all_ubd(anonymous=True)
        ubd = ubd[UBD_REQUIRED_COLUMN]
        return ubd


    @staticmethod
    def groupby(ubd):
        ubd_view_count = get_view_counts(ubd, UBD_GROUP_BY, VIEW_FREQUENCY)
        ubd_duration = get_duration(ubd, UBD_GROUP_BY, WATCH_DURATION)
        ubd_created_on = get_created_on(ubd, UBD_GROUP_BY, CREATED_ON)
        ubd_percentage_complete = get_perecentage_complete(ubd,UBD_GROUP_BY,PERCENTAGE_COMPLETE)
        ubd_temp = ubd_view_count.merge(ubd_duration, on=UBD_GROUP_BY, how=INNER)
        ubd_ = ubd_temp.merge(ubd_created_on, on=UBD_GROUP_BY, how=INNER)
        ubd_ = ubd_.merge(ubd_percentage_complete, on=UBD_GROUP_BY, how=INNER)
        return ubd_

    def map_customer_id(self,ubd: DataFrame):
        """
        merge ubd with grpah user
        to find vertex id
        """
        ubd[CUSTOMER_ID] = 'anonymous'
        ubd['~from'] = 'user:anonymous'

        return ubd

    def get_program(
            self
    ) -> DataFrame:
        """get program from
            loader csv
        """
        program_graph = self.s3_connector.fetch_csv_from_s3(
            object_name=f'{content_loader_path}{PROGRAM}{CSV}')

        return program_graph[PROGRAM_GRAPH_REQUIRED]. \
            rename(PROGRAM_GRAPH_RENAME, axis=1)

    def get_episode(
            self
    ) -> DataFrame:
        """
        get episode from
        loader csv
        """
        episode_graph = self.s3_connector.fetch_csv_from_s3(
            object_name=f'{content_loader_path}{EPISODE}{CSV}')

        return episode_graph[EPISODE_GRAPH_REQUIRED]. \
            rename(EPISODE_GRAPH_RENAME, axis=1)

    def get_clip(
            self
    ) -> DataFrame:
        """get clip from
              loader csv
              """
        clip_graph = self.s3_connector.fetch_csv_from_s3(
            object_name=f'{content_loader_path}{CLIP}{CSV}')
        return clip_graph[CLIP_GRAPH_REQUIRED]. \
            rename(CLIP_GRAPH_RENAME, axis=1)

    def get_extra(
            self
    ) -> DataFrame:
        """get extra from
            loader csv
        """
        extra_graph = self.s3_connector.fetch_csv_from_s3(
            object_name=f'{content_loader_path}{EXTRA}{CSV}')

        return extra_graph[EXTRA_GRAPH_REQUIRED]. \
            rename(EXTRA_GRAPH_RENAME, axis=1)

    def map_content(
            self,
            ubd,
            content,
            key
    ) -> DataFrame:
        """map content id to
        get vertex id
        """
        ubd = ubd[ubd["content_type"] == key]

        ubd = self.groupby(ubd)

        ubd[CONTENT_ID] = ubd[CONTENT_ID].astype(str)

        content["{}_id".format(key)] = content["{}_id".format(key)].astype(str)
        ubd_map = merge(
            ubd,
            content,
            how=INNER,
            left_on=CONTENT_ID,
            right_on="{}_id".format(key))

        ubd_map = ubd_map.rename({"watch_duration": "total_watch_duration"}, axis=1)

        ubd_ = RatingGenerator.calculate_implicit_rating(ubd_map, key)

        ubd_rating = ubd_['implicit_rating']

        ubd_map = concat([ubd_map, ubd_rating], axis=1)

        ubd_map = ubd_map.rename({"total_watch_duration": "watch_duration"}, axis=1)

        return ubd_map

    def dump_viewed(self,viewed):
        """
        dump viwed on the graph
        """
        viewed = viewed[VIEWED_REQUIRED].\
            rename(VIEWED_RENAME, axis=1)
        Logging.info(
            f"Start preparing {VIEWED}{CSV}".center(
                100, "*"
            )
        )

        viewed["~id"] = viewed["~from"] + "-" + viewed["~to"]

        viewed['~label'] = VIEWED

        self.s3_connector.store_csv_to_s3(
            object_name=f'{ubd_loader_path}anoymous_user_{VIEWED.lower()}{CSV}',
            data=viewed
        )

        UpdateNode.update_node(
            key=f'{ubd_loader_path}anoymous_user_{VIEWED.lower()}{CSV}'
        )

        Logging.info(
            f"Successfully dump {VIEWED} on the graph ".center(
                100, "*"
            )
        )

    def genrate_mapping(self):
        """Mapping all content and customer with ubd
        """
        Logging.info("Fetch all ubd from S3".center(100, "*"))
        ubd = self.fetch_anonymous_ubd()
        Logging.info("Mapping of user with vertex_id".center(100, "*"))
        ubd_map = self.map_customer_id(ubd)
        Logging.info("Get episode".center(100, "*"))
        episode = self.get_episode()
        Logging.info("Mapping content_id with episode".center(100, "*"))
        ubd_episode = self.map_content(ubd_map, episode, EPISODE)
        Logging.info("Mapping clip".center(100, "*"))
        clip = self.get_clip()
        Logging.info("Mapping content_id with clip".center(100, "*"))
        ubd_clip = self.map_content(ubd_map, clip, CLIP)
        Logging.info("Fetching extra".center(100, "*"))
        extra = self.get_extra()
        Logging.info("Mapping content_id with extra".center(100, "*"))
        ubd_extra = self.map_content(ubd_map, extra, EXTRA)
        Logging.info("Join user behaviour data ".center(100, "*"))
        final_viewed = concat([ubd_episode, ubd_clip, ubd_extra], ignore_index=True)
        return final_viewed

    def ubd_program(self,
                    final_viwed,
                    program
                    ):
        """"
        group by all data over programId
        and customerId
        """

        ubd_program = final_viwed.drop("~to", axis=1)

        Logging.info(
            "Preparing program  implicit rating".center(100, "*")
        )

        ubd_program_implicit_rating = get_groupby_implict_rating(
            ubd_program, UBD_PROGRAM_GROUP_BY, IMPLICIT_RATING
        )

        Logging.info(
            "Preparing program view frequency ".center(100, "*")
        )

        ubd_program_view_count = get_view_counts(

            ubd_program, UBD_PROGRAM_GROUP_BY, VIEW_FREQUENCY
        )

        Logging.info(
            "Preparing program created_on ".center(100, "*")
        )

        ubd_program_created_on = get_created_on(
            ubd_program, UBD_PROGRAM_GROUP_BY, CREATED_ON
        )

        Logging.info(
            "Preparing program watch_duration ".center(100, "*")
        )

        ubd_program_total_watch_duration = get_duration(
            ubd_program, UBD_PROGRAM_GROUP_BY, WATCH_DURATION

        )
        ubd_percentage_complete = get_perecentage_complete(
            ubd_program, UBD_PROGRAM_GROUP_BY, PERCENTAGE_COMPLETE)

        ubd_temp1 = ubd_program_implicit_rating.merge(
            ubd_program_view_count, on=UBD_PROGRAM_GROUP_BY, how=INNER
        )
        ubd_temp2 = ubd_temp1.merge(
            ubd_program_created_on, on=UBD_PROGRAM_GROUP_BY, how=INNER
        )

        ubd_temp3 = ubd_temp2.merge(
            ubd_program_total_watch_duration, on=UBD_PROGRAM_GROUP_BY, how=INNER
        )
        ubd_ = ubd_temp3.merge(ubd_percentage_complete, on=UBD_PROGRAM_GROUP_BY, how="inner"
                                      )

        ubd_program = merge(
            ubd_,
            program,
            how=INNER,
            on="{}_id".format(PROGRAM)
        )
        return ubd_program

    def viewed_relationship(
            self
    ):
        """
        dump registered user viewed relationship
        """

        Logging.info(
            "creating anonymous user on graph ".center(100, "*")
        )

        self.create_anonymous_user_node()

        Logging.info(
            "mapping all ubd and content".center(100, "*")
        )
        ubd_content = self.genrate_mapping()

        Logging.info("dumping program VIEWED relationship".center(100, "*"))

        ubd_program = self.ubd_program(ubd_content, self.get_program())

        final_viewed = concat([ubd_content, ubd_program], ignore_index=True)

        Logging.info(
            "dumping VIEWED relationship".center(100, "*")
        )

        self.dump_viewed(final_viewed)

