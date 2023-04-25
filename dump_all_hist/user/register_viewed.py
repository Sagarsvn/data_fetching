import pandas as pd
from pandas import DataFrame, concat, merge

from rplus_ingestor.user.rating.implicit_rating import RatingGenerator
from rplus_utils import fetch_all_ubd

from config.config import content_loader_path, ubd_loader_path
from config.constant_an import CUSTOMER_ID, EPISODE, CSV, CLIP, \
    EXTRA, INNER, USER_CUSTOMER_REQUIRED, EPISODE_GRAPH_REQUIRED, EPISODE_GRAPH_RENAME, \
    VIEWED_REQUIRED, VIEWED_RENAME, CLIP_GRAPH_REQUIRED, CLIP_GRAPH_RENAME, EXTRA_GRAPH_REQUIRED, EXTRA_GRAPH_RENAME, \
    VIEWED, UBD_GROUP_BY, CONTENT_ID, PROGRAM, PROGRAM_GRAPH_REQUIRED, PROGRAM_GRAPH_RENAME, UBD_PROGRAM_GROUP_BY, \
    VIEW_FREQUENCY, WATCH_DURATION, CREATED_ON, IMPLICIT_RATING, UBD_REQUIRED_COLUMN, CLUSTERING_DATA_FILE_PATH, USER, \
    FROM
from dump_all_hist.update_node import UpdateNode
from dump_all_hist.user.common import get_view_counts, get_duration, get_created_on, get_groupby_implict_rating
from export_data.export_mongo import S3Connector

from utils.logger import Logging


class RegisterViewed:
    def __init__(self):
        self.s3_connector = S3Connector()

    def get_ubd(self):
        # extract the required column using loc instead of indexing with a list
        ubd = fetch_all_ubd(anonymous=False).loc[:, UBD_REQUIRED_COLUMN]
        return ubd


    def group_by(self,ubd):

        ubd_view_count = get_view_counts(ubd, UBD_GROUP_BY, VIEW_FREQUENCY)
        ubd_duration = get_duration(ubd, UBD_GROUP_BY, WATCH_DURATION)
        ubd_created_on = get_created_on(ubd, UBD_GROUP_BY, CREATED_ON)
        ubd_temp = ubd_view_count.merge(ubd_duration, on=UBD_GROUP_BY, how=INNER)
        ubd_ = ubd_temp.merge(ubd_created_on, on=UBD_GROUP_BY, how=INNER)
        return ubd_

    def map_customer_id(self, ubd: pd.DataFrame):
            """
            merge ubd with graph user
            to find vertex id
            """
            # use self.s3_connector in fetch_pickle_from_s3
            user_history = self.s3_connector.fetch_pickle_from_s3(
                object_name=CLUSTERING_DATA_FILE_PATH)[USER_CUSTOMER_REQUIRED]
            # use astype(str) instead of calling str on each value
            user_history[CUSTOMER_ID] = user_history[CUSTOMER_ID].astype(str)
            ubd[CUSTOMER_ID] = ubd[CUSTOMER_ID].astype(str)
            # use pandas merge method instead of importing merge function from pandas
            ubd_map = ubd.merge(user_history, how='inner', on=CUSTOMER_ID)
            ubd_map[FROM] = USER + ":" + ubd_map[CUSTOMER_ID]
            return ubd_map

    def get_program(self) -> pd.DataFrame:
        """get program from loader csv"""
        program_graph = self.s3_connector.fetch_csv_from_s3(
            object_name=f"{content_loader_path}{PROGRAM}{CSV}")
        # use loc instead of indexing with a list
        return program_graph.loc[:, PROGRAM_GRAPH_REQUIRED].rename(PROGRAM_GRAPH_RENAME, axis=1)

    def get_episode(self) -> pd.DataFrame:
        """get episode from loader csv"""
        episode_graph = self.s3_connector.fetch_csv_from_s3(
            object_name=f"{content_loader_path}{EPISODE}{CSV}")
        # use loc instead of indexing with a list
        return episode_graph.loc[:, EPISODE_GRAPH_REQUIRED].rename(EPISODE_GRAPH_RENAME, axis=1)

    def get_clip(self) -> pd.DataFrame:
        """get clip from loader csv"""
        clip_graph = self.s3_connector.fetch_csv_from_s3(
            object_name=f"{content_loader_path}{CLIP}{CSV}")
        # use loc instead of indexing with a list
        return clip_graph.loc[:, CLIP_GRAPH_REQUIRED].rename(CLIP_GRAPH_RENAME, axis=1)

    def get_extra(self) -> pd.DataFrame:
        """get extra from loader csv"""
        extra_graph = self.s3_connector.fetch_csv_from_s3(
            object_name=f"{content_loader_path}{EXTRA}{CSV}")
        # use loc instead of indexing with a list
        return extra_graph.loc[:, EXTRA_GRAPH_REQUIRED].rename(EXTRA_GRAPH_RENAME, axis=1)

    def map_content(self, ubd, content, key) -> pd.DataFrame:
        """
               Map content id to get form id
               """
        ubd = ubd[ubd["content_type"] == key]
        ubd = self.group_by(ubd)
        ubd[CONTENT_ID] = ubd[CONTENT_ID].astype(str)
        content["{}_id".format(key)] = content["{}_id".format(key)].astype(str)
        ubd_map = merge(
            ubd,
            content,
            how="inner",
            left_on=CONTENT_ID,
            right_on="{}_id".format(key)
        )
        ubd_map = ubd_map.rename({"watch_duration": "total_watch_duration"}, axis=1)
        ubd_ = RatingGenerator.calculate_implicit_rating(ubd_map, key)
        ubd_rating = ubd_["implicit_rating"]
        ubd_map = concat([ubd_map, ubd_rating], axis=1)
        ubd_map = ubd_map.rename({"total_watch_duration": "watch_duration"}, axis=1)
        return ubd_map

    def generate_mapping(self) -> DataFrame:
        """
        Mapping all content and customer with ubd
        """
        Logging.info("Fetch all ubd from S3".center(100, "*"))
        ubd = self.get_ubd()
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

    def prepare_program_data(self,ubd_program, program):
        """"
        Merge ubd programId to graph program to get vertex id
        """
        Logging.info("Preparing program implicit rating".center(100, "*"))
        ubd_program_implicit_rating = get_groupby_implict_rating(
            ubd_program, UBD_PROGRAM_GROUP_BY, IMPLICIT_RATING
        )

        Logging.info("Preparing program view frequency".center(100, "*"))
        ubd_program_view_count = get_view_counts(
            ubd_program, UBD_PROGRAM_GROUP_BY, VIEW_FREQUENCY
        )

        Logging.info("Preparing program created_on".center(100, "*"))
        ubd_program_created_on = get_created_on(
            ubd_program, UBD_PROGRAM_GROUP_BY, CREATED_ON
        )

        Logging.info("Preparing program watch duration".center(100, "*"))
        ubd_program_total_watch_duration = get_duration(
            ubd_program, UBD_PROGRAM_GROUP_BY, WATCH_DURATION
        )

        ubd_temp1 = ubd_program_implicit_rating.merge(
            ubd_program_view_count, on=UBD_PROGRAM_GROUP_BY, how="inner"
        )
        ubd_temp2 = ubd_temp1.merge(
            ubd_program_created_on, on=UBD_PROGRAM_GROUP_BY, how="inner"
        )
        ubd_program = ubd_temp2.merge(
            ubd_program_total_watch_duration, on=UBD_PROGRAM_GROUP_BY, how="inner"
        )

        Logging.info("Mapping program_id with user".center(100, "*"))

        ubd_program = merge(
            ubd_program, program, how="inner", on="{}_id".format(PROGRAM)
        )
        return ubd_program

    def dump_viewed_data(self,viewed)-> pd.DataFrame:
        """
        Dump viewed on the graph
        """
        viewed = viewed[VIEWED_REQUIRED].rename(VIEWED_RENAME, axis=1)
        viewed["~id"] = viewed["~from"] + "-" + viewed["~to"]
        viewed['~label'] = VIEWED

        Logging.info(f"Start preparing {VIEWED}{CSV}".center(100, "*"))

        s3_path = f"{ubd_loader_path}{VIEWED.lower()}{CSV}"
        self.s3_connector.store_csv_to_s3(object_name=s3_path,data= viewed)
        UpdateNode.update_node(key=s3_path)
        Logging.info(f"Successfully dumped {VIEWED} on the graph".center(100, "*"))

    def viewed_relationship(self):
        """
        Generate VIEWED relationships between users and content/program
        """
        Logging.info("Preparing user to content VIEWED relationship".center(100, "*"))
        ubd_content = self.generate_mapping()

        Logging.info("Preparing user to program VIEWED relationship".center(100, "*"))
        ubd_program = self.prepare_program_data(ubd_content, self.get_program())

        final_viewed = concat([ubd_content, ubd_program], ignore_index=True)

        Logging.info("Dumping VIEWED relationship".center(100, "*"))
        self.dump_viewed_data(final_viewed)


