import uuid

import pandas as pd
from pandas import DataFrame, concat, merge, read_csv
from rplus_ingestor.user.preprocessing.ubd import ubd_data_preprocessing
from rplus_ingestor.user.rating.implicit_rating  import RegisterUserRating

from config.config import ubd_path, content_loader_path, user_loader_path, ubd_loader_path, registered_ubd_start_month
from config.constant_an import PKL, CUSTOMER_ID, EPISODE, CSV, CLIP, \
    EXTRA, INNER, USER_CUSTOMER_RENAME, USER_CUSTOMER_REQUIRED, EPISODE_GRAPH_REQUIRED, EPISODE_GRAPH_RENAME, \
    VIEWED_REQUIRED, VIEWED_RENAME, CLIP_GRAPH_REQUIRED, CLIP_GRAPH_RENAME, EXTRA_GRAPH_REQUIRED, EXTRA_GRAPH_RENAME, \
    VIEWED, UBD_GROUP_BY, CONTENT_ID, PROGRAM, PROGRAM_GRAPH_REQUIRED, PROGRAM_GRAPH_RENAME, UBD_PROGRAM_GROUP_BY, \
    VIEW_FREQUENCY, WATCH_DURATION, CREATED_ON, IMPLICIT_RATING
from dump_all_hist.create_node import GenerateNode
from dump_all_hist.user.common import get_view_counts, get_duration, get_created_on, get_groupby_implict_rating

from utils.logger import Logging
from utils.s3_service import S3Service


class RegisterViewed:

    def __init__(self):
        self.cls = S3Service.from_connection()

    def get_ubd(self):
        ubd_combine = DataFrame()

        for ubd_record in registered_ubd_start_month:
            ubd = self.cls.read_pickles_from_s3(
                object_name=f"{ubd_path}registered_ubd_{ubd_record.replace('-', '')}{PKL}")
            ubd = ubd[ubd[CUSTOMER_ID].str.isnumeric()]
            ubd_combine = concat([ubd_combine, ubd])

        return ubd_combine

    @staticmethod
    def preprocessing_ubd(
            ubd
    ):
        """
            preprocessing of all ubd
            return view count and
            duration and created_on
            """
        ubd = ubd_data_preprocessing(
            ubd
        )
        return ubd

    @staticmethod
    def groupby(
            ubd
    ):
        ubd_view_count = get_view_counts(
            ubd, UBD_GROUP_BY, VIEW_FREQUENCY
        )
        ubd_duration = get_duration(
            ubd, UBD_GROUP_BY, WATCH_DURATION
        )
        ubd_created_on = get_created_on(
            ubd, UBD_GROUP_BY, CREATED_ON
        )
        ubd_temp = ubd_view_count.merge(
            ubd_duration, on=UBD_GROUP_BY, how=INNER
        )
        ubd_ = ubd_temp.merge(
            ubd_created_on, on=UBD_GROUP_BY, how=INNER
        )

        return ubd_

    def map_customer_id(
            self,
            ubd: DataFrame):
        """
        merge ubd with grpah user
        to find vertex id
        """

        user_history = self.cls.read_csv_from_s3(
            object_name=f'{user_loader_path}user_history.csv')
        user_history = user_history[USER_CUSTOMER_REQUIRED]. \
            rename(USER_CUSTOMER_RENAME, axis=1)
        user_history[CUSTOMER_ID] = user_history[CUSTOMER_ID].astype(str)
        ubd[CUSTOMER_ID] = ubd[CUSTOMER_ID].astype(str)
        ubd_map = merge(
            ubd,
            user_history,
            how=INNER,
            left_on=CUSTOMER_ID,
            right_on=CUSTOMER_ID)

        return ubd_map

    def get_program(
            self
    ) -> DataFrame:
        """get program from
            loader csv
        """
        program_graph = self.cls.read_csv_from_s3(
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
        episode_graph = self.cls.read_csv_from_s3(
            object_name=f'{content_loader_path}{EPISODE}{CSV}')

        return episode_graph[EPISODE_GRAPH_REQUIRED]. \
            rename(EPISODE_GRAPH_RENAME, axis=1)

    def get_clip(
            self
    ) -> DataFrame:
        """get clip from
              loader csv
              """
        clip_graph = self.cls.read_csv_from_s3(
            object_name=f'{content_loader_path}{CLIP}{CSV}')
        return clip_graph[CLIP_GRAPH_REQUIRED]. \
            rename(CLIP_GRAPH_RENAME, axis=1)

    def get_extra(
            self
    ) -> DataFrame:
        """get extra from
            loader csv
        """
        extra_graph = self.cls.read_csv_from_s3(
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
        get form id
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

        ubd_map = RegisterUserRating().calculate_rating(
            ubd_map, "registered_user"
        )

        return ubd_map

    def genrate_mapping(
            self
    ):
        """
        mapping all content and
        customer with ubd
        """
        Logging.info(
            "fetch all ubd from S3".center(100, "*")
        )
        ubd = self.get_ubd()

        Logging.info(
            "preprocessing of ubd".center(100, "*")
        )

        ubd_ = self.preprocessing_ubd(ubd)

        Logging.info(
            "mapping of user with vertex_id".center(100, "*")
        )
        ubd_map = self.map_customer_id(ubd_)

        Logging.info(
            "get episode".center(100, "*")
        )
        episode = self.get_episode()

        Logging.info(
            "mapping content_id with episode".center(100, "*")
        )

        ubd_episode = self.map_content(ubd_map, episode, EPISODE)

        Logging.info(
            "mapping clip".center(100, "*")
        )

        clip = self.get_clip()

        Logging.info(
            "mapping content_id with clip".center(100, "*")
        )
        ubd_clip = self.map_content(ubd_map, clip, CLIP)

        Logging.info(
            "fetching extra".center(100, "*")
        )

        extra = self.get_extra()

        Logging.info(
            "mapping content_id with extra".center(100, "*")
        )

        ubd_extra = self.map_content(ubd_map, extra, EXTRA)

        Logging.info(
            "join user behaviour data ".center(100, "*")
        )

        final_viewed = concat([ubd_episode, ubd_clip, ubd_extra], ignore_index=True)

        return final_viewed

    def ubd_program(self,
                    final_viewed,
                    program
                    ):
        """"
        merge ubd programId to
        graph program to
        get vertex id
        """

        ubd_program = final_viewed.drop("~to", axis=1)

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

            ubd_program, UBD_PROGRAM_GROUP_BY,VIEW_FREQUENCY
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
        ubd_temp1 = ubd_program_implicit_rating.merge(
            ubd_program_view_count, on=UBD_PROGRAM_GROUP_BY, how=INNER
        )
        ubd_temp2 = ubd_temp1.merge(
            ubd_program_created_on, on=UBD_PROGRAM_GROUP_BY, how=INNER
        )

        ubd_ = ubd_temp2.merge(
            ubd_program_total_watch_duration, on=UBD_PROGRAM_GROUP_BY, how=INNER
        )

        Logging.info(
            "mapping progrma_id with user".center(100, "*")
        )

        ubd_program = merge(
            ubd_,
            program,
            how=INNER,
            on="{}_id".format(PROGRAM)
        )
        return ubd_program

    def dump_viewed(self,
                    viewed
                    ):
        """
        dump viwed on the graph
        """

        viewed = viewed[VIEWED_REQUIRED].rename(VIEWED_RENAME, axis=1)
        Logging.info(
            f"Start preparing {VIEWED}{CSV}".center(
                100, "*"
            )
        )

        viewed["~id"] = viewed.apply(
            lambda _: str(uuid.uuid4()), axis=
            1)

        viewed['~label'] = VIEWED

        self.cls.write_csv_to_s3(
            object_name=f'{ubd_loader_path}{VIEWED.lower()}{CSV}',
            df_to_upload=viewed
        )

        GenerateNode.create_node(
            key=f'{ubd_loader_path}{VIEWED.lower()}{CSV}'
        )

        Logging.info(
            f"Successfully dump {VIEWED} on the graph ".center(
                100, "*"
            )
        )

    def viewed_relationship(
            self
    ):
        Logging.info("Preparing  user to content VIEWED relationship".center(100, "*"))
        ubd_content = self.genrate_mapping()

        Logging.info("Preparing user to program VIEWED relationship".center(100, "*"))

        ubd_program = self.ubd_program(ubd_content, self.get_program())

        final_viewed = concat([ubd_content, ubd_program], ignore_index=True)

        Logging.info(
            "dumping VIEWED relationship".center(100, "*")
        )

        self.dump_viewed(final_viewed)


RegisterViewed().viewed_relationship()
