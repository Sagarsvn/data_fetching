import uuid

from ast import literal_eval

from pandas import merge

from config.config import static_loader_path, user_loader_path
from config.constant_an import GENRE, CSV, USER_PREF_RENAME, GENRE_RENAME, \
    GENRE_REQUIRED, GENRE_ID, USER_PREF_REQUIRED, INNER, HAS_PREFERENCE_1, \
    HAS_PREFERENCE_2, HAS_PREFERENCE_3, FROM, CUSTOMER_PREFERENCE
from dump_all_hist.create_node import GenerateNode
from utils.logger import Logging

from utils.s3_service import S3Service


class HasPreferenceNode:
    def __init__(
            self
    ):
        self.cls = S3Service.from_connection()

    def fetch_customer_preference(
            self
    ):
        """
        fetch data
        from s3
        """
        try:
            user_pref = self.cls.read_csv_from_s3(
                object_name=f'{user_loader_path}{CUSTOMER_PREFERENCE}{CSV}')

            Logging.info(f"Start exploding {GENRE_ID}".center(100, '*'))

            user_pref = user_pref.rename(
                USER_PREF_RENAME, axis=1
            )

            user_pref = user_pref[
                USER_PREF_REQUIRED
            ]

            user_pref[GENRE_ID] = user_pref[GENRE_ID].apply(literal_eval)

            return user_pref

        except Exception as e:
            Logging.error(f"Unable to fetch customer pereference file from s3 {str(e)}")

    def fetch_genre(
            self
    ):
        """
        fetch genre
        from s3
        """
        try:
            genre = self.cls.read_csv_from_s3(
                object_name=f'{static_loader_path}{GENRE}{CSV}')

            genre = genre.rename(
                GENRE_RENAME, axis=1
            )
            genre = genre[
                GENRE_REQUIRED
            ]
            return genre
        except:
            Logging.error("Unable to fetch genre file from s3")

    def dump_has_preference(
            self,
            has_pereference,
            label
    ):
        """
        merging customer
        preference with genre
        & dump HAS_PREFERENCE ON
        the Graph Network
        """
        try:

            genre = HasPreferenceNode(

            ).fetch_genre(

            )

            Logging.info(f"Merging on  {GENRE_ID}".center(
                100, '*')
            )
            has_preference = merge(
                has_pereference,
                genre,
                how=INNER,
                right_on=GENRE_ID,
                left_on=label
            )

            has_preference = has_preference.drop(
                [GENRE_ID, label], axis="columns"
            )

            Logging.info(
                f"Start preparing {label}{CSV}".center(
                    100, "*"
                )
            )

            has_preference["~id"] = has_preference.apply(
                lambda _: str(uuid.uuid4()), axis=
                1)

            has_preference['~label'] = label.upper()

            self.cls.write_csv_to_s3(
                object_name=f'{user_loader_path}{label}{CSV}',
                df_to_upload=has_preference
            )

            GenerateNode.create_node(
                key=f'{user_loader_path}{label}{CSV}'
            )

            Logging.info(
                f"Successfully dump {label} on the graph ".center(
                    100, "*"
                )
            )

        except Exception as e:
            Logging.error(f"Unable to dum {label},{str(e)}")

    def has_preference(self
                       ):
        user_pref = self.fetch_customer_preference()
        user_pref[HAS_PREFERENCE_1.lower()] = user_pref[GENRE_ID].str[0]
        user_pref[HAS_PREFERENCE_2.lower()] = user_pref[GENRE_ID].str[1]
        user_pref[HAS_PREFERENCE_3.lower()] = user_pref[GENRE_ID].str[2]

        user_pref = user_pref.drop(GENRE_ID, axis="columns")

        Logging.info("Start dumping HAS_PREFERENCE_1 on Graph".center(100, "*"))
        self.dump_has_preference(
            user_pref[[FROM, HAS_PREFERENCE_1]], label=HAS_PREFERENCE_1
        )

        Logging.info("Start dumping HAS_PREFERENCE_2 on Graph".center(100, "*"))
        self.dump_has_preference(
            user_pref[[FROM, HAS_PREFERENCE_2]], label=HAS_PREFERENCE_2
        )

        Logging.info("Start dumping HAS_PREFERENCE_3 on Graph".center(100, "*"))

        self.dump_has_preference(
            user_pref[[FROM, HAS_PREFERENCE_3]], label=HAS_PREFERENCE_3
        )
