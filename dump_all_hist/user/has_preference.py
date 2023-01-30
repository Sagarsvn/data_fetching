import uuid

from ast import literal_eval

from pandas import merge

from config.config import static_loader_path, user_loader_path
from config.constant_an import GENRE, CSV, USER_PREF_RENAME, GENRE_RENAME, \
    GENRE_REQUIRED, HAS_PREFERENCE, GENRE_ID, USER_PREF_REQUIRED, INNER, CUSTOMER_PREFERENCE
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
            user_pref = self.cls.read_csv_to_s3(
                object_name=f'{user_loader_path}{CUSTOMER_PREFERENCE}{CSV}')

            Logging.info(f"Start exploding {GENRE_ID}".center(100, '*'))

            user_pref = user_pref.rename(
                USER_PREF_RENAME, axis=1
            )

            user_pref = user_pref[
                USER_PREF_REQUIRED
            ]

            user_pref[GENRE_ID] = user_pref[GENRE_ID].apply(literal_eval)

            return user_pref.explode(GENRE_ID)

        except:
            Logging.error("Unable to fetch customer pereference file from s3")

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
            self
    ):
        """
        merging customer
        preference with genre
        & dump HAS_PREFERENCE ON
        the Graph Network
        """
        try:
            user_pref = HasPreferenceNode(

            ).fetch_customer_preference(

            )

            genre = HasPreferenceNode(

            ).fetch_genre(

            )

            Logging.info(f"Merging on  {GENRE_ID}".center(
                100, '*')
            )
            has_preference = merge(
                user_pref,
                genre,
                how=INNER,
                on=GENRE_ID
            )

            has_preference = has_preference.drop(
                GENRE_ID, axis="columns"
            )

            Logging.info(
                f"Start preparing user_preference{CSV}".center(
                    100, "*"
                )
            )

            has_preference["~id"] = has_preference.apply(
                lambda _: str(uuid.uuid4()), axis=
                1)

            has_preference['~label'] = HAS_PREFERENCE

            self.cls.write_csv_to_s3(
                object_name=f'{user_loader_path}user_perferences.csv',
                df_to_upload=has_preference
            )

            GenerateNode.create_node(
                key=f'{user_loader_path}user_perferences.csv'
            )

            Logging.info(
                f"Successfully dump {HAS_PREFERENCE} on the graph ".center(
                    100, "*"
                )
            )

        except Exception as e:
            Logging.error(f"Unable to dum {HAS_PREFERENCE},{str(e)}")
