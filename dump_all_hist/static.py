import uuid

from config.config import static_loader_path, static_path
from config.constant_an import ACTOR_AN_RENAME, ACTOR, WRITER, DIRECTOR, GENRE, WRITER_AN_RENAME, DIRECTOR_AN_RENAME, \
    GENRE_AN_RENAME
from dump_all_hist.create_node import GenerateNode
from export_data.export_bulk_content import fetch_actor, fetch_writers, fetch_directors, fetch_genre
from utils.logger import Logging
from utils.s3_service import S3Service


class DumpStatic:

    def __init__(self):
        self.cls = S3Service.from_connection()

    def actor(self):
        """
        fetch actor and dump into
        neptune graph
        """
        try:
            Logging.info("Fetching actor".center(100, "*"))
            try:
                actor = fetch_actor()
            except:
                actor = self. \
                    cls.read_pickles_from_s3(object_name=static_path + ACTOR + '.pkl')

            actor = actor.rename(ACTOR_AN_RENAME, axis=1)

            actor["~id"] = actor.apply(lambda _: str(uuid.uuid4()), axis=1)

            actor['~label'] = ACTOR
            # total length
            Logging.info(f"total record of actor {len(actor)}")

            self.cls.write_csv_to_s3(object_name=static_loader_path + ACTOR + '.csv',
                                     df_to_upload=actor)

            GenerateNode.create_node(key=static_loader_path + ACTOR + '.csv')

        except Exception as e:

            Logging.error(f"unable to dump the {ACTOR} data on graph network,{str(e)}")

    def writer(self):
        """
        fetch writer and dump into
        neptune graph
        """
        try:

            Logging.info("Fetching writer  ".center(100, "*"))
            try:
                writer = fetch_writers()
            except:

                writer = self.cls.read_pickles_from_s3(object_name=
                                                       static_path + WRITER + '.pkl')

            writer = writer.rename(WRITER_AN_RENAME, axis=1)

            writer["~id"] = writer.apply(lambda _: str(uuid.uuid4()), axis=1)

            writer['~label'] = WRITER

            # total length of writer

            Logging.info("total record of writer {}".format(len(writer)))

            self.cls.write_csv_to_s3(object_name=static_loader_path + WRITER + '.csv',
                                     df_to_upload=writer)

            GenerateNode.create_node(key=static_loader_path + WRITER + '.csv')

        except Exception as e:

            Logging.error(f"unable to dump the {WRITER} data on graph network,{str(e)}")

    def director(self):
        """
        fetch writer and dump into
        neptune graph
        """

        try:
            Logging.info("Fetching director  ".center(100, "*"))

            try:
                director = fetch_directors()
            except:

                director = self.cls.read_pickles_from_s3(
                    object_name=static_path + DIRECTOR + '.pkl')

            director = director.rename(DIRECTOR_AN_RENAME, axis=1)

            director["~id"] = director.apply(lambda _: str(uuid.uuid4()), axis=1)

            director['~label'] = DIRECTOR

            # total length of director

            Logging.info("total record of director {}".format(len(director)))

            self.cls.write_csv_to_s3(object_name=static_loader_path + DIRECTOR + '.csv',
                                     df_to_upload=director)

            GenerateNode.create_node(key=static_loader_path + DIRECTOR + '.csv')

        except Exception as e:

            Logging.error(f"unable to dump the {DIRECTOR} data on graph network,{str(e)}")

    def genre(self):
        """
        fetch writer and dump into
        neptune graph
        """
        try:
            Logging.info("Fetching genre  ".center(100, "*"))

            try:
                genre = fetch_genre()

            except:

                genre = self.cls.read_pickles_from_s3(
                    object_name=static_path + GENRE + '.pkl')

            genre = genre.rename(GENRE_AN_RENAME, axis=1)

            genre["~id"] = genre.apply(lambda _: str(uuid.uuid4()), axis=1)

            genre['~label'] = GENRE

            # total length of genre

            Logging.info("total record of genre {}".format(len(genre)))

            self.cls.write_csv_to_s3(object_name=static_loader_path + GENRE + '.csv',
                                     df_to_upload=genre)

            GenerateNode.create_node(key=static_loader_path + GENRE + '.csv')

        except Exception as e:
            Logging.error(f"unable to dump the {GENRE} data on graph network,{str(e)}")

    @staticmethod
    def dump_static_node():
        """
        dump all static node
        on graph
        """

        DumpStatic().actor()

        DumpStatic().writer()

        DumpStatic().director()

        DumpStatic().genre()


DumpStatic().dump_static_node()