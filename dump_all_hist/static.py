import uuid

from config.config import Config, static_loader_path, static_path
from config.constant_an import ACTOR_AN_RENAME, ACTOR, WRITER, DIRECTOR, GENRE, WRITER_AN_RENAME, DIRECTOR_AN_RENAME, \
    GENRE_AN_RENAME
from dump_all_hist.create_node import GenerateNode
from utils.s3_service import S3Service


class DumpStatic:

    def __init__(self):
        self.cls = S3Service.from_connection()

    def actor(self):
        """
        fetch actor and dump into
        neptune graph
        """

        actor = self.cls.read_pickles_from_s3(object_name=static_path + ACTOR + '.pkl')

        actor = actor.rename(ACTOR_AN_RENAME, axis=1)

        actor["~id"] = actor.apply(lambda _: str(uuid.uuid4()), axis=1)

        actor['~label'] = ACTOR
        # total length
        print("total record of actor",len(actor))

        self.cls.write_csv_to_s3(object_name=static_loader_path + ACTOR + '.csv',
                                 df_to_upload=actor)
        print("Successfully convert actor csv and put into S3".center(100, "*"))

        GenerateNode.create_node(key=static_loader_path + ACTOR + '.csv')

    def writer(self):
        """
        fetch writer and dump into
        neptune graph
        """

        writer = self.cls.read_pickles_from_s3(object_name=static_path + WRITER + '.pkl')

        writer = writer.rename(WRITER_AN_RENAME, axis=1)

        writer["~id"] = writer.apply(lambda _: str(uuid.uuid4()), axis=1)

        writer['~label'] = WRITER

        # total length of writer

        print("total record of writer", len(writer))

        self.cls.write_csv_to_s3(object_name=static_loader_path + WRITER + '.csv',
                                 df_to_upload=writer)
        print("Successfully convert actor csv and put into S3".center(100, "*"))

        GenerateNode.create_node(key=static_loader_path + WRITER + '.csv')

    def director(self):
        """
        fetch writer and dump into
        neptune graph
        """

        director = self.cls.read_pickles_from_s3(object_name=static_path + DIRECTOR + '.pkl')

        director = director.rename(DIRECTOR_AN_RENAME, axis=1)

        director["~id"] = director.apply(lambda _: str(uuid.uuid4()), axis=1)

        director['~label'] = DIRECTOR

        # total length of director

        print("total record of director", len(director))

        self.cls.write_csv_to_s3(object_name=static_loader_path + DIRECTOR + '.csv',
                                 df_to_upload=director)
        print("Successfully convert actor csv and put into S3".center(100, "*"))

        GenerateNode.create_node(key=static_loader_path + DIRECTOR + '.csv')

    def genre(self):
        """
        fetch writer and dump into
        neptune graph
        """

        genre = self.cls.read_pickles_from_s3(object_name=static_path + GENRE + '.pkl')

        genre = genre.rename(GENRE_AN_RENAME, axis=1)

        genre["~id"] = genre.apply(lambda _: str(uuid.uuid4()), axis=1)

        genre['~label'] = GENRE

        # total length of genre

        print("total record of genre", len(genre))

        self.cls.write_csv_to_s3(object_name=static_loader_path + GENRE + '.csv',
                                 df_to_upload=genre)
        print("Successfully convert actor csv and put into S3".center(100, "*"))

        GenerateNode.create_node(key=static_loader_path + GENRE + '.csv')

    def dump_static_node(self):
        """
        dump all static node
        on graph
        """
        print("start dumping actor data".center(100, "*"))

        DumpStatic().actor()

        print("start dumping writer data".center(100, "*"))

        DumpStatic().writer()

        print("start dumping director data".center(100, "*"))

        DumpStatic().director()

        print("start dumping genre data".center(100, "*"))

        DumpStatic().genre()


