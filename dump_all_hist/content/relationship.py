import uuid
from ast import literal_eval

from pandas import merge

from config.config import content_loader_path, static_loader_path
from config.constant_an import CSV, ACTOR, INNER, ACTOR_DEPENDENCIES_RENAME, HAS_ACTOR, ACTOR_DEPENDENCIES_REQUIRED, \
    WRITER, WRITER_DEPENDENCIES_RENAME, WRITER_DEPENDENCIES_REQUIRED, HAS_WRITER, HAS_DIRECTOR, DIRECTOR, GENRE, \
    HAS_GENRE, GENRE_DEPENDENCIES_RENAME, GENRE_DEPENDENCIES_REQUIRED, DIRECTOR_DEPENDENCIES_REQUIRED, \
    DIRECTOR_DEPENDENCIES_RENAME, CLIP, CLIP_DEPENDENCIES_RENAME, CLIP_DEPENDENCIES_REQUIRED, HAS_CLIP, EPISODE, \
    EPISODE_DEPENDENCIES_RENAME, EPISODE_DEPENDENCIES_REQUIRED, HAS_EPISODE, EXTRA, HAS_EXTRA, \
    EXTRA_DEPENDENCIES_RENAME, EXTRA_DEPENDENCIES_REQUIRED
from dump_all_hist.create_node import GenerateNode
from utils.logger import Logging
from utils.s3_service import S3Service


class ProgramRelationship:
    def __init__(
            self
    ):
        self.cls = S3Service.from_connection()
        self.program_dependencies = self.cls.read_csv_from_s3(
            object_name=f'{content_loader_path}program_dependencies{CSV}')

    def has_actor(
            self):
        try:
            actor = self.cls.read_csv_from_s3(
                object_name=
                f'{static_loader_path}{ACTOR}{CSV}'
            )
            actor = actor.rename(
                ACTOR_DEPENDENCIES_RENAME,
                axis=1)[ACTOR_DEPENDENCIES_REQUIRED]

            ProgramRelationship().create_static_relationship(
                dependencies=actor,
                merge_on="actor_id",
                label=HAS_ACTOR
            )
        except Exception as e:
            Logging.info(f"Unable to process {HAS_ACTOR},{str(e)}")

    def has_writer(
            self):

        try:
            writer = self.cls.read_csv_from_s3(
                object_name=
                f'{static_loader_path}{WRITER}{CSV}'
            )
            writer = writer.rename(
                WRITER_DEPENDENCIES_RENAME,
                axis=1)[WRITER_DEPENDENCIES_REQUIRED]

            ProgramRelationship().create_static_relationship(
                dependencies=writer,
                merge_on="writer_id",
                label=HAS_WRITER
            )
        except Exception as e:
            Logging.error(f"Unable to process {HAS_WRITER},{str(e)}")

    def has_director(
            self):
        try:
            director = self.cls.read_csv_from_s3(
                object_name=
                f'{static_loader_path}{DIRECTOR}{CSV}'
            )
            director = director.rename(
                DIRECTOR_DEPENDENCIES_RENAME,
                axis=1)[DIRECTOR_DEPENDENCIES_REQUIRED]

            ProgramRelationship().create_static_relationship(
                dependencies=director,
                merge_on="director_id",
                label=HAS_DIRECTOR
            )
        except Exception as e:
            Logging.error(f"Unable to process {HAS_DIRECTOR},{str(e)}")

    def has_genre(
            self):
        try:
            genre = self.cls.read_csv_from_s3(
                object_name=
                f'{static_loader_path}{GENRE}{CSV}'
            )
            genre = genre.rename(
                GENRE_DEPENDENCIES_RENAME,
                axis=1)[GENRE_DEPENDENCIES_REQUIRED]

            ProgramRelationship().create_static_relationship(
                dependencies=genre,
                merge_on="genre_id",
                label=HAS_GENRE
            )
        except Exception as e:
            Logging.error(f"Unable to process {HAS_GENRE},{str(e)}")

    def create_static_relationship(
            self, dependencies, label, merge_on,):

        try:
            program_from = self.program_dependencies[['~id', merge_on]]. \
                rename({'~id': '~from'}, axis=1)

            program_from[merge_on] = \
                program_from[merge_on].apply(literal_eval)

            program_from = program_from. \
                explode(merge_on)

            has_dependencies = merge(
                program_from,
                dependencies,
                how=INNER,
                on=merge_on
            )
            has_dependencies = has_dependencies.drop(merge_on,
                                                     axis="columns"
                                                     )
        except Exception as e:
            Logging.error(f"Unable to merge {merge_on} dependencies,{str(e)}")

        try:

            has_dependencies["~id"] = has_dependencies["~from"] + '-' + has_dependencies['~to']

            has_dependencies['~label'] = label

            self.cls.write_csv_to_s3(
                object_name=f'{content_loader_path}{label.lower()}{CSV}',
                df_to_upload=has_dependencies)

            GenerateNode.create_node(
                key=f'{content_loader_path}{label.lower()}{CSV}'
            )
        except Exception as e:
            Logging.error(f"Unable to dump  {label},{str(e)}")

    def has_clip(
            self
    ):
        try:
            clip = self.cls.read_csv_from_s3(
                object_name=
                f'{content_loader_path}{CLIP}{CSV}'
            )
            clip = clip.rename(
                CLIP_DEPENDENCIES_RENAME,
                axis=1)[CLIP_DEPENDENCIES_REQUIRED]

            ProgramRelationship().create_content_relationship(
                dependencies=clip,
                merge_on="program_id",
                label=HAS_CLIP
            )
        except Exception as e:
            Logging.error(f"Unable to process  {CLIP},{str(e)}")

    def has_episode(
            self
    ):
        try:
            episode = self.cls.read_csv_from_s3(
                object_name=
                f'{content_loader_path}{EPISODE}{CSV}'
            )
            episode = episode.rename(
                EPISODE_DEPENDENCIES_RENAME,
                axis=1)[EPISODE_DEPENDENCIES_REQUIRED]

            ProgramRelationship().create_content_relationship(
                dependencies=episode,
                merge_on="program_id",
                label=HAS_EPISODE
            )
        except Exception as e:
            Logging.error(f"Unable to process  {EPISODE},{str(e)}")

    def has_extra(
            self
    ):
        try:
            extra = self.cls.read_csv_from_s3(
                object_name=
                f'{content_loader_path}{EXTRA}{CSV}'
            )
            extra = extra.rename(
                EXTRA_DEPENDENCIES_RENAME,
                axis=1)[EXTRA_DEPENDENCIES_REQUIRED]

            ProgramRelationship().create_content_relationship(
                dependencies=extra,
                merge_on="program_id",
                label=HAS_EXTRA
            )
        except Exception as e:

            Logging.error(f"Unable to process  {EXTRA},{str(e)}")

    def create_content_relationship(
            self, dependencies, label, merge_on
    ):
        try:
            program_from = self.program_dependencies[['~id', merge_on]]. \
                rename({'~id': '~from'}, axis=1)

            has_dependencies = merge(
                program_from,
                dependencies,
                how=INNER,
                on=merge_on
            )
            has_dependencies = has_dependencies.drop(merge_on,
                                                     axis="columns"
                                                     )

        except Exception as e:
            Logging.error(f"Unable to merge {merge_on} dependecies csv,{str(e)}")

        try:
            has_dependencies["~id"] = has_dependencies['~from'] + '-' + has_dependencies['~to']

            has_dependencies['~label'] = label

            self.cls.write_csv_to_s3(
                object_name=f'{content_loader_path}{label.lower()}{CSV}',
                df_to_upload=has_dependencies)

            GenerateNode.create_node(
                key=f'{content_loader_path}{label.lower()}{CSV}'
            )

            Logging.info(
                f"Successfully dump {label} on the graph ".center(
                    100, "*"
                )
            )

        except Exception as e:

            Logging.error(f"Unable to dump  {label},{str(e)}")

    def dump_relationship(
            self
    ):
        """
        Dump
        has_actor
        has_genre
        has_actor
        has_writer
        relationship
        """
        Logging.info(
            f"Start dumping of {HAS_ACTOR}".center(100, "*")
        )

        ProgramRelationship().has_actor()

        Logging.info(
            f"Start dumping of {HAS_WRITER}".center(100, "*")
        )

        ProgramRelationship().has_writer()

        Logging.info(
            f"Start dumping of {HAS_GENRE}".center(100, "*")
        )

        ProgramRelationship().has_genre()

        Logging.info(
            f"Start dumping of {HAS_DIRECTOR}".center(100, "*")
        )

        ProgramRelationship().has_director()

        Logging.info(
            f"Start dumping of {HAS_CLIP}".center(100, "*")
        )

        ProgramRelationship().has_clip()

        Logging.info(
            f"Start dumping of {HAS_EPISODE}".center(100, "*")
        )

        ProgramRelationship().has_episode()

        Logging.info(
            f"Start dumping of {HAS_EXTRA}".center(100, "*")
        )

        ProgramRelationship().has_extra()

        Logging.info(
            f"Successfully dumped all program relationship".center(100, "*")
        )
