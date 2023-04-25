from ast import literal_eval
from pandas import merge

from config.config import content_loader_path, static_loader_path
from config.constant_an import CSV, ACTOR, INNER, ACTOR_DEPENDENCIES_RENAME, HAS_ACTOR, ACTOR_DEPENDENCIES_REQUIRED, \
    WRITER, WRITER_DEPENDENCIES_RENAME, WRITER_DEPENDENCIES_REQUIRED, HAS_WRITER, HAS_DIRECTOR, DIRECTOR, GENRE, \
    HAS_GENRE, GENRE_DEPENDENCIES_RENAME, GENRE_DEPENDENCIES_REQUIRED, DIRECTOR_DEPENDENCIES_REQUIRED, \
    DIRECTOR_DEPENDENCIES_RENAME, CLIP, CLIP_DEPENDENCIES_REQUIRED, CLIP_DEPENDENCIES_RENAME, HAS_CLIP, EPISODE, \
    EPISODE_DEPENDENCIES_REQUIRED, EPISODE_DEPENDENCIES_RENAME, HAS_EPISODE, HAS_EXTRA, EXTRA_DEPENDENCIES_RENAME, \
    EXTRA_DEPENDENCIES_REQUIRED, EXTRA
from dump_all_hist.create_node import GenerateNode
from export_data.export_mongo import S3Connector
from utils.logger import Logging



class ProgramRelationship:
    def __init__(self):
        self.s3_connector = S3Connector()
        self.program_dependencies = self.s3_connector.fetch_csv_from_s3(
            object_name=f"{content_loader_path}program_dependencies{CSV}")

    def has_static_dependencies(self, dependency_name, required_columns, rename_columns, merge_on, label):
        try:
            dependencies = self.s3_connector.fetch_csv_from_s3(object_name=f"{static_loader_path}{dependency_name}{CSV}")
            dependencies = dependencies.rename(rename_columns, axis=1)[required_columns]
            ProgramRelationship().create_static_relationship(dependencies, merge_on, label)
        except Exception as e:
            Logging.error(f"Unable to process {label}, {str(e)}")

    def has_content_dependencies(self, dependency_name, required_columns, rename_columns, merge_on, label):
        try:
            dependencies = self.s3_connector.fetch_csv_from_s3(object_name=f"{content_loader_path}{dependency_name}{CSV}")
            dependencies = dependencies.rename(rename_columns, axis=1)[required_columns]
            ProgramRelationship().create_content_relationship(dependencies, merge_on, label)
        except Exception as e:
            Logging.error(f"Unable to process {label}, {str(e)}")


    def dump_relationship(self):
        Logging.info("create relationship between program to actor")
        self.has_static_dependencies(ACTOR, ACTOR_DEPENDENCIES_REQUIRED, ACTOR_DEPENDENCIES_RENAME, "actor_id", HAS_ACTOR)

        Logging.info("create relationship between program to writer")
        self.has_static_dependencies(WRITER, WRITER_DEPENDENCIES_REQUIRED, WRITER_DEPENDENCIES_RENAME, "writer_id", HAS_WRITER)

        Logging.info("create relationship between program to director")
        self.has_static_dependencies(DIRECTOR, DIRECTOR_DEPENDENCIES_REQUIRED, DIRECTOR_DEPENDENCIES_RENAME, "director_id",
                                     HAS_DIRECTOR)

        Logging.info("create relationship between program to genre")
        self.has_static_dependencies(GENRE, GENRE_DEPENDENCIES_REQUIRED, GENRE_DEPENDENCIES_RENAME, "genre_id", HAS_GENRE)

        Logging.info("create relationship between program to clip")
        self.has_content_dependencies(CLIP, CLIP_DEPENDENCIES_REQUIRED, CLIP_DEPENDENCIES_RENAME, "program_id", HAS_CLIP)

        Logging.info("create relationship between program to episode")
        self.has_content_dependencies(EPISODE, EPISODE_DEPENDENCIES_REQUIRED, EPISODE_DEPENDENCIES_RENAME, "program_id", HAS_EPISODE)

        Logging.info("create relationship between program to extra")
        self.has_content_dependencies(EXTRA, EXTRA_DEPENDENCIES_REQUIRED, EXTRA_DEPENDENCIES_RENAME, "program_id", HAS_EXTRA)

    def create_static_relationship(self, dependencies, merge_on, label):
        try:
            program_from = self.program_dependencies[["~id", merge_on]].rename({"~id": "~from"}, axis=1)
            program_from[merge_on] = program_from[merge_on].apply(literal_eval)
            program_from = program_from.explode(merge_on)
            has_dependencies = merge(program_from, dependencies, how=INNER, on=merge_on)
            has_dependencies = has_dependencies.drop(merge_on, axis="columns")
        except Exception as e:
            Logging.error(f"Unable to merge {merge_on} dependencies, {str(e)}")

        try:
            has_dependencies["~id"] = has_dependencies["~from"] + "-" + has_dependencies["~to"]
            has_dependencies["~label"] = label
            self.s3_connector.store_csv_to_s3(object_name=f"{content_loader_path}{label.lower()}{CSV}",
                                              data=has_dependencies)
            GenerateNode.create_node(key=f"{content_loader_path}{label.lower()}{CSV}")
        except Exception as e:
            Logging.error(f"Unable to dump {label} {str(e)}")

    def create_content_relationship(self, dependencies, merge_on, label):
        try:
            program_from = self.program_dependencies[["~id", merge_on]].rename({"~id": "~from"}, axis=1)
            has_dependencies = merge(program_from, dependencies, how=INNER, on=merge_on)
            has_dependencies = has_dependencies.drop(merge_on, axis="columns")
        except Exception as e:
            Logging.error(f"Unable to merge {merge_on} dependencies, {str(e)}")

        try:
            has_dependencies["~id"] = has_dependencies["~from"] + "-" + has_dependencies["~to"]
            has_dependencies["~label"] = label
            self.s3_connector.store_csv_to_s3(object_name=f"{content_loader_path}{label.lower()}{CSV}",
                                              data=has_dependencies)
            GenerateNode.create_node(key=f"{content_loader_path}{label.lower()}{CSV}")
        except Exception as e:
            Logging.error(f"Unable to dump {label} {str(e)}")



