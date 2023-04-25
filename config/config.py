import configparser
import os

from pydantic import BaseSettings

config = configparser.RawConfigParser()
config.read("{}/{}".format(os.path.join(os.path.dirname(__file__)), "config.ini"))
ENV = os.environ.get("ENV", "dev")


class AwsConfig(BaseSettings):
    s3_bucket_name: str = os.getenv(
        "AWS_BUCKET_NAME", config.get(ENV, "s3_bucket_name")
    )
    aws_access_key_id: str = os.getenv(
        "AWS_ACCESS_KEY_ID", config.get(ENV, "aws_access_key_id")
    )
    aws_secret_access_key: str = os.getenv(
        "AWS_SECRET_ACCESS_KEY", config.get(ENV, "aws_secret_access_key")
    )
    region_name: str = os.getenv("AWS_REGION_NAME", config.get(ENV, "region_name"))


class Config(BaseSettings):
    env: str = os.getenv("ENV", config.get(ENV, "env", fallback="dev"))
    redis_localhost: str = os.getenv("REDIS_LOCAL_HOST",
                                     config.get(ENV, "redis_local_host"))

    redis_port: str = os.getenv("REDIS_PORT",
                                config.get(ENV, "redis_port"))

    redis_user_name: str = os.getenv("REDIS_USER_NAME",
                                     config.get(ENV, "redis_user_name"))

    redis_password: str = os.getenv("REDIS_PASSWORD",
                                    config.get(ENV, "redis_password"))

    mongodb_user: str = os.getenv("MONGODB_USER",
                                  config.get(ENV, "mongodb_user_name"))

    mongodb_password: str = os.getenv("MONGODB_PASSWORD",
                                      config.get(ENV, "mongodb_password"))

    rplus_mongodb_uri: str = os.getenv("RPLUS_MONGODB_URI",
                                       config.get(ENV, "rplus_mongodb_uri"))

    graph_loader: str = os.getenv("GRAPH_LOADER",
                                  config.get(ENV, "graph_loader"))




genre_missing_id = {"genre_id": ['74', '89', '94'],
                    "genre_name": ["Excersice", "Sport Highlights", "Misteri"]}
date_to_save = "20230423"
content_path = "historical_data/content/raw/{}/".format(date_to_save)
user_path = "historical_data/user/raw/{}/".format(date_to_save)
user_loader_path = "historical_data/user/loader_csv/{}/".format(date_to_save)
static_path = "historical_data/static/raw/{}/".format(date_to_save)
static_loader_path = "historical_data/static/loader_csv/{}/".format(date_to_save)
content_loader_path = "historical_data/content/loader_csv/{}/".format(date_to_save)
ubd_loader_path = "ubd/loader_csv/{}/".format(date_to_save)
registered_ubd_start_month = ["2022-08-01", "2022-09-01", "2022-10-01", "2022-11-01", "2022-12-01",
                              "2023-01-01", "2023-02-01"]
registered_ubd_end_month = ["2022-08-31", "2022-09-30", "2022-10-31", "2022-11-30", "2022-12-31",
                            "2023-01-31", "2023-02-26"]

