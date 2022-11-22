import os
from io import BytesIO
import pandas as pd
import boto3

client = boto3.client(
    "s3",
    region_name=os.getenv("AWS_REGION_NAME", "ap-southeast-1"),
    aws_access_key_id=os.getenv("AWS_SECRET_ACCESS_KEY_ID", "AKIATO2X45HJUUZGZZGO"),
    aws_secret_access_key=os.getenv(
        "AWS_SECRET_ACCESS_KEY", "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"
    ),
)


def read_files(bucket_name: str, file_name: str) -> bytes:
    """Read value from s3 and return value as string
    :param bucket_name: string bucket name
    :param file_name: file name is s3
    :return: string value
    """
    file_bytes = client.get_object(Bucket=bucket_name, Key=file_name)
    if v := file_bytes.get("Body", None):
        return v.read()

    return bytes()


def load_to_dataframe(data: bytes) -> pd.DataFrame:
    """Convert dataframe from bytes
    :param data: bytes
    :return: dataframe
    """
    return pd.read_csv(BytesIO(data))


def run():
    """file is from 1 to 516
    example: ubd_1.csv, ubd_2.csv, ubd_3.csv, ..., ubd_516.csv
    each file have 500_000 data
    :return:
    """
    file_name = "ubd/ubd_1.csv"
    resp = read_files(os.getenv("AWS_BUCKET_NAME", "dev-rctiplus"), file_name)
    df = load_to_dataframe(resp)
    print(len(df))


if __name__ == "__main__":
    run()
