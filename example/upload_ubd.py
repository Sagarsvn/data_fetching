import io
import os
from multiprocessing import Process
from typing import Union, BinaryIO

import boto3
import pandas as pd


def write_files(
    files: Union[bytes, BinaryIO, str],
    bucket_name: str,
    file_name: str,
) -> None:
    """Update files blob to s3
    :param files: bytes data types
    :param bucket_name: string bucket name
    :param file_name: string file name in s3
    :return:
    """
    client = boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION_NAME", "ap-southeast-1"),
        aws_access_key_id=os.getenv("AWS_SECRET_ACCESS_KEY_ID", "AKIATO2X45HJUUZGZZGO"),
        aws_secret_access_key=os.getenv(
            "AWS_SECRET_ACCESS_KEY", "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"
        ),
    )
    client.put_object(
        Body=files,
        Bucket=bucket_name,
        Key=file_name,
    )
    client.close()


def load_file(
    path: str,
    chunksize: int = 500_000,
) -> pd:
    """Load file from path
    :param path: string path file
    :param chunksize: int chunk data
    :return: pandas object dataframe
    """
    try:
        print("loading csv data")
        return pd.read_csv(path, chunksize=chunksize, low_memory=False)
    except Exception as e:
        return pd.DataFrame()


def uploading_data(df: pd.DataFrame, cnt: int):
    new_writer = io.BytesIO()
    df.to_csv(new_writer, index=False, encoding="utf-8")
    new_writer.seek(0)
    path = "{path}/{file_name}".format(path="ubd", file_name="ubd_{}.csv".format(cnt))
    print("start processing {}".format(path))
    write_files(
        new_writer.getvalue(), os.getenv("AWS_BUCKET_NAME", "dev-rctiplus"), path
    )
    print("done processing {}".format(path))


def run():
    tmp = []
    cnt = 1
    for df in load_file("/home/ubuntu/export-mongo/ubd.csv"):
        if len(df) == 0:
            continue

        if cnt < 368:
            cnt += 1
            continue

        t = Process(target=uploading_data, args=(df, cnt))
        t.start()
        tmp.append(t)
        cnt += 1

    _ = [t.join() for t in tmp]


if __name__ == "__main__":
    run()
