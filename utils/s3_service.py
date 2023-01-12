import os
import pickle
from typing import ClassVar

import boto3
from pandas import DataFrame, read_csv
from io import StringIO, BytesIO

from config.config import AwsConfig


class S3Service:
    def __init__(self, boto_object: boto3.resource):
        self.resource = boto_object

    @classmethod
    def from_connection(cls, access_key: str = AwsConfig().aws_access_key_id,
                        secret_key: str = AwsConfig().aws_secret_access_key,
                        region_name: str = AwsConfig().region_name) -> ClassVar:
        conn = boto3.resource('s3',
                              aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key,
                              region_name=region_name)

        return cls(conn)

    def read_csv_from_s3(
            self,
            bucket_name=AwsConfig().s3_bucket_name,
            object_name=None,
    ) -> DataFrame:
        """
        This function returns dataframe object of csv file stored in S3

        :param bucket_name: Name of the bucket where csv is stored
        :param object_name: Path of the object in S3
        :param resource: Connection object
        :return: dataframe object pandas
        """
        try:
            content_object = self.resource.Object(bucket_name, object_name)
            csv_string = content_object.get()['Body'].read().decode('utf - 8')
            df = read_csv(StringIO(csv_string))
            print('Successfully read data from s3')
            return df
        except Exception as e:
            print(f"Error while reading {object_name} to S3, Exception: {e}")

    def write_csv_to_s3(
            self,
            bucket_name=AwsConfig().s3_bucket_name,
            object_name=None,
            df_to_upload=None
    ) -> None:
        """
        Function to write csv in S3

        :param bucket_name: Name of the bucket where csv shall be stored
        :param object_name: Path of the object in S3
        :param df_to_upload: dataframe to be stored as csv
        :param resource: Connection object
        :return:
        """
        try:
            csv_buffer = StringIO()
            df_to_upload.to_csv(csv_buffer, index=False)
            content_object = self.resource.Object(bucket_name, object_name)
            content_object.put(Body=csv_buffer.getvalue())
            print('Successfully dumped data into s3')
        except Exception as e:
            print(f"Error while dumping {object_name} to S3, Exception: {e}")

    def write_df_pkl_to_s3(
            self,
            bucket_name=
            AwsConfig().s3_bucket_name,
            object_name=None,
            data=None
    ) -> None:
        try:
            file_name = os.path.split(object_name)[1]
            print("Start dumping " + file_name + " data into s3")
            pickle_buffer = BytesIO()
            data.to_pickle(pickle_buffer, compression='gzip')
            self.resource.Object(bucket_name, object_name).put(Body=pickle_buffer.getvalue())
            print("Successfully dumped " + file_name + " data into s3")
        except Exception as e:
            print(f"Error while dumping {object_name} to S3, Exception: {e}")

    def read_pickles_from_s3(self,
                             bucket_name=AwsConfig().s3_bucket_name,
                             object_name=None):
        try:
            response = self.resource.Bucket(bucket_name).Object(object_name).get()
            body_string = response['Body'].read(compresion='gzip')
            loaded_pickle = pickle.loads(body_string)
            print("File {} has been read successfully".format(object_name))
            return loaded_pickle
        except Exception as e:
            print(f"Unable to find file {object_name}. No such file exists ,Exception:{e}")
