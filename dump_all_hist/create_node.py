import json
import time

from utils.logger import Logging
from utils.service.rest import Rest

from config.config import AwsConfig, Config


class GenerateNode:

    @staticmethod
    def create_node(key=None, url=Config().graph_loader):
        rest = Rest()
        # Dump to AN
        payload = {
            "source": "s3://{}/{}".format(AwsConfig().s3_bucket_name, key),
            "format": "csv",
            "mode": "AUTO",
            "iamRoleArn": "arn:aws:iam::238018161107:role/NeptuneAccessS3",
            "region": "ap-southeast-1",
            "failOnError": "FALSE",
            "parallelism": "MEDIUM",
            "updateSingleCardinalityProperties": "FALSE",
        }
        resp = rest.post(url, payload=payload)
        time.sleep(2)
        if resp.status_code != 200:
            raise Exception("Error to post neptune -> ", resp.json())
        response = resp.json()
        queue_id = response["payload"]["loadId"]
        Logging.info("queue id response -> {}".format(queue_id))

        while True:

            temp_url = "{}/{}".format(url, queue_id)
            resp = rest.get(temp_url)
            if resp.status_code != 200:

                raise Exception("error fetching status neptune")

            response = resp.json()
            Logging.info("status neptune -> {}".format(json.dumps(response)))
            if response["payload"]["overallStatus"]["status"] != "LOAD_COMPLETED":
                time.sleep(5)

            else:
                break
        Logging.info(f"data is dumped for {key}".center(100, "*"))
