from datetime import datetime, timedelta
import pandas as pd
import pytz
from rplus_constants import ID
from rplus_utils import MongoDbConnection, AwsANGraphDb, Logging

class ContentTesting:

    @staticmethod
    def get_latest_one_day_content_id(content_type):
        data = MongoDbConnection.new_connection_conviva()
        data = data.find(
                f"master:{content_type}",
                {
                    "id": {"$ne": None},
                },
                {},
        )
        data = pd.DataFrame(data)
        jakarta_timezone = pytz.timezone('Asia/Jakarta')
        # Convert UTC datetime to Jakarta time
        jakarta_time = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(jakarta_timezone)
        # Calculate the time 1 day  ago
        time_one_day_ago = jakarta_time - timedelta(hours=24)
        time_one_day_ago = time_one_day_ago.strftime("%Y-%m-%d %H:%M:%S")
        content_id = data[data['created_at'] > time_one_day_ago]
        content_id = content_id["id"].values
        return content_id

    @staticmethod
    def content_id_in_graph(content_type):
        query = (
            f'g.V().hasLabel("{content_type}").values("{content_type +"_"+ ID}").fold()'
        )
        try:
            graph = AwsANGraphDb.new_connection_config().graph
            list_of_content_id = graph.custom_query(query, {})
            list_of_content_id = list_of_content_id[0][0]
        except ValueError:
            Logging.error("Content Category is not correct")
        except Exception as e:
            Logging.error(
                f"Error while fetching data from network, retrying with new connection, "
                f"Error : {e}"
            )
            graph.connection.close()
            graph = AwsANGraphDb.new_connection_config().graph
            list_of_content_id = graph.custom_query(query, {})
            list_of_content_id = list_of_content_id[0][0]
        finally:
            graph.connection.close()
            return list_of_content_id

    @staticmethod
    def latest_data_in_graph(content_type):
        content_id_mongodb = ContentTesting.get_latest_one_day_content_id(content_type)
        content_id_graph = ContentTesting.content_id_in_graph(content_type)
        content_id_graph = list(map(int, content_id_graph))
        content_id = {x: x in content_id_graph for x in content_id_mongodb}
        print("content id is not updated in graph" ":", [content_id])
# if __name__ == '__main__':
#     x= ContentTesting.latest_data_in_graph("program")

