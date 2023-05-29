import itertools

from rplus_utils.db_services.aws_netpune.neptune_graph_db import AwsANGraphDb
from rplus_constants.constants import CUSTOMER_ID, USER
from rplus_utils.db_services.aws_s3.s3bucket import S3Services
from rplus_constants.config import CUSTOMER_IDS_PATH


def fetch_customers_count():
    count_query = f"g.V().hasLabel('{USER}').count()"
    try:
        graph = AwsANGraphDb.new_connection_config().graph
        response = graph.custom_query(count_query, {})
        graph.connection.close()
        return response[0][0]
    except Exception as e:
        print(f"Unable to fetch customer node count, Error: {e}")


def fetch_customer_ids():
    vertices = f"g.V().hasLabel('{USER}').values('{CUSTOMER_ID}')"
    all_vertices = []
    try:
        total_users = fetch_customers_count()
        graph = AwsANGraphDb.new_connection_config().graph
        if total_users > 246238:
            max_size = 50000  # Number of vertices to fetch per iteration
            for offset in range(0, total_users, max_size):
                vertices = f"g.V().hasLabel('{USER}').values('{CUSTOMER_ID}')" \
                           f".range({offset}, {offset + max_size}).toList()"
                response = graph.custom_query(vertices, {})
                all_vertices.extend(list(itertools.chain(*response)))
        else:
            response = graph.custom_query(vertices, {})
            all_vertices = list(itertools.chain(*response))
        graph.connection.close()
        return all_vertices
    except Exception as e:
        print(f"Unable to fetch customer node count, Error: {e}")


def save_customer_ids_s3():
    all_vertices_ids = fetch_customer_ids()
    try:
        S3Services.new_connection().write_pickle_list_s3(object_name=CUSTOMER_IDS_PATH, data=all_vertices_ids)
        print("Successfully dumped customer_ids list on S3..")
    except Exception as e:
        print(f"Exception while saving customer_ids list in S3, Error: {e}")

# Run the code below after dumping all the user nodes in graphDB
# if __name__ == '__main__':
#
#     save_customer_ids_s3()
