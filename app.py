import argparse

from dump_all_hist.main import create_node_on_graph
from export_data.main import export_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate selected records")
    parser.add_argument("-csd", "--content-with-static-data", type=int, default=0)
    parser.add_argument("-cs", "--customer", type=int, default=0)
    parser.add_argument("-ubd", "--ubd", type=int, default=0)
    parser.add_argument("-dsn", "--dump-static-node", type=int, default=0)

    args = parser.parse_args()
    export_data(content_with_static=args.content_with_static_data or False,
                customer=args.customer or False,
                ubd=args.ubd or False, )

    create_node_on_graph(dump_static_node=args.dump_static_node
                                          or False)
