import argparse

from dump_all_hist.main import create_node_on_graph
from export_data.main import export_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate selected records")
    parser.add_argument("-dsn", "--static-node", type=int, default=0)
    parser.add_argument("-dcn", "--content-node", type=int, default=0)
    parser.add_argument("-dpr", "--program_relationship", type=int, default=0)
    parser.add_argument("-user_node", "--user_node", type=int, default=0)
    parser.add_argument("-user_preference_node", "--user_preference_node", type=int, default=0)

    args = parser.parse_args()
    create_node_on_graph(static_node=args.static_node or False,
                         content_node=args.content_node or False,
                         program_relationship=args.program_relationship or False,
                         user_node= args.user_node or False,
                         user_preference_node = args.user_preference_node or False)
