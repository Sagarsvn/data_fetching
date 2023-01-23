from dump_all_hist.static import DumpStatic


def create_node_on_graph(
        dump_static_node: bool = False
):
    if dump_static_node:
        print("dump all static node in graph ".center(100, "*"))

        DumpStatic().dump_static_node()
