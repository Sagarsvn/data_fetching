from dump_all_hist.content.clip import DumpClip
from dump_all_hist.content.episode import DumpEpisode
from dump_all_hist.content.extra import DumpExtra
from dump_all_hist.content.program import DumpProgram
from dump_all_hist.content.relationship import ProgramRelationship
from dump_all_hist.static import DumpStatic
from dump_all_hist.user.anonymous_viewed import AnonymousViewed

from dump_all_hist.user.user import DumpUser
from dump_all_hist.user.register_viewed import RegisterViewed

from utils.logger import Logging


def create_node_on_graph(
        static_node: bool = False,
        content_node: bool = False,
        program_relationship: bool = False,
        user_node: bool = False,
        user_cluster_node: bool = False,
        user_viewed_node: bool = False,
        anonymous_viewed_node: bool = False,
        user_profile_update: bool = False
):
    if static_node:
        Logging.info("Dumping all static nodes on the graph".center(100, "*"))
        DumpStatic().dump_static_node()

    if content_node:
        Logging.info("Dumping clip nodes on the graph".center(100, "*"))
        DumpClip().dump_clip_on_graph()

        Logging.info("Dumping episode nodes on the graph".center(100, "*"))
        DumpEpisode().dump_episode_on_graph()

        Logging.info("Dumping extra nodes on the graph".center(100, "*"))
        DumpExtra().dump_extra_on_graph()

        Logging.info("Dumping program nodes on the graph".center(100, "*"))
        DumpProgram().dump_program_on_graph()

    if program_relationship:
        Logging.info("Creating program relationship on the graph".center(100, "*"))
        ProgramRelationship().dump_relationship()

    if user_node:
        Logging.info("Dumping user nodes on the graph".center(100, "*"))
        DumpUser().dump_user_on_graph()

    if user_viewed_node:
        Logging.info("Dumping user viewed nodes on the graph".center(100, "*"))
        RegisterViewed().viewed_relationship()

    if anonymous_viewed_node:
        Logging.info("Dumping anonymous viewed nodes on the graph".center(100, "*"))
        AnonymousViewed().viewed_relationship()
