from dump_all_hist.content.clip import DumpClip
from dump_all_hist.content.episode import DumpEpisode
from dump_all_hist.content.extra import DumpExtra
from dump_all_hist.content.program import DumpProgram
from dump_all_hist.content.relationship import ProgramRelationship
from dump_all_hist.static import DumpStatic
from utils.logger import Logging


def create_node_on_graph(
        static_node: bool = False,
        content_node:bool = False,
        program_relationship:bool =False,
        user_node:bool = False,
        user_preference_node:bool = False
):
    if static_node:
        Logging.info("dump all static node in graph ".center(100, "*"))

        DumpStatic().dump_static_node()

    if content_node:
        Logging.info(
            "Start Dumping of clip node on network".center(
                100, "*"
            )
        )

        DumpClip().dump_clip_on_graph()

        Logging.info(
            "Start Dumping of episode node on network".center(
                100, "*"
            )
        )

        DumpEpisode().dump_episode_on_graph()

        Logging.info(
            "Start Dumping of episode node on network".center(
                100, "*"
            )
        )
        DumpExtra().dump_extra_on_graph()

        Logging.info(
            "Start Dumping of program node on network".center(
                100, "*"
            )
        )
        DumpProgram().dump_program_on_graph()

    if program_relationship:

        Logging.info(
            "Start Dumping of program relationship network".center(
                100, "*"
            )
        )
        ProgramRelationship().dump_relationship()




