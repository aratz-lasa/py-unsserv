from enum import Enum, auto
from typing import Dict, Any

from unsserv.common.utils import IConfig


class ViewSelectionPolicy(Enum):
    RAND = auto()
    HEAD = auto()
    TAIL = auto()


class PeerSelectionPolicy(Enum):
    RAND = auto()
    HEAD = auto()
    TAIL = auto()


class ViewPropagationPolicy(Enum):
    PUSH = auto()
    PULL = auto()
    PUSHPULL = auto()


class GossipConfig(IConfig):
    LOCAL_VIEW_SIZE = 10  # todo: select a proper size
    GOSSIPING_FREQUENCY = 0.2  # todo: select a proper size
    VIEW_SELECTION = ViewSelectionPolicy.HEAD
    VIEW_PROPAGATION = ViewPropagationPolicy.PUSHPULL
    PEER_SELECTION = PeerSelectionPolicy.RAND
    RPC_TIMEOUT = 1

    def load_from_dict(self, config_dict: Dict[str, Any]):
        self.LOCAL_VIEW_SIZE = config_dict.get(
            "local_view_size", GossipConfig.LOCAL_VIEW_SIZE
        )
        self.GOSSIPING_FREQUENCY = config_dict.get(
            "gossiping_frequency", GossipConfig.GOSSIPING_FREQUENCY
        )
        self.VIEW_SELECTION = config_dict.get(
            "view_selection", GossipConfig.VIEW_SELECTION
        )
        self.VIEW_PROPAGATION = config_dict.get(
            "view_propagation", GossipConfig.VIEW_PROPAGATION
        )
        self.PEER_SELECTION = config_dict.get(
            "peer_selection", GossipConfig.PEER_SELECTION
        )
        self.RPC_TIMEOUT = config_dict.get("rpc_timeout", GossipConfig.RPC_TIMEOUT)
