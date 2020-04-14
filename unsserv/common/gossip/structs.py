from dataclasses import dataclass
from enum import Enum, auto
from typing import Dict, Any

from unsserv.common.gossip.typing import Payload, View


@dataclass
class PushData:
    view: View
    payload: Payload

    def encode(self) -> Dict[str, Any]:
        return {"view": self.view, "payload": self.payload}


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
