from dataclasses import dataclass
from unsserv.common.gossip.typing import Payload, View


@dataclass
class PushData:
    view: View
    payload: Payload
