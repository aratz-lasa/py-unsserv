from dataclasses import dataclass
from typing import Dict, Any

from unsserv.common.gossip.typing import Payload, View


@dataclass
class PushData:
    view: View
    payload: Payload

    def encode(self) -> Dict[str, Any]:
        return {"view": self.view, "payload": self.payload}
