from unsserv.common.structs import Node
from unsserv.extreme.dissemination.lpbcast.typing import Digest
from dataclasses import dataclass


@dataclass
class Event:
    id: str
    data: bytes
    origin: Node
    digest: Digest
