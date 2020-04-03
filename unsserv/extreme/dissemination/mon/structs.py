from dataclasses import dataclass


@dataclass
class Session:
    broadcast_id: str
    level: int


@dataclass
class Broadcast:
    id: str
    data: bytes
