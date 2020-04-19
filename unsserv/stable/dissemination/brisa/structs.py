from dataclasses import dataclass


@dataclass
class Push:
    data: bytes
    data_id: str
