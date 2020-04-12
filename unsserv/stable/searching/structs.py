from dataclasses import dataclass

from unsserv.common.structs import Node


@dataclass
class Search:
    id: str
    origin_node: Node
    ttl: int
    data_id: str


@dataclass
class SearchResult:
    search_id: str
    result: bytes


@dataclass
class DataChange:
    data_id: str
    ttl: int
