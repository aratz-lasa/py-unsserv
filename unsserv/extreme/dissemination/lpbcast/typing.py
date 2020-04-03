from typing import List, Tuple, Union
from unsserv.common.structs import Node

EventData = bytes
EventId = str
EventOrigin = Node
LpbcastEvent = List[Union[EventId, EventData, EventOrigin]]
Digest = List[Tuple[EventId, EventOrigin]]
