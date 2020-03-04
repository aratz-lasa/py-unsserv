from typing import List, Union
from unsserv.common.data_structures import Node

EventData = bytes
EventId = str
EventOrigin = Node
LpbcastEvent = List[Union[EventId, EventData, EventOrigin]]
