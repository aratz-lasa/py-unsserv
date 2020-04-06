from typing import Callable, Coroutine, Any, List, Dict
from unsserv.common.structs import Node
from unsserv.common.services_abc import View


LocalViewCallback = Callable[[View], Coroutine[Any, Any, None]]
CustomSelectionRanking = Callable[[View], List[Node]]
ExternalViewSource = Callable
Payload = Dict[Any, Any]
