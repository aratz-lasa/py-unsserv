from typing import Callable, Coroutine, Any, List
from unsserv.common.data_structures import Node
from unsserv.common.services_abc import View


LocalViewCallback = Callable[[View], Coroutine[Any, Any, None]]
CustomSelectionRanking = Callable[[View], List[Node]]
ExternalViewSource = Callable
