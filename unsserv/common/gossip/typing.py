from typing import Callable, Any, List, Dict

from unsserv.common.typing import View
from unsserv.common.structs import Node

CustomSelectionRanking = Callable[[View], List[Node]]
ExternalViewSource = Callable
Payload = Dict[Any, Any]
