from collections import Counter
from typing import Callable, List, Coroutine, Any, Union
from unsserv.common.utils.data_structures import Node

View = Counter
AggregateCallback = Callable[[Any], Coroutine[Any, Any, None]]
NeighboursCallback = Union[
    Callable[[Union[List[Node], View]], Coroutine[Any, Any, None]], None
]
