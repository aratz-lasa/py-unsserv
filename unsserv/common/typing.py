from collections import Counter
from typing import Any, Callable, Coroutine, List, Optional, Union

from unsserv.common.data_structures import Node

View = Counter
AggregateCallback = Callable[[Any], Coroutine[Any, Any, None]]
NeighboursCallback = Optional[
    Callable[[Union[List[Node], View]], Coroutine[Any, Any, None]]
]
