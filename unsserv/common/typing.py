from collections import Counter
from typing import Any, Callable, Coroutine, List, Optional, Union

from unsserv.common.data_structures import Node

View = Counter
NeighboursCallback = Optional[
    Callable[[Union[List[Node], View]], Coroutine[Any, Any, None]]
]
AggregateCallback = Callable[[Any], Coroutine[Any, Any, None]]
