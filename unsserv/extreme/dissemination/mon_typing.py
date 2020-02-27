from typing import Optional, Callable, Any, Coroutine

BroadcastID = str
BroadcastHandler = Optional[Callable[[Any], Coroutine[Any, Any, None]]]
