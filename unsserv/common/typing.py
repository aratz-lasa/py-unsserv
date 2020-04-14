from collections import Counter
from typing import Any, Callable, Coroutine, Union

View = Counter

SyncHandler = Callable[..., None]
AsyncHandler = Callable[..., Coroutine[Any, Any, None]]
Handler = Union[SyncHandler, AsyncHandler]
