from typing import Any, Callable, Coroutine, Union

from unsserv.common.data_structures import Message

RpcCallback = Callable[[Message], Coroutine[Any, Any, Union[None, Any]]]
