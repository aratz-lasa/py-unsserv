from typing import Any, Callable, Coroutine, Union

from unsserv.common.structs import Message

RpcCallback = Callable[[Message], Coroutine[Any, Any, Union[None, Any]]]
