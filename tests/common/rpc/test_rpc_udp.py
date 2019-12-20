import asyncio

import pytest

from unsserv.common.gossip.config import DATA_FIELD_VIEW
from unsserv.common.rpc.rpc import RpcUdp
from unsserv.data_structures import Node, Message

flag_destination = asyncio.Event()

node_origin = Node(("127.0.0.1", 7771))
node_destination = Node(("127.0.0.1", 7772))
SERVICE_ID = "rpc-udp"


async def callback_destination(message):
    assert message.node == node_origin
    assert message.data[DATA_FIELD_VIEW]

    flag_destination.set()
    data = {DATA_FIELD_VIEW: True}
    return Message(node_destination, SERVICE_ID, data)


rpc_origin = RpcUdp(node_origin)
rpc_destination = RpcUdp(node_destination)


async def start_rpc_servers(loop):
    await rpc_origin.register_service(SERVICE_ID, None)
    await rpc_destination.register_service(SERVICE_ID, callback_destination)


async def stop_rpc_servers():
    await rpc_origin.unregister_service(SERVICE_ID)
    await rpc_destination.unregister_service(SERVICE_ID)


@pytest.mark.asyncio
async def test_push():
    await start_rpc_servers(asyncio.get_event_loop())

    data = {DATA_FIELD_VIEW: True}
    message = Message(node_origin, SERVICE_ID, data)
    await rpc_origin.call_push(node_destination, message)
    assert flag_destination.is_set()

    await stop_rpc_servers()


@pytest.mark.asyncio
async def test_pushpull():
    await start_rpc_servers(asyncio.get_event_loop())

    data = {DATA_FIELD_VIEW: True}
    message = Message(node_origin, SERVICE_ID, data)
    pull_message = await rpc_origin.call_pushpull(node_destination, message)
    assert pull_message.node == node_destination
    assert pull_message.data[DATA_FIELD_VIEW]

    assert flag_destination.is_set()

    await stop_rpc_servers()
