import asyncio

import pytest

from unsserv.data_structures import Node, Message
from unsserv.common.gossip.config import DATA_FIELD_VIEW
from unsserv.common.gossip.rpc.rpc_udp import GossipRPCUDP

flag_destination = asyncio.Event()

node_origin = Node(("127.0.0.1", 7771))
node_destination = Node(("127.0.0.1", 7772))


async def callback_destination(message):
    assert message.node == node_origin
    assert message.data[DATA_FIELD_VIEW]

    flag_destination.set()
    data = {DATA_FIELD_VIEW: True}
    return Message(node_destination, "id", data)


rpc_origin = GossipRPCUDP(node_origin, None)
rpc_destination = GossipRPCUDP(node_destination, callback_destination)


async def start_rpc_servers(loop):
    await rpc_origin.start()
    await rpc_destination.start()


async def stop_rpc_servers():
    await rpc_origin.stop()
    await rpc_destination.stop()


@pytest.mark.asyncio
async def test_push():
    await start_rpc_servers(asyncio.get_event_loop())

    data = {DATA_FIELD_VIEW: True}
    message = Message(node_origin, "id", data)
    await rpc_origin.call_push(node_destination, message)
    assert flag_destination.is_set()

    await stop_rpc_servers()


@pytest.mark.asyncio
async def test_pushpull():
    await start_rpc_servers(asyncio.get_event_loop())

    data = {DATA_FIELD_VIEW: True}
    message = Message(node_origin, "id", data)
    pull_message = await rpc_origin.call_pushpull(node_destination, message)
    assert pull_message.node == node_destination
    assert pull_message.data[DATA_FIELD_VIEW]

    assert flag_destination.is_set()

    await stop_rpc_servers()
