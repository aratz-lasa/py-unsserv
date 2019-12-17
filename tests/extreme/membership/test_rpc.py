import asyncio

import pytest

from unsserv.data_structures import Node, Message
from unsserv.common.gossip.config import DATA_FIELD_VIEW
from unsserv.common.gossip.rpc import GossipRPC

flag_destination = asyncio.Event()

node_origin = Node("127.0.0.1", 7771)
node_destination = Node("127.0.0.1", 7772)


async def callback_destination(message):
    assert Node(*message.node) == node_origin
    assert message.data[DATA_FIELD_VIEW]

    flag_destination.set()
    data = {DATA_FIELD_VIEW: True}
    return Message(node_destination, data)


rpc_origin = GossipRPC(node_origin, None)
rpc_destination = GossipRPC(node_destination, callback_destination)
transport_origin = None
transport_destination = None


async def start_rpc_servers(loop):
    global transport_origin, transport_destination
    transport_origin, _ = await loop.create_datagram_endpoint(
        lambda: rpc_origin, node_origin
    )
    transport_destination, _ = await loop.create_datagram_endpoint(
        lambda: rpc_destination, node_destination
    )


async def stop_rpc_servers():
    transport_origin.close()
    transport_destination.close()


@pytest.mark.asyncio
async def test_push():
    await start_rpc_servers(asyncio.get_event_loop())

    data = {DATA_FIELD_VIEW: True}
    message = Message(node_origin, data)
    await rpc_origin.call_push(node_destination, message)
    assert flag_destination.is_set()

    await stop_rpc_servers()


@pytest.mark.asyncio
async def test_pushpull():
    await start_rpc_servers(asyncio.get_event_loop())

    data = {DATA_FIELD_VIEW: True}
    message = Message(node_origin, data)
    pull_message = await rpc_origin.call_pushpull(node_destination, message)
    assert Node(*pull_message.node) == node_destination
    assert pull_message.data[DATA_FIELD_VIEW]

    assert flag_destination.is_set()

    await stop_rpc_servers()
