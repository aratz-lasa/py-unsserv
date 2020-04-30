import asyncio
from typing import Tuple

from unsserv import join_network, get_dissemination_service

Host = Tuple[str, int]


async def broadcaster(host: Host):
    # join p2p network
    membership = await join_network(host, "network.id")
    # initialize dissemination service
    dissemination = await get_dissemination_service(membership, "dissemination.id")
    # wait for receiver to join network
    await asyncio.sleep(1)
    # broadcast data
    for _ in range(10):
        await dissemination.broadcast(b"data.alert")
        await asyncio.sleep(1)


async def receiver(host: Host, broadcaster_host: Host):
    # wait for broadcaster to join network
    await asyncio.sleep(1)
    # join p2p network
    membership = await join_network(
        host, "network.id", bootstrap_nodes=[broadcaster_host]
    )
    # initialize dissemination service and add broadcast handler

    async def handler(data: bytes):
        print(data)

    await get_dissemination_service(
        membership, "dissemination.id", broadcast_handler=handler
    )
    # wait for broadcast
    await asyncio.sleep(10)


if __name__ == "__main__":
    broadcaster_host = ("127.0.0.1", 7771)
    receiver_host = ("127.0.0.1", 7772)

    loop = asyncio.new_event_loop()
    loop.create_task(broadcaster(broadcaster_host))
    loop.create_task(receiver(receiver_host, broadcaster_host=broadcaster_host))
    loop.run_forever()
