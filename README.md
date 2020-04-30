# py-unsserv
[![PyPI version](https://badge.fury.io/py/unsserv.svg)](https://badge.fury.io/py/unsserv)
[![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/)
[![Build Status](https://travis-ci.com/aratz-lasa/py-unsserv.svg?branch=master)](https://travis-ci.com/aratz-lasa/py-unsserv)
[![codecov](https://codecov.io/gh/aratz-lasa/py-unsserv/branch/master/graph/badge.svg)](https://codecov.io/gh/aratz-lasa/py-unsserv)

[![PEP8](https://img.shields.io/badge/code%20style-pep8-orange.svg)](https://www.python.org/dev/peps/pep-0008/)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

**py-unsserv** is high-level Python library, designed to offer out-of-the-box peer-to-peer (p2p) 
network, as well as a bunch of functionalities on top of it. These functionalities include:
- Membership (or p2p network creation)
- Clustering
- Metrics aggregation
- Nodes sampling
- Dissemination (or broadcasting) 
- Data searching (key-value caching oriented)

**Look how easy it is to use:**
```python
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
    membership = await join_network(host, "network.id", bootstrap_nodes=[broadcaster_host])
    # initialize dissemination service and add broadcast handler
    async def handler(data: bytes):
        print(data)
    await get_dissemination_service(membership, "dissemination.id", broadcast_handler=handler)
    # wait for broadcast
    await asyncio.sleep(10)


if __name__ == "__main__":
    broadcaster_host = ("127.0.0.1", 7771)
    receiver_host = ("127.0.0.1", 7772)

    loop = asyncio.new_event_loop()
    loop.create_task(broadcaster(broadcaster_host))
    loop.create_task(receiver(receiver_host, broadcaster_host=broadcaster_host))
    loop.run_forever()
```

## Installation
```bash
pip install unsserv
```

## More about UnsServ
**py-unsserv** is the Python implementation of UnsServ, which is the academic work behind it.
For a more detailed understanding you can check the UnsServ non-published paper: 
[Original UnsServ academic paper](https://aratz.lasa.eus/file/unsserv.pdf)

