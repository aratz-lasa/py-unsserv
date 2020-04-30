Quickstart
===========

Set-up a P2P network
=====================
Setting up a P2P network (membership) is very simple.

Start by importing the join_network function:

.. code-block:: python

    from unsserv import join_network
Now, the first node is initialized, to whom the other nodes will connect to join the network:

.. code-block:: python

    first_membership = await join_network(("127.0.0.1", 77771), "network.id")
:code:`("127.0.0.1", 77771)` is the host where the local host will be listening. :code:`"network.id"` is
the identifier used for joining a specific network or service.

Join the network
=================
Once the first node is initialized, the other nodes are able to join the netwok, by using the service ID
:code:`"network.id"`

.. code-block:: python

    second_membership = await join_network(
            ("127.0.0.1", 7772), "network.id", bootstrap_nodes=[("127.0.0.1", 77771)]
        )
:code:`bootstrap_nodes` are nodes that already joined the network. In this case it is the :code:`first_node` address.

Retrieving neighbours
======================
After joining the network, it is possible to retrieve the neighbours that is connected to, by just:

.. code-block:: python

    neighbours = second_membership.get_neighbours()
Or, in order to be notified whenever the neighbours change:

.. code-block:: python

    async def handler(neighbours):
        for neighbour in neighbours:
            print(neighbour)

    second_membership.add_neighbours_handler(handler)
:code:`handler(neighbours)` will be called whenever the neighbourship changes.

Clustering the network
=======================
It is possible to create a clustered view of the network by making use of the **Clustering** service.

First a function for clustering is imported:

.. code-block:: python

    from unsserv import get_clustering_service
Then, the clustered view is initialized by passing defining a ranking function and calling
:code:`get_clustering_service` with an instance of the memberhsip (or network).
The function must receive a node and return a numeric value representing the its suitability (lower is better).

In this case, the ranking function is a function is biased by the distance between their sockets ports values.

.. code-block:: python

    def port_distance(node: Node):
        my_port = 7771
        ip, port = node.address_info
        return my_port - port

    await get_clustering_service(membership, "clustering.id", ranking_function=port_distance)
:code:`"clustering.id"` is the ID representing the Clustering service, which is needed for joining the
clustered network.