Quickstart
===========

Set-up a P2P network
---------------------
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
-----------------
Once the first node is initialized, the other nodes are able to join the netwok, by using the service ID
:code:`"network.id"`

.. code-block:: python

    second_membership = await join_network(
            ("127.0.0.1", 7772), "network.id", bootstrap_nodes=[("127.0.0.1", 77771)]
        )
:code:`bootstrap_nodes` are nodes that already joined the network. In this case it is the :code:`first_node` address.

Leave/Stop service
-------------------
Once a service is running it can be stopped easily by:

.. code-block:: python

    await membership.leave()

Retrieving neighbours
----------------------
After joining the network, it is possible to retrieve the neighbours that is connected to, by just:

.. code-block:: python

    neighbours = second_membership.get_neighbours()
Or, in order to be notified whenever the neighbours change:

.. code-block:: python

    async def handler(neighbours):
        ...

    second_membership.add_neighbours_handler(handler)
:code:`handler(neighbours)` will be called whenever the neighbourship changes.

Clustering the network
-----------------------
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

    clustered_memberhsip = await get_clustering_service(membership, "clustering.id", ranking_function=port_distance)
:code:`"clustering.id"` is the ID representing the Clustering service, which is needed for joining the
clustered network.

Aggregate metrics
------------------
For aggregating simple metrics from the network, or even node properties, UnsServ offers
the **Aggregation** service.

For it, first the following function must be imported:

.. code-block:: python

    from unsserv import get_aggregation_service
Then the Aggregation service is instanced passing as arguments the membership, the initial value of the aggregate,
and the aggregate type:

.. code-block:: python

    aggregation = await get_aggregation_service(membership, "aggregation.id", aggregate_value=1, aggregate_type="mean")
Where :code:`aggregate_type` can be one of :code:`["mean", "max", "min"]`.

Once started the aggregation service, the aggregate can be retrieved using a callback,
or by explicitly calling a function:

.. code-block:: python

    aggregate = aggregation.get_aggregate()
    ...
    def handler(aggregate):
        ...
    aggregate.add_handler(handler)

Sampling peers
---------------
In order to use the **Sampling** service first the following is imported:

.. code-block:: python

    from unsserv import get_sampling_service
Then, the service instance is created:

.. code-block:: python

    samping = await get_sampling_service(membership, "sampling.id")
And in order to sample a peer from the network, just :code:`get_sample` must be called:

.. code-block:: python

    sampled_node = await samping.get_sample()


Searching data
---------------
First the function for creating the **Searching** service must be imported:

.. code-block:: python

    from unsserv import get_searching_service
Then, the Searching service is instantiated:

.. code-block:: python

    searching = await get_sampling_service(membership, "searching.id")

In order to search data, first the data must be published (make it available for others):

.. code-block:: python

    await searching.publish("data.id", b"data")
The data is publsihed along with its identifier :code:`"data.id"`, which is used by
the other nodes in order to find it.

And finally, the data is searched:

.. code-block:: python

    data = await searching.search("data.id")

Data dissemination (broadcast)
-------------------------------
UnsServ offers the **Dissemination**, which is used for broadcasting data in the network.

First the function for creating the Dissemination service is imported:

.. code-block:: python

    from unsserv import get_dissemination_service
Then, the Dissemination service is instantiated:

.. code-block:: python

    async def handler(broadcast_data):
        ...
    dissemination = await get_dissemination_service(membership, "dissemination.id", broadcast_handler=handler)
:code:`broadcast_handler` is the callback that is called whenever a broadcast is received.

In order to broadcast data :code::`broadcast` method is called:

.. code-block:: python

    await dissemination.broadcast(b"data")
