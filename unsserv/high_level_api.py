from typing import Tuple, List

from unsserv import extreme, stable
from unsserv.common.services_abc import (
    IMembershipService,
    IClusteringService,
    IAggregationService,
    ISamplingService,
    IDisseminationService,
    ISearchingService,
)
from unsserv.common.structs import Node

__all__ = [
    "join_network",
    "get_aggregation_service",
    "get_clustering_service",
    "get_sampling_service",
    "get_dissemination_service",
    "get_searching_service",
]


async def join_network(
    host: Tuple[str, int], service_id: str, is_extreme=True, **config
) -> IMembershipService:
    node = Node(address_info=host)
    if "bootstrap_nodes" in config:
        config["bootstrap_nodes"] = _bootstrap_nodes_to_structs(
            config["bootstrap_nodes"]
        )
    membership: IMembershipService
    if is_extreme:
        membership = extreme.Newscast(node)
    else:
        membership = stable.HyParView(node)
    await membership.join(service_id, **config)
    return membership


async def get_clustering_service(
    membership: IMembershipService, service_id: str, is_extreme=True, **config
) -> IClusteringService:
    clustering: IClusteringService
    if is_extreme:
        clustering = extreme.TMan(membership)
    else:
        clustering = stable.XBot(membership)
    await clustering.join(service_id, **config)
    return clustering


async def get_aggregation_service(
    membership: IMembershipService, service_id: str, is_extreme=True, **config
) -> IAggregationService:
    aggregation: IAggregationService
    if is_extreme:
        aggregation = extreme.AntiEntropy(membership)
    else:
        aggregation = stable.AntiEntropy(membership)
    await aggregation.join(service_id, **config)
    return aggregation


async def get_sampling_service(
    membership: IMembershipService, service_id: str, is_extreme=True, **config
) -> ISamplingService:
    sampling: ISamplingService
    if is_extreme:
        sampling = extreme.MRWB(membership)
    else:
        sampling = stable.RWD(membership)
    await sampling.join(service_id, **config)
    return sampling


async def get_dissemination_service(
    membership: IMembershipService,
    service_id: str,
    is_extreme=True,
    many_to_many=True,
    **config
):
    dissemination: IDisseminationService
    if is_extreme:
        if many_to_many:
            dissemination = extreme.Lpbcast(membership)
        else:
            dissemination = extreme.Mon(membership)
    else:
        if many_to_many:
            dissemination = stable.Plumtree(membership)
        else:
            dissemination = stable.Brisa(membership)
    await dissemination.join(service_id, **config)
    return dissemination


async def get_searching_service(
    membership: IMembershipService, service_id: str, is_extreme=True, **config
):
    searching: ISearchingService
    if is_extreme:
        searching = extreme.KWalker(membership)
    else:
        searching = stable.ABloom(membership)
    await searching.join(service_id, **config)
    return searching


def _bootstrap_nodes_to_structs(boostrap_nodes: List[Tuple[str, int]]) -> List[Node]:
    new_boostrap_nodes = []
    for node in boostrap_nodes:
        new_boostrap_nodes.append(Node(address_info=node))
    return new_boostrap_nodes
