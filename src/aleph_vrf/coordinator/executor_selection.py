import abc
import json
from pathlib import Path
from typing import List, Dict, Any, AsyncIterator
import random

import aiohttp
from aleph_message.models import ItemHash

from aleph_vrf.exceptions import NotEnoughExecutors, AlephNetworkError
from aleph_vrf.models import Executor, Node, AlephExecutor, ComputeResourceNode
from aleph_vrf.settings import settings


class ExecutorSelectionPolicy(abc.ABC):
    @abc.abstractmethod
    async def select_executors(self, nb_executors: int) -> List[Executor]:
        ...


async def _get_corechannel_aggregate() -> Dict[str, Any]:
    async with aiohttp.ClientSession(settings.API_HOST) as session:
        url = (
            f"/api/v0/aggregates/{settings.CORECHANNEL_AGGREGATE_ADDRESS}.json?"
            f"keys={settings.CORECHANNEL_AGGREGATE_KEY}"
        )
        async with session.get(url) as response:
            if response.status != 200:
                raise AlephNetworkError(f"CRN list not available")

            return await response.json()


class ExecuteOnAleph(ExecutorSelectionPolicy):
    def __init__(self, vm_function: ItemHash):
        self.vm_function = vm_function

    @staticmethod
    async def _list_compute_nodes() -> AsyncIterator[ComputeResourceNode]:
        content = await _get_corechannel_aggregate()

        if (
            not content["data"]["corechannel"]
            or not content["data"]["corechannel"]["resource_nodes"]
        ):
            raise AlephNetworkError(f"Bad CRN list format")

        resource_nodes = content["data"]["corechannel"]["resource_nodes"]

        for resource_node in resource_nodes:
            # Filter nodes by score, with linked status
            if resource_node["status"] == "linked" and resource_node["score"] > 0.9:
                node_address = resource_node["address"].strip("/")
                node = ComputeResourceNode(
                    hash=resource_node["hash"],
                    address=node_address,
                    score=resource_node["score"],
                )
                yield node

    @staticmethod
    def _get_unauthorized_nodes() -> List[str]:
        unauthorized_nodes_list_path = Path(__file__).with_name(
            "unauthorized_node_list.json"
        )
        if unauthorized_nodes_list_path.is_file():
            with open(unauthorized_nodes_list_path, "rb") as fd:
                return json.load(fd)

        return []

    async def select_executors(self, nb_executors: int) -> List[Executor]:
        compute_nodes = self._list_compute_nodes()
        blacklisted_nodes = self._get_unauthorized_nodes()
        whitelisted_nodes = (
            node
            async for node in compute_nodes
            if node.address not in blacklisted_nodes
        )
        executors = [
            AlephExecutor(node=node, vm_function=self.vm_function)
            async for node in whitelisted_nodes
        ]

        if len(executors) < nb_executors:
            raise NotEnoughExecutors(requested=nb_executors, available=len(executors))
        return random.sample(executors, nb_executors)


class UsePredeterminedExecutors(ExecutorSelectionPolicy):
    def __init__(self, executors: List[Executor]):
        self.executors = executors

    async def select_executors(self, nb_executors: int) -> List[Executor]:
        if len(self.executors) < nb_executors:
            raise NotEnoughExecutors(
                requested=nb_executors, available=len(self.executors)
            )

        # If we request fewer executors than available, return the N first executors.
        return self.executors[:nb_executors]
