import abc
import json
import logging
import random
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional

import aiohttp
import math
from aleph_message.models import ItemHash

from aleph_vrf.exceptions import AlephNetworkError, NotEnoughExecutors
from aleph_vrf.models import AlephExecutor, ComputeResourceNode, Executor, VRFExecutor
from aleph_vrf.settings import settings
from aleph_vrf.utils import percentile

logger = logging.getLogger(__name__)


class ExecutorSelectionPolicy(abc.ABC):
    """
    How the coordinator selects executors.
    """

    @abc.abstractmethod
    async def select_executors(self, nb_executors: int) -> List[Executor]:
        """
        Returns nb_executors executor objects.
        Raises NotEnoughExecutors if there are fewer than nb_executors available.
        """
        ...


async def _get_corechannel_aggregate() -> Dict[str, Any]:
    """
    Returns the "corechannel" aleph.im aggregate.
    This aggregate contains an up-to-date list of staked nodes on the network.
    """
    async with aiohttp.ClientSession(settings.API_HOST) as session:
        url = (
            f"/api/v0/aggregates/{settings.CORECHANNEL_AGGREGATE_ADDRESS}.json?"
            f"keys={settings.CORECHANNEL_AGGREGATE_KEY}"
        )
        async with session.get(url) as response:
            if response.status != 200:
                raise AlephNetworkError(f"CRN list not available")

            return await response.json()


async def _get_unauthorized_node_list_aggregate(aggregate_address: str) -> List[str]:
    """
    Returns the "vrf_unauthorized_nodes" list aggregate.
    This aggregate contains an up-to-date list of nodes not allowed to run a VRF request.
    """
    async with aiohttp.ClientSession(settings.API_HOST) as session:
        url = (
            f"/api/v0/aggregates/{aggregate_address}.json?"
            f"keys={settings.VRF_AGGREGATE_KEY}"
        )
        async with session.get(url) as response:
            if response.status != 200:
                logger.debug("No VRF unauthorized nodes list found")
                return []

            content = await response.json()

            if (
                not content["data"]["vrf"]
                or not content["data"]["vrf"]["unauthorized_nodes"]
            ):
                logger.error(f"Bad VRF unauthorized nodes list format")
                return []

            unauthorized_nodes = content["data"]["vrf"]["unauthorized_nodes"]

            unauthorized_list = [str(unauthorized_node) for unauthorized_node in unauthorized_nodes]
            return unauthorized_list


class ExecuteOnAleph(ExecutorSelectionPolicy):
    """
    Select executors at random on the aleph.im network.
    """

    def __init__(
            self,
            vm_function: ItemHash,
            aggregate_address: Optional[str] = None,
            crn_score_threshold: Optional[float] = None
    ):
        self.vm_function = vm_function
        self.crn_score_threshold = crn_score_threshold
        self.aggregate_address = aggregate_address

    async def _list_compute_nodes(self) -> AsyncIterator[ComputeResourceNode]:
        """
        Returns a list of all compute resource nodes that are linked to a core channel node
        and have a score above the required threshold.
        """

        content = await _get_corechannel_aggregate()

        if (
            not content["data"]["corechannel"]
            or not content["data"]["corechannel"]["resource_nodes"]
        ):
            raise AlephNetworkError(f"Bad CRN list format")

        resource_nodes = content["data"]["corechannel"]["resource_nodes"]

        if not self.crn_score_threshold:
            self.crn_score_threshold = self._get_minimum_score_threshold(resource_nodes)
            print(f"Filtering CRNs with score better than {self.crn_score_threshold}")

        for resource_node in resource_nodes:
            # Filter nodes by score, with linked status
            if (
                resource_node["status"] == "linked"
                and resource_node["score"] >= self.crn_score_threshold
            ):
                node_address = resource_node["address"].strip("/")
                node = ComputeResourceNode(
                    hash=resource_node["hash"],
                    address=node_address,
                    score=resource_node["score"],
                )
                yield node

    @staticmethod
    def _get_minimum_score_threshold(
        resource_nodes: List[Dict[str, Any]],
        percentile_value: int = 75
    ) -> float:
        """
        Returns the 75 percentile of all CRN scores as a minimum score threshold
        """
        # Returns score and filter by linked status
        scores = [resource_node["score"] for resource_node in resource_nodes if resource_node["status"] == "linked"]

        score_percentile = percentile(scores, percentile_value)
        # Round down minimum score to 3 decimals
        rounded_score = math.floor(score_percentile * 1000) / 1000
        return rounded_score

    @staticmethod
    def _get_unauthorized_nodes_file(unauthorized_nodes_list_path: Optional[Path]) -> List[str]:
        """
        Returns a list of unauthorized nodes.
        The caller may provide a blacklist of nodes by specifying a list of URLs in a file
        named `unauthorized_node_list.json` in the working directory.
        """

        if not unauthorized_nodes_list_path:
            unauthorized_nodes_list_path = Path(__file__).with_name(
                "unauthorized_node_list.json"
            )
        if unauthorized_nodes_list_path.is_file():
            with open(unauthorized_nodes_list_path, "rb") as fd:
                return json.load(fd)

        return []

    async def _get_unauthorized_nodes(self, unauthorized_nodes_list_path: Optional[Path] = None) -> List[str]:
        """
        Returns a list of unauthorized nodes.
        The caller may provide a blacklist of nodes by specifying a list of URLs in a file
        named `unauthorized_node_list.json` in the working directory.
        """
        aggregate_unauthorized_list = []
        aggregate_address = self.aggregate_address or settings.VRF_AGGREGATE_ADDRESS
        if aggregate_address:
            aggregate_unauthorized_list = await _get_unauthorized_node_list_aggregate(aggregate_address)

        file_unauthorized_nodes_list = self._get_unauthorized_nodes_file(
            unauthorized_nodes_list_path=unauthorized_nodes_list_path
        )

        return aggregate_unauthorized_list + file_unauthorized_nodes_list

    async def select_executors(self, nb_executors: int) -> List[VRFExecutor]:
        """
        Selects nb_executors compute resource nodes at random from the aleph.im network.
        """

        compute_nodes = self._list_compute_nodes()
        blacklisted_nodes = await self._get_unauthorized_nodes()

        executors = [
            AlephExecutor(node=node, vm_function=self.vm_function)
            async for node in compute_nodes
            if node.address not in blacklisted_nodes
        ]

        if len(executors) < nb_executors:
            raise NotEnoughExecutors(requested=nb_executors, available=len(executors))
        return random.sample(executors, nb_executors)

    async def get_candidate_executors(self) -> List[VRFExecutor]:
        compute_nodes = self._list_compute_nodes()
        blacklisted_nodes = await self._get_unauthorized_nodes()
        executors: List[VRFExecutor] = [
            AlephExecutor(node=node, vm_function=self.vm_function)
            async for node in compute_nodes
            if node.address not in blacklisted_nodes
        ]

        return executors

    async def get_all_executors(self) -> List[VRFExecutor]:
        compute_nodes = self._list_compute_nodes()
        executors: List[VRFExecutor] = [
            AlephExecutor(node=node, vm_function=self.vm_function)
            async for node in compute_nodes
        ]

        return executors


class UsePredeterminedExecutors(ExecutorSelectionPolicy):
    """
    Use a hardcoded list of executors.
    """

    def __init__(self, executors: List[VRFExecutor]):
        self.executors = executors

    async def select_executors(self, nb_executors: int) -> List[VRFExecutor]:
        """
        Returns nb_executors from the hardcoded list of executors.
        If nb_executors is lower than the total number of executors, this method
        will always return the nb_executors first executors in the list.
        """

        if len(self.executors) < nb_executors:
            raise NotEnoughExecutors(
                requested=nb_executors, available=len(self.executors)
            )

        # If we request fewer executors than available, return the N first executors.
        return self.executors[:nb_executors]
