import abc
import asyncio
import json
import logging
import random
from hashlib import sha3_256
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Type,
    TypeVar,
    Union,
    Iterator,
    Iterable,
    AsyncIterator,
    Optional,
)
from uuid import uuid4

import aiohttp
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.client import AuthenticatedAlephClient
from aleph_message.models import ItemHash
from aleph_message.status import MessageStatus
from pydantic import BaseModel
from pydantic.json import pydantic_encoder

from aleph_vrf.models import (
    CRNVRFResponse,
    Node,
    VRFRandomBytes,
    VRFRequest,
    VRFResponse,
    VRFResponseHash,
)
from aleph_vrf.settings import settings
from aleph_vrf.utils import (
    binary_to_bytes,
    bytes_to_int,
    generate_nonce,
    int_to_bytes,
    verify,
    xor_all,
)

VRF_FUNCTION_GENERATE_PATH = "generate"
VRF_FUNCTION_PUBLISH_PATH = "publish"


logger = logging.getLogger(__name__)


M = TypeVar("M", bound=BaseModel)


async def post_node_vrf(url: str, model: Type[M]) -> Union[Exception, M]:
    async with aiohttp.ClientSession() as session:
        async with session.post(url, timeout=60) as resp:
            if resp.status != 200:
                raise ValueError(f"VRF node request failed on {url}")

            response = await resp.json()

            return model.parse_obj(response["data"])


async def _get_corechannel_aggregate() -> Dict[str, Any]:
    async with aiohttp.ClientSession(settings.API_HOST) as session:
        url = (
            f"/api/v0/aggregates/{settings.CORECHANNEL_AGGREGATE_ADDRESS}.json?"
            f"keys={settings.CORECHANNEL_AGGREGATE_KEY}"
        )
        async with session.get(url) as response:
            if response.status != 200:
                raise ValueError(f"CRN list not available")

            return await response.json()


def _get_unauthorized_node_list() -> List[str]:
    unauthorized_nodes_list_path = Path(__file__).with_name(
        "unauthorized_node_list.json"
    )
    if unauthorized_nodes_list_path.is_file():
        with open(unauthorized_nodes_list_path, "rb") as fd:
            return json.load(fd)

    return []


async def select_random_nodes(
    node_amount: int, unauthorized_nodes: List[str]
) -> Iterable[Node]:
    node_list: List[Node] = []

    content = await _get_corechannel_aggregate()

    if (
        not content["data"]["corechannel"]
        or not content["data"]["corechannel"]["resource_nodes"]
    ):
        raise ValueError(f"Bad CRN list format")

    resource_nodes = content["data"]["corechannel"]["resource_nodes"]

    for resource_node in resource_nodes:
        # Filter nodes by score, with linked status and remove unauthorized nodes
        if (
            resource_node["status"] == "linked"
            and resource_node["score"] > 0.9
            and resource_node["address"].strip("/") not in unauthorized_nodes
        ):
            node_address = resource_node["address"].strip("/")
            node = Node(
                hash=resource_node["hash"],
                address=node_address,
                score=resource_node["score"],
            )
            node_list.append(node)

    if len(node_list) < node_amount:
        raise ValueError(
            f"Not enough CRNs linked, only {len(node_list)} available from {node_amount} requested"
        )

    # Randomize node order
    return random.sample(node_list, min(node_amount, len(node_list)))


class ExecutorSelectionPolicy(abc.ABC):
    @abc.abstractmethod
    async def select_nodes(self, nb_executors: int) -> List[Node]:
        ...


class ExecuteOnAleph(ExecutorSelectionPolicy):
    def __init__(self, vm_function: str):
        self.vm_function = vm_function

    @staticmethod
    async def _list_compute_nodes() -> AsyncIterator[Node]:
        content = await _get_corechannel_aggregate()

        if (
            not content["data"]["corechannel"]
            or not content["data"]["corechannel"]["resource_nodes"]
        ):
            raise ValueError(f"Bad CRN list format")

        resource_nodes = content["data"]["corechannel"]["resource_nodes"]

        for resource_node in resource_nodes:
            # Filter nodes by score, with linked status and remove unauthorized nodes
            if (
                resource_node["status"] == "linked"
                and resource_node["score"] > 0.9
                # and resource_node["address"].strip("/") not in unauthorized_nodes
            ):
                node_address = resource_node["address"].strip("/")
                node = Node(
                    hash=resource_node["hash"],
                    address=node_address,
                    score=resource_node["score"],
                )
                yield node

    @staticmethod
    def _get_unauthorized_node_list() -> List[str]:
        unauthorized_nodes_list_path = Path(__file__).with_name(
            "unauthorized_node_list.json"
        )
        if unauthorized_nodes_list_path.is_file():
            with open(unauthorized_nodes_list_path, "rb") as fd:
                return json.load(fd)

        return []

    async def select_nodes(self, nb_executors: int) -> List[Node]:
        compute_nodes = self._list_compute_nodes()
        blacklisted_nodes = self._get_unauthorized_node_list()
        whitelisted_nodes = [
            node
            async for node in compute_nodes
            if node.address not in blacklisted_nodes
        ]

        if len(whitelisted_nodes) < nb_executors:
            raise ValueError(
                f"Not enough CRNs linked, only {len(whitelisted_nodes)} "
                f"available from {nb_executors} requested"
            )
        return random.sample(whitelisted_nodes, nb_executors)


async def _generate_vrf(
    account: ETHAccount,
    nb_executors: int,
    executor_selection_policy: ExecutorSelectionPolicy,
    nb_bytes: int,
    vrf_function: str,
) -> VRFResponse:
    selected_nodes = await executor_selection_policy.select_nodes(nb_executors)
    selected_node_list_json = json.dumps(
        selected_nodes, default=pydantic_encoder
    ).encode(encoding="utf-8")

    nonce = generate_nonce()

    vrf_request = VRFRequest(
        nb_bytes=nb_bytes,
        nb_executors=nb_executors,
        nonce=nonce,
        vrf_function=ItemHash(vrf_function),
        request_id=str(uuid4()),
        node_list_hash=sha3_256(selected_node_list_json).hexdigest(),
    )

    ref = f"vrf_{vrf_request.request_id}_request"

    request_item_hash = await publish_data(vrf_request, ref, account)

    logger.debug(f"Generated VRF request with item_hash {request_item_hash}")

    vrf_generated_result = await send_generate_requests(
        selected_nodes, request_item_hash
    )

    logger.debug(
        f"Received VRF generated requests from {len(vrf_generated_result)} nodes"
    )

    vrf_publish_result = await send_publish_requests(
        vrf_generated_result, vrf_request.request_id
    )

    logger.debug(
        f"Received VRF publish requests from {len(vrf_generated_result)} nodes"
    )

    vrf_response = generate_final_vrf(
        nb_executors,
        nonce,
        vrf_generated_result,
        vrf_publish_result,
        vrf_request,
    )

    ref = f"vrf_{vrf_response.request_id}"

    logger.debug(f"Publishing final VRF summary")

    response_item_hash = await publish_data(vrf_response, ref, account)

    vrf_response.message_hash = response_item_hash

    return vrf_response


async def generate_vrf(
    account: ETHAccount,
    nb_executors: Optional[int] = None,
    executor_selection_policy: Optional[ExecutorSelectionPolicy] = None,
    nb_bytes: Optional[int] = None,
) -> VRFResponse:
    vrf_function = settings.VRF_FUNCTION

    nb_executors = nb_executors or settings.NB_EXECUTORS
    executor_selection_policy = executor_selection_policy or ExecuteOnAleph(
        vm_function=vrf_function
    )
    nb_bytes = nb_bytes or settings.NB_BYTES

    return await _generate_vrf(
        account=account,
        nb_executors=nb_executors,
        executor_selection_policy=executor_selection_policy,
        nb_bytes=nb_bytes,
        vrf_function=vrf_function
    )


async def send_generate_requests(
    selected_nodes: List[Node], request_item_hash: str
) -> Dict[str, Union[Exception, VRFResponseHash]]:
    generate_tasks = []
    nodes: List[str] = []
    for node in selected_nodes:
        nodes.append(node.address)
        url = f"{node.address}/vm/{settings.FUNCTION}/{VRF_FUNCTION_GENERATE_PATH}/{request_item_hash}"
        generate_tasks.append(asyncio.create_task(post_node_vrf(url, VRFResponseHash)))

    vrf_generated_responses = await asyncio.gather(
        *generate_tasks, return_exceptions=True
    )
    return dict(zip(nodes, vrf_generated_responses))


async def send_publish_requests(
    vrf_generated_result: Dict[str, VRFResponseHash],
    request_id: str,
) -> Dict[str, Union[Exception, VRFRandomBytes]]:
    publish_tasks = []
    nodes: List[str] = []
    for node, vrf_generated_response in vrf_generated_result.items():
        nodes.append(node)
        if isinstance(vrf_generated_response, Exception):
            raise ValueError(
                f"Generate response not found for Node {node} on request_id {request_id}"
            )

        node_message_hash = vrf_generated_response.message_hash
        url = (
            f"{node}/vm/{settings.FUNCTION}"
            f"/{VRF_FUNCTION_PUBLISH_PATH}/{node_message_hash}"
        )
        publish_tasks.append(asyncio.create_task(post_node_vrf(url, VRFRandomBytes)))

    vrf_publish_responses = await asyncio.gather(*publish_tasks, return_exceptions=True)
    return dict(zip(nodes, vrf_publish_responses))


def generate_final_vrf(
    nb_executors: int,
    nonce: int,
    vrf_generated_result: Dict[str, VRFResponseHash],
    vrf_publish_result: Dict[str, VRFRandomBytes],
    vrf_request: VRFRequest,
) -> VRFResponse:
    nodes_responses = []
    random_numbers_list = []
    for node, vrf_publish_response in vrf_publish_result.items():
        if isinstance(vrf_publish_response, Exception):
            raise ValueError(f"Publish response not found for {node}")

        if (
            vrf_generated_result[node].random_bytes_hash
            != vrf_publish_response.random_bytes_hash
        ):
            generated_hash = vrf_publish_response.random_bytes_hash
            publish_hash = vrf_publish_response.random_bytes_hash
            raise ValueError(
                f"Publish response hash ({publish_hash})"
                f"different from generated one ({generated_hash})"
            )

        verified = verify(
            binary_to_bytes(vrf_publish_response.random_bytes),
            nonce,
            vrf_publish_response.random_bytes_hash,
        )
        if not verified:
            execution = vrf_publish_response.execution_id
            raise ValueError(f"Failed hash verification for {execution}")

        random_numbers_list.append(
            int_to_bytes(int(vrf_publish_response.random_number))
        )

        node_response = CRNVRFResponse(
            url=node,
            execution_id=vrf_publish_response.execution_id,
            random_number=str(vrf_publish_response.random_number),
            random_bytes=vrf_publish_response.random_bytes,
            random_bytes_hash=vrf_generated_result[node].random_bytes_hash,
            generation_message_hash=vrf_generated_result[node].message_hash,
            publish_message_hash=vrf_publish_response.message_hash,
        )
        nodes_responses.append(node_response)

    final_random_nb_bytes = xor_all(random_numbers_list)
    final_random_number = bytes_to_int(final_random_nb_bytes)

    return VRFResponse(
        nb_bytes=settings.NB_BYTES,
        nb_executors=nb_executors,
        nonce=nonce,
        vrf_function=settings.FUNCTION,
        request_id=vrf_request.request_id,
        nodes=nodes_responses,
        random_number=str(final_random_number),
    )


async def publish_data(
    data: Union[VRFRequest, VRFResponse], ref: str, account: ETHAccount
) -> ItemHash:
    channel = f"vrf_{data.request_id}"

    logger.debug(f"Publishing message to {settings.API_HOST}")

    async with AuthenticatedAlephClient(
        account=account, api_server=settings.API_HOST, allow_unix_sockets=False
    ) as client:
        message, status = await client.create_post(
            post_type="vrf_library_post",
            post_content=data,
            channel=channel,
            ref=ref,
            sync=True,
        )

        if status != MessageStatus.PROCESSED:
            raise ValueError(
                f"Message could not be processed for ref {ref} and item_hash {message.item_hash}"
            )

        return message.item_hash
