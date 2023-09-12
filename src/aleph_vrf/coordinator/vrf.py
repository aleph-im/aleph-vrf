import asyncio
import json
import random
from hashlib import sha3_256
from typing import Any, Dict, List, Union
from uuid import UUID, uuid4

import aiohttp
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.client import AuthenticatedAlephClient
from aleph_message.models import ItemHash
from aleph_message.status import MessageStatus
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
from aleph_vrf.utils import bytes_to_int, generate_nonce, int_to_bytes, verify, xor_all

VRF_FUNCTION_GENERATE_PATH = "/generate"
VRF_FUNCTION_PUBLISH_PATH = "/publish"


async def post_node_vrf(session, url):
    async with session.post(url) as resp:
        if resp.status != 200:
            raise ValueError(f"VRF node request failed on {url}")

        response = await resp.json()
        return response["data"]


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


async def select_random_nodes(node_amount: int) -> List[Node]:
    node_list: List[Node] = []

    content = await _get_corechannel_aggregate()

    if (
        not content["data"]["corechannel"]
        or not content["data"]["corechannel"]["resource_nodes"]
    ):
        raise ValueError(f"Bad CRN list format")

    resource_nodes = content["data"]["corechannel"]["resource_nodes"]

    for resource_node in resource_nodes:
        node = Node(
            hash=resource_node["hash"],
            address=resource_node["address"],
            score=resource_node["score"],
        )
        node_list.append(node)

    # Randomize node order
    return random.sample(node_list, min(node_amount, len(node_list)))


async def select_nodes() -> List[Node]:
    if settings.EXECUTORS == "dynamic":
        return await select_random_nodes(settings.NB_EXECUTORS)

    return settings.EXECUTORS


async def generate_vrf(account: ETHAccount) -> VRFResponse:
    selected_nodes = await select_nodes()
    selected_node_list = json.dumps(selected_nodes, default=pydantic_encoder).encode(
        encoding="utf-8"
    )

    nonce = generate_nonce()

    vrf_request = VRFRequest(
        nb_bytes=settings.NB_BYTES,
        nonce=nonce,
        vrf_function=ItemHash(settings.EXECUTOR_VM_HASH),
        request_id=str(uuid4()),
        node_list_hash=sha3_256(selected_node_list).hexdigest(),
    )

    ref = f"vrf_{vrf_request.request_id}_request"

    request_item_hash = await publish_data(vrf_request, ref, account)

    async with aiohttp.ClientSession() as session:
        vrf_generated_result = await send_generate_requests(
            session, selected_nodes, request_item_hash
        )

        vrf_publish_result = await send_publish_requests(
            session, selected_nodes, vrf_generated_result
        )

        vrf_response = generate_final_vrf(
            nonce,
            vrf_generated_result,
            vrf_publish_result,
            vrf_request,
        )

        ref = f"vrf_{vrf_response.request_id}"

        response_item_hash = await publish_data(vrf_response, ref, account)

        vrf_response.message_hash = response_item_hash

        return vrf_response


async def send_generate_requests(
    session: aiohttp.ClientSession, selected_nodes: List[Node], request_item_hash: str
) -> Dict[Node, VRFResponseHash]:
    generate_tasks = []
    for node in selected_nodes:
        url = f"{node.address}/{VRF_FUNCTION_GENERATE_PATH}/{request_item_hash}"
        generate_tasks.append(asyncio.create_task(post_node_vrf(session, url)))

    vrf_generated_responses = await asyncio.gather(
        *generate_tasks, return_exceptions=True
    )
    return dict(zip(selected_nodes, vrf_generated_responses))


async def send_publish_requests(
    session: aiohttp.ClientSession,
    selected_nodes: List[Node],
    vrf_generated_result: Dict[Node, VRFResponseHash],
) -> Dict[Node, VRFRandomBytes]:
    publish_tasks = []
    for node, vrf_generated_response in vrf_generated_result:
        if isinstance(vrf_generated_response, Exception):
            raise ValueError(f"Generate response not found for Node {node.address}")
        else:
            url = f"{node.address}/{VRF_FUNCTION_PUBLISH_PATH}/{vrf_generated_response.message_hash}"
            publish_tasks.append(asyncio.create_task(post_node_vrf(session, url)))

    vrf_publish_responses = await asyncio.gather(*publish_tasks, return_exceptions=True)
    return dict(zip(selected_nodes, vrf_publish_responses))


def generate_final_vrf(
    nonce: int,
    vrf_generated_result: Dict[Node, VRFResponseHash],
    vrf_publish_result: Dict[Node, VRFRandomBytes],
    vrf_request: VRFRequest,
) -> VRFResponse:
    nodes_responses = []
    random_numbers_list = []
    for node, vrf_publish_response in vrf_publish_result:
        if isinstance(vrf_publish_response, Exception):
            raise ValueError(f"Publish response not found for {node.hash}")

        if (
            vrf_generated_result[node].random_bytes_hash
            != vrf_publish_response.random_bytes_hash
        ):
            raise ValueError(
                f"Publish response hash ({vrf_publish_response.random_bytes_hash})"
                f"different from generated one ({vrf_generated_result[node].random_bytes_hash})"
            )

        verified = verify(
            vrf_publish_response.random_bytes,
            nonce,
            vrf_publish_response.random_bytes_hash,
        )
        if not verified:
            raise ValueError(f"Failed hash verification for {vrf_publish_response.url}")

        random_numbers_list.append(int_to_bytes(vrf_publish_response.random_number))

        node_response = CRNVRFResponse(
            url=vrf_publish_response.url,
            node_hash=node.hash,
            execution_id=vrf_publish_response.execution_id,
            random_number=vrf_publish_response.random_number,
            random_bytes=vrf_publish_response.random_bytes,
            random_bytes_hash=vrf_generated_result[node].random_bytes_hash,
        )
        nodes_responses.append(node_response)

    final_random_nb_bytes = xor_all(random_numbers_list)
    final_random_number = bytes_to_int(final_random_nb_bytes)

    return VRFResponse(
        nb_bytes=settings.NB_BYTES,
        nonce=nonce,
        vrf_function=settings.EXECUTOR_VM_HASH,
        request_id=vrf_request.request_id,
        nodes=nodes_responses,
        random_number=final_random_number,
    )


async def publish_data(
    data: Union[VRFRequest, VRFResponse], ref: str, account: ETHAccount
) -> ItemHash:
    channel = f"vrf_{data.request_id}"

    async with AuthenticatedAlephClient(
        account=account, api_server=settings.API_HOST
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
