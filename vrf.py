import asyncio
from hashlib import sha3_256
from random import shuffle
from typing import Dict, List, Union

import aiohttp
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.client import AuthenticatedAlephClient
from aleph_message.status import MessageStatus

from models import (
    CRNVRFResponse,
    Node,
    VRFRandomBytes,
    VRFRequest,
    VRFResponse,
    VRFResponseHash,
)
from utils import bytes_to_int, generate_nonce, int_to_bytes, verify, xor_all

# TODO: Use environment settings
API_HOST = "https://api2.aleph.im"
API_PATH = "api/v0/aggregates/0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10.json?keys=corechannel&limit=50"
NUM_RANDOM_NODES = 32
NUM_BYTES = 32
VRF_FUNCTION = "0x111111111111111111111111"
VRF_FUNCTION_GENERATE_PATH = "/generate"
VRF_FUNCTION_PUBLISH_PATH = "/publish"


async def post_node_vrf(session, url):
    async with session.post(url) as resp:
        if resp.status != 200:
            raise ValueError(f"VRF node request failed on {url}")

        response = await resp.json()
        return response["data"]


async def select_random_nodes(node_amount: int) -> List[Node]:
    node_list: List[Node] = []

    async with aiohttp.ClientSession() as session:
        url = f"{API_HOST}/{API_PATH}"
        async with session.get(url) as response:
            if response.status != 200:
                raise ValueError(f"CRN list not available")

            content = await response.json()

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
            shuffle(node_list)

            random_nodes: List[Node] = []
            for node in range(node_amount):
                random_nodes.append(node_list[node])

            return random_nodes


async def generate_vrf(account: ETHAccount) -> VRFResponse:
    selected_nodes = await select_random_nodes(NUM_RANDOM_NODES)

    nonce = generate_nonce()

    vrf_request = VRFRequest(
        num_bytes=NUM_BYTES,
        nonce=nonce,
        vrf_function=VRF_FUNCTION,
        nodes=sha3_256(selected_nodes),
    )

    ref = f"vrf_{vrf_request.request_id.__str__()}_request"

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

        ref = f"vrf_{vrf_response.request_id.__str__()}"

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

    final_random_number_bytes = xor_all(random_numbers_list)
    final_random_number = bytes_to_int(final_random_number_bytes)

    return VRFResponse(
        num_bytes=NUM_BYTES,
        nonce=nonce,
        vrf_function=VRF_FUNCTION,
        request_id=vrf_request.request_id,
        nodes=nodes_responses,
        random_number=final_random_number,
    )


async def publish_data(
    data: Union[VRFRequest, VRFResponse], ref: str, account: ETHAccount
):
    channel = f"vrf_{data.request_id.__str__()}"

    async with AuthenticatedAlephClient(account=account, api_server=API_HOST) as client:
        message, status = await client.create_post(
            post_type="vrf_library_post",
            post_content=data,
            channel=channel,
            ref=ref,
        )

        if status != MessageStatus.PROCESSED:
            raise ValueError(f"Message could not be processed for ref {ref}")

        return message.item_hash.__str__()
