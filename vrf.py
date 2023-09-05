import aiohttp
import asyncio

from hashlib import sha3_256
from typings import Union

from aleph.sdk.client import AlephClient, AuthenticatedAlephClient
from aleph.sdk.chains.ethereum import ETHAccount

from models import (
    VRFRequest,
    VRFResponse,
    CRNVRFResponse,
)
from utils import first, generate_nonce, verify, xor_all, bytes_to_int, int_to_bytes

# TODO: Use environment settings
API_HOST = "https://api2.aleph.im"
NUM_BYTES = 32
VRF_FUNCTION = "0x111111111111111111111111"
VRF_FUNCTION_GENERATE_PATH = "/generate"
VRF_FUNCTION_PUBLISH_PATH = "/publish"


async def post_node_vrf(session, url):
    async with session.post(url) as resp:
        response = await resp.json()
        return response['data']


async def select_random_nodes():
    return []


async def generate_vrf(account: ETHAccount) -> VRFResponse:
    # TODO: Select random nodes
    selected_nodes = select_random_nodes()

    nonce = generate_nonce()

    vrf_request = VRFRequest(
        num_bytes=NUM_BYTES,
        nonce=nonce,
        vrf_function=VRF_FUNCTION,
        nodes=sha3_256(selected_nodes),
    )

    ref = f"vrf_{vrf_request.requestId.__str__()}_request"

    request_item_hash = await publish_data(vrf_request, ref, account)

    generate_tasks = []
    publish_tasks = []

    async with aiohttp.ClientSession() as session:

        for node in selected_nodes:
            url = f'{node}/{VRF_FUNCTION_GENERATE_PATH}/{request_item_hash}'
            generate_tasks.append(asyncio.ensure_future(post_node_vrf(session, url)))

        vrf_generate_responses = await asyncio.gather(*generate_tasks)

        for node in selected_nodes:
            vrf_generate_response = first(x for x in vrf_generate_responses if x.url.find(node) != -1)
            if not vrf_generate_response:
                raise ValueError(f"Generate response not found for {node}")

            url = f'{node}/{VRF_FUNCTION_PUBLISH_PATH}/{vrf_generate_response.message_hash}'
            publish_tasks.append(asyncio.ensure_future(post_node_vrf(session, url)))

        vrf_publish_responses = await asyncio.gather(*publish_tasks)

        nodes_responses = []
        random_numbers_list = []
        for vrf_publish_response in vrf_publish_responses:
            vrf_generate_response = first(x for x in vrf_generate_responses if x.url.find(vrf_publish_response.url) != -1)
            if not vrf_generate_response:
                raise ValueError(f"Publish response not found for {vrf_publish_response.url}")

            if vrf_generate_response.random_bytes_hash != vrf_publish_response.random_bytes_hash:
                raise ValueError(f"Publish response hash ({vrf_publish_response.random_bytes_hash})"
                                 f"different from generated one ({vrf_generate_response.random_bytes_hash})")

            verified = verify(vrf_publish_response.random_bytes, nonce, vrf_publish_response.random_bytes_hash)
            if not verified:
                raise ValueError(f"Failed hash verification for {vrf_publish_response.url}")

            random_numbers_list.append(int_to_bytes(vrf_publish_response.random_number))

            node_response = CRNVRFResponse(
                url=vrf_publish_response.url,
                execution_id=vrf_publish_response.execution_id,
                random_number=vrf_publish_response.random_number,
                random_bytes=vrf_publish_response.random_bytes,
                random_bytes_hash=vrf_generate_response.random_bytes_hash,
            )
            nodes_responses.append(node_response)

        final_random_number_bytes = xor_all(random_numbers_list)
        final_random_number = bytes_to_int(final_random_number_bytes)

        vrf_response = VRFResponse(
            num_bytes=NUM_BYTES,
            nonce=nonce,
            vrf_function=VRF_FUNCTION,
            requestId=vrf_request.requestId,
            nodes=nodes_responses,
            random_number=final_random_number,
        )

        ref = f"vrf_{vrf_response.requestId.__str__()}"

        response_item_hash = await publish_data(vrf_response, ref, account)

        vrf_response.message_hash = response_item_hash

        return vrf_response


async def publish_data(data: Union[VRFRequest, VRFResponse], ref: str, account: ETHAccount):

    channel = f"vrf_{data.requestId.__str__()}"

    async with AuthenticatedAlephClient(
            account=account, api_server=API_HOST
    ) as client:
        message, status = await client.create_post(
            content=data,
            channel=channel,
            ref=ref,
        )

        # TODO: Check message status
        return message["item_hash"]
