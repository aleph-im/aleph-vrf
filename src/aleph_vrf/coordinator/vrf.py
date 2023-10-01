import asyncio
import json
import logging
from hashlib import sha3_256
from typing import Dict, List, Type, TypeVar, Union, Optional
from uuid import uuid4

import aiohttp
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.client import AuthenticatedAlephClient
from aleph_message.models import ItemHash
from aleph_message.status import MessageStatus
from pydantic import BaseModel
from pydantic.json import pydantic_encoder

from aleph_vrf.coordinator.executor_selection import (
    ExecuteOnAleph,
    ExecutorSelectionPolicy,
)
from aleph_vrf.exceptions import (
    HashValidationFailed,
    AlephNetworkError,
    ExecutorHttpError,
    RandomNumberPublicationFailed,
    RandomNumberGenerationFailed,
    HashesDoNotMatch,
)
from aleph_vrf.models import (
    CRNVRFResponse,
    VRFRequest,
    VRFResponse,
    PublishedVRFResponseHash,
    PublishedVRFRandomBytes,
    Executor,
    PublishedVRFResponse,
)
from aleph_vrf.settings import settings
from aleph_vrf.types import RequestId, Nonce
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


async def post_node_vrf(url: str, model: Type[M]) -> M:
    async with aiohttp.ClientSession() as session:
        async with session.post(url, timeout=60) as resp:
            if resp.status != 200:
                raise ExecutorHttpError(
                    url=url, status_code=resp.status, response_text=await resp.text()
                )

            response = await resp.json()

            return model.parse_obj(response["data"])


async def _generate_vrf(
    aleph_client: AuthenticatedAlephClient,
    nb_executors: int,
    nb_bytes: int,
    vrf_function: ItemHash,
    executor_selection_policy: ExecutorSelectionPolicy,
) -> PublishedVRFResponse:
    executors = await executor_selection_policy.select_executors(nb_executors)
    selected_nodes_json = json.dumps(
        [executor.node for executor in executors], default=pydantic_encoder
    ).encode(encoding="utf-8")

    nonce = generate_nonce()

    vrf_request = VRFRequest(
        nb_bytes=nb_bytes,
        nb_executors=nb_executors,
        nonce=nonce,
        vrf_function=vrf_function,
        request_id=RequestId(str(uuid4())),
        node_list_hash=sha3_256(selected_nodes_json).hexdigest(),
    )

    ref = f"vrf_{vrf_request.request_id}_request"

    request_item_hash = await publish_data(
        aleph_client=aleph_client, data=vrf_request, ref=ref
    )

    logger.debug(f"Generated VRF request with item_hash {request_item_hash}")

    vrf_generated_result = await send_generate_requests(
        executors=executors,
        request_item_hash=request_item_hash,
        request_id=vrf_request.request_id,
    )

    logger.debug(
        f"Received VRF generated requests from {len(vrf_generated_result)} executors"
    )

    vrf_publish_result = await send_publish_requests(vrf_generated_result)

    logger.debug(
        f"Received VRF publish requests from {len(vrf_generated_result)} executors"
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

    response_item_hash = await publish_data(
        aleph_client=aleph_client, data=vrf_response, ref=ref
    )

    published_response = PublishedVRFResponse.from_vrf_response(
        vrf_response=vrf_response, message_hash=response_item_hash
    )
    return published_response


async def generate_vrf(
    account: ETHAccount,
    nb_executors: Optional[int] = None,
    nb_bytes: Optional[int] = None,
    vrf_function: Optional[ItemHash] = None,
    aleph_api_server: Optional[str] = None,
    executor_selection_policy: Optional[ExecutorSelectionPolicy] = None,
):
    vrf_function = vrf_function or settings.FUNCTION

    async with AuthenticatedAlephClient(
        account=account, api_server=aleph_api_server or settings.API_HOST
    ) as aleph_client:
        return await _generate_vrf(
            aleph_client=aleph_client,
            nb_executors=nb_executors or settings.NB_EXECUTORS,
            nb_bytes=nb_bytes or settings.NB_BYTES,
            vrf_function=vrf_function or settings.FUNCTION,
            executor_selection_policy=executor_selection_policy
            or ExecuteOnAleph(vm_function=vrf_function),
        )


async def send_generate_requests(
    executors: List[Executor],
    request_item_hash: ItemHash,
    request_id: RequestId,
) -> Dict[Executor, PublishedVRFResponseHash]:
    generate_tasks = []
    for executor in executors:
        url = f"{executor.api_url}/{VRF_FUNCTION_GENERATE_PATH}/{request_item_hash}"
        generate_tasks.append(
            asyncio.create_task(post_node_vrf(url, PublishedVRFResponseHash))
        )

    vrf_generated_responses = await asyncio.gather(
        *generate_tasks, return_exceptions=True
    )
    generate_results = dict(zip(executors, vrf_generated_responses))

    for executor, result in generate_results.items():
        if isinstance(result, Exception):
            raise RandomNumberGenerationFailed(executor=executor) from result

    return generate_results


async def send_publish_requests(
    vrf_generated_result: Dict[Executor, PublishedVRFResponseHash],
) -> Dict[Executor, PublishedVRFRandomBytes]:
    publish_tasks = []
    executors: List[Executor] = []

    for executor, vrf_generated_response in vrf_generated_result.items():
        executors.append(executor)

        node_message_hash = vrf_generated_response.message_hash
        url = f"{executor.api_url}/{VRF_FUNCTION_PUBLISH_PATH}/{node_message_hash}"
        publish_tasks.append(
            asyncio.create_task(post_node_vrf(url, PublishedVRFRandomBytes))
        )

    vrf_publish_responses = await asyncio.gather(*publish_tasks, return_exceptions=True)
    publish_results = dict(zip(executors, vrf_publish_responses))

    for executor, result in publish_results.items():
        if isinstance(result, Exception):
            raise RandomNumberPublicationFailed(executor=executor) from result

    return publish_results


def generate_final_vrf(
    nb_executors: int,
    nonce: Nonce,
    vrf_generated_result: Dict[Executor, PublishedVRFResponseHash],
    vrf_publish_result: Dict[Executor, PublishedVRFRandomBytes],
    vrf_request: VRFRequest,
) -> VRFResponse:
    nodes_responses = []
    random_numbers_list = []
    for executor, vrf_publish_response in vrf_publish_result.items():
        if (generation_hash := vrf_generated_result[executor].random_bytes_hash) != (
            publication_hash := vrf_publish_response.random_bytes_hash
        ):
            raise HashesDoNotMatch(
                executor=executor,
                generation_hash=generation_hash,
                publication_hash=publication_hash,
            )

        verified = verify(
            binary_to_bytes(vrf_publish_response.random_bytes),
            nonce,
            generation_hash,
        )
        if not verified:
            raise HashValidationFailed(
                random_bytes=vrf_publish_response,
                random_number_hash=generation_hash,
                executor=executor,
            )

        random_numbers_list.append(
            int_to_bytes(int(vrf_publish_response.random_number), n=vrf_request.nb_bytes)
        )

        node_response = CRNVRFResponse(
            url=executor.node.address,
            execution_id=vrf_publish_response.execution_id,
            random_number=str(vrf_publish_response.random_number),
            random_bytes=vrf_publish_response.random_bytes,
            random_bytes_hash=vrf_generated_result[executor].random_bytes_hash,
            generation_message_hash=vrf_generated_result[executor].message_hash,
            publish_message_hash=vrf_publish_response.message_hash,
        )
        nodes_responses.append(node_response)

    final_random_nb_bytes = xor_all(random_numbers_list)
    final_random_number = bytes_to_int(final_random_nb_bytes)

    return VRFResponse(
        nb_bytes=vrf_request.nb_bytes,
        nb_executors=nb_executors,
        nonce=nonce,
        vrf_function=vrf_request.vrf_function,
        request_id=vrf_request.request_id,
        nodes=nodes_responses,
        random_number=str(final_random_number),
    )


async def publish_data(
    aleph_client: AuthenticatedAlephClient,
    data: Union[VRFRequest, VRFResponse],
    ref: str,
) -> ItemHash:
    channel = f"vrf_{data.request_id}"

    logger.debug(f"Publishing message to {aleph_client.api_server}")

    message, status = await aleph_client.create_post(
        post_type="vrf_library_post",
        post_content=data,
        channel=channel,
        ref=ref,
        sync=True,
    )

    if status != MessageStatus.PROCESSED:
        raise AlephNetworkError(
            f"Message could not be processed for ref {ref} and item_hash {message.item_hash}"
        )

    return message.item_hash
