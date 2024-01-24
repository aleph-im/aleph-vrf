import asyncio
import json
import logging
from hashlib import sha3_256
from typing import Dict, List, Optional, Type, TypeVar, Union
from uuid import uuid4

import aiohttp
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.client import AuthenticatedAlephClient
from aleph_message.models import ItemHash, MessageType, PostMessage
from aleph_message.status import MessageStatus
from hexbytes import HexBytes
from pydantic import BaseModel
from pydantic.json import pydantic_encoder

from aleph_vrf.coordinator.executor_selection import (
    ExecuteOnAleph,
    ExecutorSelectionPolicy,
)
from aleph_vrf.exceptions import (
    AlephNetworkError,
    ExecutorHttpError,
    HashesDoNotMatch,
    HashValidationFailed,
    PublishedHashesDoNotMatch,
    PublishedHashValidationFailed,
    RandomNumberGenerationFailed,
    RandomNumberPublicationFailed,
)
from aleph_vrf.models import (
    Executor,
    ExecutorVRFResponse,
    PublishedVRFRandomNumber,
    PublishedVRFRandomNumberHash,
    PublishedVRFResponse,
    VRFRequest,
    VRFResponse,
)
from aleph_vrf.settings import settings
from aleph_vrf.types import Nonce, RequestId
from aleph_vrf.utils import generate_nonce, verify, xor_all

VRF_FUNCTION_GENERATE_PATH = "generate"
VRF_FUNCTION_PUBLISH_PATH = "publish"

logger = logging.getLogger(__name__)

M = TypeVar("M", bound=BaseModel)


async def post_executor_api_request(url: str, model: Type[M]) -> M:
    async with aiohttp.ClientSession() as session:
        async with session.post(url, timeout=60) as resp:
            if resp.status != 200:
                raise ExecutorHttpError(
                    url=url, status_code=resp.status, response_text=await resp.text()
                )

            response = await resp.json()

            return model.parse_obj(response["data"])


async def prepare_executor_api_request(url: str) -> bool:
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=120) as resp:
            try:
                resp.raise_for_status()
            except aiohttp.ClientResponseError as error:
                raise ExecutorHttpError(
                    url=url, status_code=resp.status, response_text=await resp.text()
                ) from error

            response = await resp.json()

            return response["name"] == "vrf_generate_api"


async def _generate_vrf(
    aleph_client: AuthenticatedAlephClient,
    nb_executors: int,
    nb_bytes: int,
    vrf_function: ItemHash,
    executor_selection_policy: ExecutorSelectionPolicy,
    request_id: Optional[str] = None,
) -> PublishedVRFResponse:
    executors = await executor_selection_policy.select_executors(nb_executors)
    selected_nodes_json = json.dumps(
        [executor.node for executor in executors], default=pydantic_encoder
    ).encode(encoding="utf-8")

    nonce = generate_nonce()

    if request_id:
        existing_message = await get_existing_vrf_message(aleph_client, request_id)
        if existing_message:
            message = PublishedVRFResponse.from_vrf_post_message(existing_message)
            await check_message_integrity(aleph_client, message)

            return message

    vrf_request = VRFRequest(
        nb_bytes=nb_bytes,
        nb_executors=nb_executors,
        nonce=nonce,
        vrf_function=vrf_function,
        request_id=RequestId(request_id or str(uuid4())),
        node_list_hash=sha3_256(selected_nodes_json).hexdigest(),
    )

    ref = f"vrf_{vrf_request.request_id}_request"

    request_item_hash = await publish_data(
        aleph_client=aleph_client, data=vrf_request, ref=ref
    )

    logger.debug(f"Generated VRF request with item_hash {request_item_hash}")

    vrf_generation_results = await send_generate_requests(
        executors=executors,
        request_item_hash=request_item_hash,
    )

    logger.debug(
        f"Received VRF generated requests from {len(vrf_generation_results)} executors"
    )

    vrf_publication_results = await send_publish_requests(vrf_generation_results)

    logger.debug(
        f"Received VRF publish requests from {len(vrf_generation_results)} executors"
    )

    vrf_response = generate_final_vrf(
        nb_executors,
        nonce,
        vrf_generation_results,
        vrf_publication_results,
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
    request_id: Optional[str] = None,
    nb_executors: Optional[int] = None,
    nb_bytes: Optional[int] = None,
    vrf_function: Optional[ItemHash] = None,
    aleph_api_server: Optional[str] = None,
    executor_selection_policy: Optional[ExecutorSelectionPolicy] = None,
):
    vrf_function = vrf_function or settings.FUNCTION

    async with AuthenticatedAlephClient(
        account=account,
        api_server=aleph_api_server or settings.API_HOST,
        # Avoid going through the VM connector on aleph.im CRNs
        allow_unix_sockets=False,
    ) as aleph_client:
        return await _generate_vrf(
            aleph_client=aleph_client,
            request_id=request_id,
            nb_executors=nb_executors or settings.NB_EXECUTORS,
            nb_bytes=nb_bytes or settings.NB_BYTES,
            vrf_function=vrf_function or settings.FUNCTION,
            executor_selection_policy=executor_selection_policy
            or ExecuteOnAleph(vm_function=vrf_function),
        )


async def send_generate_requests(
    executors: List[Executor],
    request_item_hash: ItemHash,
) -> Dict[Executor, PublishedVRFRandomNumberHash]:
    generate_tasks = []
    for executor in executors:
        url = f"{executor.api_url}/{VRF_FUNCTION_GENERATE_PATH}/{request_item_hash}"
        generate_tasks.append(
            asyncio.create_task(
                post_executor_api_request(url, PublishedVRFRandomNumberHash)
            )
        )

    vrf_generated_responses = await asyncio.gather(
        *generate_tasks, return_exceptions=True
    )
    generation_results = dict(zip(executors, vrf_generated_responses))

    for executor, result in generation_results.items():
        if isinstance(result, Exception):
            raise RandomNumberGenerationFailed(executor=executor) from result

    return generation_results


async def send_publish_requests(
    vrf_generation_results: Dict[Executor, PublishedVRFRandomNumberHash],
) -> Dict[Executor, PublishedVRFRandomNumber]:
    publish_tasks = []
    executors: List[Executor] = []

    for executor, vrf_random_number_hash in vrf_generation_results.items():
        executors.append(executor)

        executor_message_hash = vrf_random_number_hash.message_hash
        url = f"{executor.api_url}/{VRF_FUNCTION_PUBLISH_PATH}/{executor_message_hash}"
        publish_tasks.append(
            asyncio.create_task(
                post_executor_api_request(url, PublishedVRFRandomNumber)
            )
        )

    vrf_publish_responses = await asyncio.gather(*publish_tasks, return_exceptions=True)
    publication_results = dict(zip(executors, vrf_publish_responses))

    for executor, result in publication_results.items():
        if isinstance(result, Exception):
            raise RandomNumberPublicationFailed(executor=executor) from result

    return publication_results


def generate_final_vrf(
    nb_executors: int,
    nonce: Nonce,
    vrf_generation_results: Dict[Executor, PublishedVRFRandomNumberHash],
    vrf_publication_results: Dict[Executor, PublishedVRFRandomNumber],
    vrf_request: VRFRequest,
) -> VRFResponse:
    executor_responses = []
    random_numbers_list = []
    for executor, vrf_random_number in vrf_publication_results.items():
        if (generation_hash := vrf_generation_results[executor].random_number_hash) != (
            publication_hash := vrf_random_number.random_number_hash
        ):
            raise HashesDoNotMatch(
                executor=executor,
                generation_hash=generation_hash,
                publication_hash=publication_hash,
            )

        random_number = HexBytes(vrf_random_number.random_number)
        verified = verify(
            random_number,
            nonce,
            generation_hash,
        )
        if not verified:
            raise HashValidationFailed(
                random_number=vrf_random_number,
                random_number_hash=generation_hash,
                executor=executor,
            )

        random_numbers_list.append(random_number)

        executor_response = ExecutorVRFResponse(
            url=executor.node.address,
            execution_id=vrf_random_number.execution_id,
            random_number=vrf_random_number.random_number,
            random_number_hash=vrf_generation_results[executor].random_number_hash,
            generation_message_hash=vrf_generation_results[executor].message_hash,
            publication_message_hash=vrf_random_number.message_hash,
        )
        executor_responses.append(executor_response)

    final_random_number = xor_all(random_numbers_list)

    return VRFResponse(
        nb_bytes=vrf_request.nb_bytes,
        nb_executors=nb_executors,
        nonce=nonce,
        vrf_function=vrf_request.vrf_function,
        request_id=vrf_request.request_id,
        executors=executor_responses,
        random_number=f"0x{final_random_number.hex()}",
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


async def get_existing_vrf_message(
    aleph_client: AuthenticatedAlephClient,
    request_id: str,
) -> Optional[PostMessage]:
    channel = f"vrf_{request_id}"
    ref = f"vrf_{request_id}"

    logger.debug(
        f"Getting VRF messages on {aleph_client.api_server} from request id {request_id}"
    )

    messages, status = await aleph_client.get_messages(
        message_type=MessageType.post,
        channels=[channel],
        refs=[ref],
    )

    if messages:
        if len(messages) > 1:
            logger.warning(f"Multiple VRF messages found for request id {request_id}")
        return messages[0]
    else:
        logger.debug(f"Existing VRF message for request id {request_id} not found")
        return None


async def get_existing_message(
    aleph_client: AuthenticatedAlephClient,
    item_hash: ItemHash,
) -> Optional[PostMessage]:
    logger.debug(
        f"Getting VRF message on {aleph_client.api_server} for item_hash {item_hash}"
    )

    message, status = await aleph_client.get_message(
        item_hash=item_hash,
    )

    if not message:
        raise AlephNetworkError(
            f"Message could not be read for item_hash {message.item_hash}"
        )

    if status != MessageStatus.PROCESSED:
        raise AlephNetworkError(
            f"Message found for item_hash {message.item_hash} not in processed status"
        )

    return message


async def check_message_integrity(
    aleph_client: AuthenticatedAlephClient, vrf_response: PublishedVRFResponse
):
    logger.debug(
        f"Checking VRF response message on {aleph_client.api_server} for item_hash {vrf_response.message_hash}"
    )

    for executor in vrf_response.executors:
        generation_message = await get_existing_message(
            aleph_client, executor.generation_message_hash
        )
        loaded_generation_message = PublishedVRFRandomNumberHash.from_published_message(
            generation_message
        )
        publish_message = await get_existing_message(
            aleph_client, executor.publication_message_hash
        )
        loaded_publish_message = PublishedVRFRandomNumber.from_published_message(
            publish_message
        )

        if (
            loaded_generation_message.random_number_hash
            != loaded_publish_message.random_number_hash
        ):
            raise PublishedHashesDoNotMatch(
                executor=executor,
                generation_hash=loaded_generation_message.random_number_hash,
                publication_hash=loaded_publish_message.random_number_hash,
            )

        if not verify(
            HexBytes(loaded_publish_message.random_number),
            loaded_generation_message.nonce,
            loaded_generation_message.random_number_hash,
        ):
            raise PublishedHashValidationFailed(
                executor=executor,
                random_number=loaded_publish_message,
                random_number_hash=loaded_generation_message.random_number_hash,
            )
