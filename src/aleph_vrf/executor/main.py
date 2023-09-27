import logging
import sys
from typing import Dict, Union, Set

from aleph_vrf.exceptions import AlephNetworkError

# Annotated is only available in Python 3.9+
if sys.version_info < (3, 9):
    from typing_extensions import Annotated
else:
    from typing import Annotated

import fastapi
from aleph.sdk.exceptions import MessageNotFoundError, MultipleMessagesError

from aleph_vrf.settings import settings
from aleph_vrf.types import ExecutionId, RequestId

logger = logging.getLogger(__name__)

logger.debug("import aleph_client")
from aleph.sdk.client import AlephClient, AuthenticatedAlephClient
from aleph.sdk.vm.app import AlephApp
from aleph_message.models import ItemHash, PostMessage
from aleph_message.status import MessageStatus

logger.debug("import fastapi")
from fastapi import FastAPI, Depends

logger.debug("local imports")
from aleph_vrf.models import (
    APIResponse,
    VRFRandomBytes,
    VRFResponseHash,
    generate_request_from_message,
    generate_response_hash_from_message,
    PublishedVRFResponseHash,
    PublishedVRFRandomBytes,
)
from aleph_vrf.utils import bytes_to_binary, bytes_to_int, generate

logger.debug("imports done")

GENERATE_MESSAGE_REF_PATH = "hash"

# TODO: Use another method to save the data
ANSWERED_REQUESTS: Set[RequestId] = set()
SAVED_GENERATED_BYTES: Dict[ExecutionId, bytes] = {}

http_app = FastAPI()
app = AlephApp(http_app=http_app)


async def authenticated_aleph_client() -> AuthenticatedAlephClient:
    account = settings.aleph_account()
    async with AuthenticatedAlephClient(
        account=account, api_server=settings.API_HOST
    ) as client:
        yield client


@app.get("/")
async def index():
    return {
        "name": "vrf_generate_api",
        "endpoints": ["/generate/{vrf_request}", "/publish/{hash_message}"],
    }


async def _get_message(client: AlephClient, item_hash: ItemHash) -> PostMessage:
    try:
        return await client.get_message(item_hash=item_hash, message_type=PostMessage)
    except MessageNotFoundError:
        raise fastapi.HTTPException(
            status_code=404, detail=f"Message {item_hash} not found"
        )
    except MultipleMessagesError:
        raise fastapi.HTTPException(
            status_code=409,
            detail=f"Multiple messages have the following hash: {item_hash}",
        )
    except TypeError:
        raise fastapi.HTTPException(
            status_code=409, detail=f"Message {item_hash} is not a POST message"
        )


@app.post("/generate/{vrf_request}")
async def receive_generate(
    vrf_request: ItemHash,
    aleph_client: Annotated[
        AuthenticatedAlephClient, Depends(authenticated_aleph_client)
    ],
) -> APIResponse[PublishedVRFResponseHash]:
    """
    Generates a random number and returns its SHA3 hash.
    """

    global SAVED_GENERATED_BYTES, ANSWERED_REQUESTS

    message = await _get_message(client=aleph_client, item_hash=vrf_request)
    generation_request = generate_request_from_message(message)

    if generation_request.request_id in ANSWERED_REQUESTS:
        raise fastapi.HTTPException(
            status_code=409,
            detail=f"A random number has already been generated for request {vrf_request}",
        )

    generated_bytes, hashed_bytes = generate(
        generation_request.nb_bytes, generation_request.nonce
    )
    SAVED_GENERATED_BYTES[generation_request.execution_id] = generated_bytes
    ANSWERED_REQUESTS.add(generation_request.request_id)

    response_hash = VRFResponseHash(
        nb_bytes=generation_request.nb_bytes,
        nonce=generation_request.nonce,
        request_id=generation_request.request_id,
        execution_id=generation_request.execution_id,
        vrf_request=vrf_request,
        random_bytes_hash=hashed_bytes,
    )

    ref = (
        f"vrf"
        f"_{response_hash.request_id}"
        f"_{response_hash.execution_id}"
        f"_{GENERATE_MESSAGE_REF_PATH}"
    )

    message_hash = await publish_data(
        aleph_client=aleph_client, data=response_hash, ref=ref
    )

    published_response_hash = PublishedVRFResponseHash.from_vrf_response_hash(
        vrf_response_hash=response_hash, message_hash=message_hash
    )

    return APIResponse(data=published_response_hash)


@app.post("/publish/{hash_message}")
async def receive_publish(
    hash_message: ItemHash,
    aleph_client: Annotated[
        AuthenticatedAlephClient, Depends(authenticated_aleph_client)
    ],
) -> APIResponse[PublishedVRFRandomBytes]:
    """
    Publishes the random number associated with the specified message hash.
    If a user attempts to call this endpoint several times for the same message hash,
    data will only be returned on the first call.
    """

    global SAVED_GENERATED_BYTES

    message = await _get_message(client=aleph_client, item_hash=hash_message)
    response_hash = generate_response_hash_from_message(message)

    if response_hash.execution_id not in SAVED_GENERATED_BYTES:
        raise fastapi.HTTPException(
            status_code=404, detail="The random number has already been published"
        )

    random_bytes: bytes = SAVED_GENERATED_BYTES.pop(response_hash.execution_id)

    response_bytes = VRFRandomBytes(
        request_id=response_hash.request_id,
        execution_id=response_hash.execution_id,
        vrf_request=response_hash.vrf_request,
        random_bytes=bytes_to_binary(random_bytes),
        random_bytes_hash=response_hash.random_bytes_hash,
        random_number=str(bytes_to_int(random_bytes)),
    )

    ref = f"vrf_{response_hash.request_id}_{response_hash.execution_id}"

    message_hash = await publish_data(
        aleph_client=aleph_client, data=response_bytes, ref=ref
    )
    published_random_bytes = PublishedVRFRandomBytes.from_vrf_random_bytes(
        vrf_random_bytes=response_bytes, message_hash=message_hash
    )

    return APIResponse(data=published_random_bytes)


async def publish_data(
    aleph_client: AuthenticatedAlephClient,
    data: Union[VRFResponseHash, VRFRandomBytes],
    ref: str,
) -> ItemHash:
    """
    Publishes the generation/publication artefacts on the aleph.im network as POST messages.
    """

    channel = f"vrf_{data.request_id}"

    message, status = await aleph_client.create_post(
        post_type="vrf_generation_post",
        post_content=data,
        channel=channel,
        ref=ref,
        sync=True,
    )

    if status != MessageStatus.PROCESSED:
        raise AlephNetworkError(
            f"Message could not be processed for request {data.request_id} "
            f"and execution_id {data.execution_id}"
        )

    return message.item_hash
