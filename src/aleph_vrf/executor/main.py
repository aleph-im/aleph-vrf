import logging
import sys
from typing import Dict, Union, Set
from uuid import uuid4

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
    VRFRandomNumber,
    VRFRandomNumberHash,
    get_vrf_request_from_message,
    get_random_number_hash_from_message,
    PublishedVRFRandomNumberHash,
    PublishedVRFRandomNumber,
)
from aleph_vrf.utils import generate

logger.debug("imports done")

GENERATE_MESSAGE_REF_PATH = "hash"

# TODO: Use another method to save the data
ANSWERED_REQUESTS: Set[RequestId] = set()
GENERATED_NUMBERS: Dict[ExecutionId, bytes] = {}

http_app = FastAPI()
app = AlephApp(http_app=http_app)


async def authenticated_aleph_client() -> AuthenticatedAlephClient:
    account = settings.aleph_account()
    async with AuthenticatedAlephClient(
        account=account,
        api_server=settings.API_HOST,
        # Avoid going through the VM connector on aleph.im CRNs
        allow_unix_sockets=False,
    ) as client:
        yield client


@app.get("/")
async def index():
    return {
        "name": "vrf_generate_api",
        "endpoints": ["/generate/{vrf_request_hash}", "/publish/{message_hash}"],
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


@app.post("/generate/{vrf_request_hash}")
async def receive_generate(
    vrf_request_hash: ItemHash,
    aleph_client: Annotated[
        AuthenticatedAlephClient, Depends(authenticated_aleph_client)
    ],
) -> APIResponse[PublishedVRFRandomNumberHash]:
    """
    Generates a random number and returns its SHA3 hash.

    :param vrf_request_hash: Hash of the aleph.im message issued by the coordinator containing the VRF request.
    :param aleph_client: Authenticated aleph.im client.
    """

    global GENERATED_NUMBERS, ANSWERED_REQUESTS

    message = await _get_message(client=aleph_client, item_hash=vrf_request_hash)
    vrf_request = get_vrf_request_from_message(message)
    execution_id = ExecutionId(str(uuid4()))

    if vrf_request.request_id in ANSWERED_REQUESTS:
        raise fastapi.HTTPException(
            status_code=409,
            detail=f"A random number has already been generated for request {vrf_request_hash}",
        )

    random_number, random_number_hash = generate(vrf_request.nb_bytes, vrf_request.nonce)
    GENERATED_NUMBERS[execution_id] = random_number
    ANSWERED_REQUESTS.add(vrf_request.request_id)

    vrf_random_number_hash = VRFRandomNumberHash(
        nb_bytes=vrf_request.nb_bytes,
        nonce=vrf_request.nonce,
        request_id=vrf_request.request_id,
        execution_id=execution_id,
        vrf_request=vrf_request_hash,
        random_number_hash=random_number_hash,
    )

    ref = (
        f"vrf"
        f"_{vrf_random_number_hash.request_id}"
        f"_{vrf_random_number_hash.execution_id}"
        f"_{GENERATE_MESSAGE_REF_PATH}"
    )

    message_hash = await publish_data(
        aleph_client=aleph_client, data=vrf_random_number_hash, ref=ref
    )

    published_random_number_hash = PublishedVRFRandomNumberHash.from_vrf_response_hash(
        vrf_response_hash=vrf_random_number_hash, message_hash=message_hash
    )

    return APIResponse(data=published_random_number_hash)


@app.post("/publish/{message_hash}")
async def receive_publish(
    message_hash: ItemHash,
    aleph_client: Annotated[
        AuthenticatedAlephClient, Depends(authenticated_aleph_client)
    ],
) -> APIResponse[PublishedVRFRandomNumber]:
    """
    Publishes the random number associated with the specified message hash.
    If a user attempts to call this endpoint several times for the same message hash,
    data will only be returned on the first call.

    :param message_hash: Hash of the aleph.im message issued by the executor during the generation phase.
    :param aleph_client: Authenticated aleph.im client.
    """

    global GENERATED_NUMBERS

    message = await _get_message(client=aleph_client, item_hash=message_hash)
    response_hash = get_random_number_hash_from_message(message)

    if response_hash.execution_id not in GENERATED_NUMBERS:
        raise fastapi.HTTPException(
            status_code=404, detail="The random number has already been published"
        )

    random_number: bytes = GENERATED_NUMBERS.pop(response_hash.execution_id)

    vrf_random_number = VRFRandomNumber(
        request_id=response_hash.request_id,
        execution_id=response_hash.execution_id,
        vrf_request=response_hash.vrf_request,
        random_number=f"0x{random_number.hex()}",
        random_number_hash=response_hash.random_number_hash,
    )

    ref = f"vrf_{response_hash.request_id}_{response_hash.execution_id}"

    message_hash = await publish_data(
        aleph_client=aleph_client, data=vrf_random_number, ref=ref
    )
    published_random_number = PublishedVRFRandomNumber.from_vrf_random_number(
        vrf_random_number=vrf_random_number, message_hash=message_hash
    )

    return APIResponse(data=published_random_number)


async def publish_data(
    aleph_client: AuthenticatedAlephClient,
    data: Union[VRFRandomNumberHash, VRFRandomNumber],
    ref: str,
) -> ItemHash:
    """
    Publishes the generation/publication artefacts on the aleph.im network as POST messages.

    :param aleph_client: Authenticated aleph.im client.
    :param data: Content of the POST message.
    :param ref: Reference of the POST message.
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
