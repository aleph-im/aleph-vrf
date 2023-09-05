import logging
from typing import Dict, Union

logger = logging.getLogger(__name__)

logger.debug("import aleph_client")
from aleph.sdk.chains.common import get_fallback_private_key
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.client import AlephClient, AuthenticatedAlephClient
from aleph.sdk.vm.app import AlephApp
from aleph.sdk.vm.cache import VmCache
from aleph_message.status import MessageStatus

logger.debug("import fastapi")
from fastapi import FastAPI, Request

logger.debug("local imports")
from models import (
    APIResponse,
    VRFRandomBytes,
    VRFResponseHash,
    generate_request_from_message,
    generate_response_hash_from_message,
)
from utils import bytes_to_binary, bytes_to_int, generate

logger.debug("imports done")

http_app = FastAPI()
app = AlephApp(http_app=http_app)
cache = VmCache()

# TODO: Use environment settings
API_HOST = "https://api2.aleph.im"
GENERATE_MESSAGE_REF_PATH = "hash"

# TODO: Use another method to save the data
SAVED_GENERATED_BYTES: Dict[str, bytes]


@app.get("/")
async def index():
    return {
        "name": "vrf_generate_api",
        "endpoints": ["/generate/{vrf_request}", "/publish/{hash_message}"],
    }


@app.post("/generate/{vrf_request}")
async def receive_generate(vrf_request: str, request: Request) -> APIResponse:
    global SAVED_GENERATED_BYTES

    private_key = get_fallback_private_key()
    account = ETHAccount(private_key=private_key)

    async with AlephClient(api_server=API_HOST) as client:
        message = await client.get_message(item_hash=vrf_request)
        generation_request = generate_request_from_message(message)

        generated_bytes, hashed_bytes = generate(
            generation_request.num_bytes, generation_request.nonce
        )
        SAVED_GENERATED_BYTES[
            generation_request.execution_id.__str__()
        ] = generated_bytes

        response_hash = VRFResponseHash(
            num_bytes=generation_request.num_bytes,
            nonce=generation_request.nonce,
            url=str(request.url),
            request_id=generation_request.request_id,
            execution_id=generation_request.execution_id,
            vrf_request=vrf_request,
            random_bytes_hash=hashed_bytes,
        )

        ref = (
            f"vrf"
            f"_{response_hash.request_id.__str__()}"
            f"_{response_hash.execution_id.__str__()}"
            f"_{GENERATE_MESSAGE_REF_PATH}"
        )

        message_hash = await publish_data(response_hash, ref, account)

        response_hash.message_hash = message_hash

        return APIResponse(data=response_hash)


@app.post("/publish/{hash_message}")
async def receive_publish(hash_message: str, request: Request) -> APIResponse:
    global SAVED_GENERATED_BYTES

    private_key = get_fallback_private_key()
    account = ETHAccount(private_key=private_key)

    async with AlephClient(api_server=API_HOST) as client:
        message = await client.get_message(item_hash=hash_message)
        response_hash = generate_response_hash_from_message(message)

        if not SAVED_GENERATED_BYTES[response_hash.execution_id.__str__()]:
            raise ValueError(
                f"Random bytes not existing for execution {response_hash.execution_id}"
            )

        random_bytes: bytes = SAVED_GENERATED_BYTES[
            response_hash.execution_id.__str__()
        ]

        response_bytes = VRFRandomBytes(
            url=str(request.url),
            request_id=response_hash.request_id,
            execution_id=response_hash.execution_id,
            vrf_request=response_hash.vrf_request,
            random_bytes=bytes_to_binary(random_bytes),
            random_bytes_hash=response_hash.random_bytes_hash,
            random_number=bytes_to_int(random_bytes),
        )

        ref = f"vrf_{response_hash.request_id.__str__()}_{response_hash.execution_id.__str__()}"

        message_hash = await publish_data(response_bytes, ref, account)

        response_bytes.message_hash = message_hash

        return APIResponse(data=response_bytes)


async def publish_data(
    data: Union[VRFResponseHash, VRFRandomBytes], ref: str, account: ETHAccount
):
    channel = f"vrf_{data.request_id.__str__()}"

    async with AuthenticatedAlephClient(account=account, api_server=API_HOST) as client:
        message, status = await client.create_post(
            post_type="vrf_generation_post",
            post_content=data,
            channel=channel,
            ref=ref,
        )

        if status != MessageStatus.PROCESSED:
            raise ValueError(
                f"Message could not be processed for request {data.request_id} and execution_id {data.execution_id}"
            )

        return message.item_hash.__str__()
