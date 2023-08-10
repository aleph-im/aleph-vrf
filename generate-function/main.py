
import logging

from ecies import decrypt, encrypt
from pydantic import BaseModel
from typings import Union

logger = logging.getLogger(__name__)

logger.debug("import aleph_client")
from aleph.sdk.client import AlephClient, AuthenticatedAlephClient
from aleph.sdk.chains.common import get_fallback_private_key
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.vm.app import AlephApp
from aleph.sdk.vm.cache import VmCache

logger.debug("import fastapi")
from fastapi import FastAPI

logger.debug("local imports")
from models import (
    APIResponse,
    generate_request_from_message,
    generate_response_hash_from_message,
    VRFResponseHash,
    VRFRandomBytes,
)
from utils import generate

logger.debug("imports done")

http_app = FastAPI()
app = AlephApp(http_app=http_app)
cache = VmCache()

# TODO: Allow some environment settings
API_HOST = "https://api2.aleph.im"


@app.get("/")
async def index():
    return {
        "name": "vrf_generate_api",
        "endpoints": [
            "/generate",
            "/publish"
        ],
    }


class GenerateRequest(BaseModel):
    vrf_request: str


@app.post("/generate")
async def receive_generate(request: GenerateRequest) -> APIResponse:

    private_key = get_fallback_private_key()
    account = ETHAccount(private_key=private_key)

    async with AlephClient() as client:
        message = await client.get_message(item_hash=request.vrf_request)
        generation_request = generate_request_from_message(message)

        generated_bytes = generate(generation_request.num_bytes, generation_request.nonce)

        content = generated_bytes + bytes(generation_request.nonce)
        encrypted_hash = encrypt(message.sender, content)

        response_hash = VRFResponseHash(
            num_bytes=generation_request.num_bytes,
            nonce=generation_request.nonce,
            request_id=generation_request.request_id,
            execution_id=generation_request.execution_id,
            vrf_request=request.vrf_request,
            random_bytes_hash=encrypted_hash,
        )

        await publish_data(response_hash, account)

        return APIResponse(
            error=False,
            data=response_hash
        )


class PublishRequest(BaseModel):
    hash_message: str


@app.post("/publish")
async def receive_publish(request: PublishRequest) -> APIResponse:

    private_key = get_fallback_private_key()
    account = ETHAccount(private_key=private_key)

    async with AlephClient() as client:
        message = await client.get_message(item_hash=request.hash_message)
        response_hash = generate_response_hash_from_message(message)

        decrypted_bytes = await account.decrypt(response_hash.random_bytes_hash)
        random_bytes = decrypted_bytes[:response_hash.num_bytes]

        response_bytes = VRFRandomBytes(
            request_id=response_hash.request_id,
            execution_id=response_hash.execution_id,
            vrf_request=response_hash.vrf_request,
            random_bytes=random_bytes,
        )

        await publish_data(response_bytes, account)

        return APIResponse(
            error=False,
            data=response_bytes
        )


async def publish_data(data: Union[VRFResponseHash, VRFRandomBytes], account: ETHAccount):

    channel = f"vrf_{data.requestId.__str__()}"

    async with AuthenticatedAlephClient(
            account=account, api_server=API_HOST
    ) as client:
        response, status = await client.create_post(
            content=data,
            channel=channel,
        )

        # TODO: Check message status
        pass
