
import logging

from ecies import decrypt, encrypt
from pydantic import BaseModel

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
from models import APIResponse, generate_request_from_message, VRFResponseHash
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
            request_id=generation_request.request_id,
            execution_id=generation_request.execution_id,
            random_bytes_hash=encrypted_hash,
        )

        await publish_hash(response_hash, account)

        return APIResponse(
            error=False,
            data=response_hash
        )


async def publish_hash(response_hash: VRFResponseHash, account: ETHAccount) -> str:

    channel = f"vrf_{response_hash.requestId.__str__()}"

    async with AuthenticatedAlephClient(
            account=account, api_server=API_HOST
    ) as client:
        response, status = await client.create_post(
            content=response_hash,
            channel=channel,
        )

        # TODO: Check message status

        return response_hash.random_bytes_hash
