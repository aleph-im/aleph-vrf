import logging
from typing import Dict, Union

from aleph_vrf.settings import settings

logger = logging.getLogger(__name__)

logger.debug("import aleph_client")
from aleph.sdk.chains.common import get_fallback_private_key
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.vm.app import AlephApp
from aleph.sdk.vm.cache import VmCache

logger.debug("import fastapi")
from fastapi import FastAPI

logger.debug("local imports")
from aleph_vrf.coordinator.vrf import generate_vrf
from aleph_vrf.models import APIResponse, PublishedVRFResponse

logger.debug("imports done")

http_app = FastAPI()
app = AlephApp(http_app=http_app)
cache = VmCache()


@app.get("/")
async def index():
    return {
        "name": "vrf_api",
        "endpoints": [
            "/vrf",
        ],
    }


@app.post("/vrf")
async def receive_vrf() -> APIResponse:
    account = settings.aleph_account()

    response: Union[PublishedVRFResponse, Dict[str, str]]

    try:
        response = await generate_vrf(account)
    except Exception as err:
        response = {"error": str(err)}

    return APIResponse(data=response)
