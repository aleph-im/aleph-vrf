import logging
from typing import Union

from aleph_vrf.settings import settings

logger = logging.getLogger(__name__)

logger.debug("import aleph_client")
from aleph.sdk.vm.app import AlephApp
from aleph.sdk.vm.cache import VmCache

logger.debug("import fastapi")
from fastapi import FastAPI

logger.debug("local imports")
from aleph_vrf.coordinator.vrf import generate_vrf
from aleph_vrf.models import APIError, APIResponse, PublishedVRFResponse

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
async def receive_vrf() -> APIResponse[Union[PublishedVRFResponse, APIError]]:
    """
    Goes through the VRF random number generation process and returns a random number
    along with details on how the number was generated.
    """

    account = settings.aleph_account()

    response: Union[PublishedVRFResponse, APIError]

    try:
        response = await generate_vrf(account)
    except Exception as err:
        response = APIError(error=str(err))

    return APIResponse(data=response)
