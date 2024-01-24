import logging
from typing import Optional, Union

from pydantic import BaseModel

from aleph_vrf.settings import settings

logger = logging.getLogger(__name__)

logger.debug("import aleph_client")
from aleph.sdk.vm.app import AlephApp
from aleph.sdk.vm.cache import VmCache

logger.debug("import fastapi")
from fastapi import FastAPI, HTTPException

logger.debug("local imports")
from aleph_vrf.coordinator.vrf import generate_vrf
from aleph_vrf.models import APIError, APIResponse, PublishedVRFResponse

logger.debug("imports done")

http_app = FastAPI()
app = AlephApp(http_app=http_app)
cache = VmCache()


class VRFRequest(BaseModel):
    request_id: Optional[str]


@app.get("/")
async def index():
    return {
        "name": "vrf_api",
        "endpoints": [
            "/vrf",
        ],
    }


@app.post("/vrf")
async def receive_vrf(
    request: Optional[VRFRequest] = None,
) -> APIResponse[Union[PublishedVRFResponse, APIError]]:
    """
    Goes through the VRF random number generation process and returns a random number
    along with details on how the number was generated.
    """

    account = settings.aleph_account()

    response: Union[PublishedVRFResponse, APIError]

    request_id = request.request_id if request and request.request_id else None
    try:
        response = await generate_vrf(account=account, request_id=request_id)
    except Exception as err:
        raise HTTPException(status_code=500, detail=str(err))

    return APIResponse(data=response)
