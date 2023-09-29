"""
A malicious executor that returns an incorrect random number willingly.
"""


import logging
import sys

from aleph_vrf.executor.main import authenticated_aleph_client

# Annotated is only available in Python 3.9+
if sys.version_info < (3, 9):
    from typing_extensions import Annotated
else:
    from typing import Annotated

logger = logging.getLogger(__name__)

from aleph.sdk.client import AuthenticatedAlephClient
from aleph.sdk.vm.app import AlephApp
from aleph_message.models import ItemHash

from fastapi import FastAPI, Depends

from aleph_vrf.models import (
    APIResponse,
    PublishedVRFResponseHash,
    PublishedVRFRandomBytes,
)
from aleph_vrf.utils import bytes_to_binary


http_app = FastAPI()
app = AlephApp(http_app=http_app)


@app.post("/generate/{vrf_request}")
async def receive_generate(
    vrf_request: ItemHash,
    aleph_client: Annotated[
        AuthenticatedAlephClient, Depends(authenticated_aleph_client)
    ],
) -> APIResponse[PublishedVRFResponseHash]:
    from aleph_vrf.executor.main import receive_generate as real_receive_generate

    return await real_receive_generate(
        vrf_request=vrf_request, aleph_client=aleph_client
    )


@app.post("/publish/{hash_message}")
async def receive_publish(
    hash_message: ItemHash,
    aleph_client: Annotated[
        AuthenticatedAlephClient, Depends(authenticated_aleph_client)
    ],
) -> APIResponse[PublishedVRFRandomBytes]:
    from aleph_vrf.executor.main import receive_publish as real_receive_publish

    api_response = await real_receive_publish(
        hash_message=hash_message, aleph_client=aleph_client
    )
    # Replace the generated random number with a hardcoded value
    random_number = 123456789
    api_response.data.random_number = str(random_number)
    api_response.data.random_bytes = bytes_to_binary(
        random_number.to_bytes(length=32, byteorder="big")
    )
    return api_response
