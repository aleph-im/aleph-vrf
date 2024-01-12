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

from aleph.sdk.client import AuthenticatedAlephHttpClient
from aleph.sdk.vm.app import AlephApp
from aleph_message.models import ItemHash

from fastapi import FastAPI, Depends

from aleph_vrf.models import (
    APIResponse,
    PublishedVRFRandomNumberHash,
    PublishedVRFRandomNumber,
)

http_app = FastAPI()
app = AlephApp(http_app=http_app)


@app.post("/generate/{vrf_request_hash}")
async def receive_generate(
    vrf_request_hash: ItemHash,
    aleph_client: Annotated[
        AuthenticatedAlephHttpClient, Depends(authenticated_aleph_client)
    ],
) -> APIResponse[PublishedVRFRandomNumberHash]:
    from aleph_vrf.executor.main import receive_generate as real_receive_generate

    return await real_receive_generate(
        vrf_request_hash=vrf_request_hash, aleph_client=aleph_client
    )


@app.post("/publish/{message_hash}")
async def receive_publish(
    message_hash: ItemHash,
    aleph_client: Annotated[
        AuthenticatedAlephHttpClient, Depends(authenticated_aleph_client)
    ],
) -> APIResponse[PublishedVRFRandomNumber]:
    from aleph_vrf.executor.main import receive_publish as real_receive_publish

    api_response = await real_receive_publish(
        message_hash=message_hash, aleph_client=aleph_client
    )
    # Replace the generated random number with a hardcoded value
    random_number = int(123456789).to_bytes(length=32, byteorder="big")
    api_response.data.random_number = f"0x{random_number.hex()}"
    return api_response
