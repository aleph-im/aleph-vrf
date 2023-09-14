import datetime as dt
from hashlib import sha256
from typing import Dict, Any, Tuple, Union

import aiohttp
import pytest
from aleph_message.models import ItemType, MessageType, PostContent, Chain, ItemHash

from aleph_vrf.models import VRFRequest, VRFResponseHash, VRFResponse


@pytest.mark.asyncio
async def test_mock(mock_ccn: str):
    """
    Sanity check test: verify that the mock CCN can be started.
    """

    async with aiohttp.ClientSession(mock_ccn) as ccn_client:
        messages = await ccn_client.get("/api/v0/messages")
        assert messages


MessageDict = Dict[str, Any]


def make_post_message(vrf_object: Union[VRFRequest, VRFResponse], sender: str) -> MessageDict:
    content = PostContent(
        type="vrf_library_post",
        ref=f"vrf_{vrf_object.request_id}_request",
        address=sender,
        time=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc).timestamp(),
        content=vrf_object,
    )

    item_content = content.json()
    item_hash = sha256(item_content.encode()).hexdigest()

    return {
        "item_hash": item_hash,
        "type": MessageType.post.value,
        "chain": Chain.ETH.value,
        "sender": sender,
        "signature": "fake-sig",
        "item_type": ItemType.inline.value,
        "item_content": item_content,
        "channel": f"vrf_{vrf_object.request_id}",
        "time": content.time,
    }


@pytest.fixture
def mock_vrf_request() -> VRFRequest:
    request_id = "513eb52c-cb74-463a-b40e-0e2adedafb8b"

    vrf_request = VRFRequest(
        nb_bytes=32,
        nonce=42,
        vrf_function="deca" * 16,
        request_id=request_id,
        node_list_hash="1234",
    )
    return vrf_request


# @pytest.fixture
# def mock_vrf_response(mock_vrf_request: VRFRequest) -> VRFResponse:
#     return VRFResponse(nb_bytes=mock_vrf_request.nb_bytes, nonce=mock_vrf_request.nonce, vrf_function=mock_vrf_request.vrf_function, nodes=[], random)


@pytest.mark.asyncio
async def test_normal_request_flow(
    mock_ccn: str,
    executor_server: str,
        mock_vrf_request: VRFRequest,
        # mock_vrf_response: VRFResponse,
):
    """
    Test that the executor works under normal circumstances:
    1. The coordinator publishes a request message
    2. The coordinator calls /generate
    3. The coordinator publishes a message to request the generated random number
    4. The coordinator calls /publish.
    """

    sender = "aleph_vrf_coordinator"
    message_dict = make_post_message(mock_vrf_request, sender=sender)
    item_hash = ItemHash(message_dict["item_hash"])

    async with aiohttp.ClientSession(mock_ccn) as ccn_client:
        resp = await ccn_client.post(
            "/api/v0/messages",
            json={"message": message_dict, "sync": True},
        )
        assert resp.status == 200, await resp.text()

    async with aiohttp.ClientSession(executor_server) as executor_client:
        resp = await executor_client.post(f"/generate/{item_hash}")
        assert resp.status == 200, await resp.text()
        response_json = await resp.json()

        response_hash = VRFResponseHash.parse_obj(response_json["data"])

        assert response_hash.nb_bytes == mock_vrf_request.nb_bytes
        assert response_hash.nonce == mock_vrf_request.nonce
        assert response_hash.request_id == mock_vrf_request.request_id
        assert response_hash.execution_id   # This should be a UUID4
        assert response_hash.vrf_request == item_hash
        assert response_hash.random_bytes_hash
        assert response_hash.message_hash

        resp = await executor_client.post(f"/publish/{response_hash.message_hash}")
        assert resp.status == 200, await resp.text()
        print(await resp.text())
