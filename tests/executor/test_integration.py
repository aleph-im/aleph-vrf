import datetime as dt
from hashlib import sha256
from typing import Dict, Any

import aiohttp
import pytest
from aleph_message.models import ItemType, MessageType, PostContent, Chain

from aleph_vrf.models import VRFRequest


@pytest.mark.asyncio
async def test_mock(mock_ccn: str):
    """
    Sanity check test: verify that the mock CCN can be started.
    """

    async with aiohttp.ClientSession(mock_ccn) as ccn_client:
        messages = await ccn_client.get("/api/v0/messages")
        assert messages


@pytest.fixture
def mock_coordinator_message():
    sender = "aleph_vrf_coordinator"
    request_id = "513eb52c-cb74-463a-b40e-0e2adedafb8b"

    vrf_request = VRFRequest(
        nb_bytes=32,
        nonce=42,
        vrf_function="deca" * 16,
        request_id=request_id,
        node_list_hash="1234",
    )
    content = PostContent(
        type="vrf_library_post",
        ref=f"vrf_{vrf_request.request_id}_request",
        address=sender,
        time=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc).timestamp(),
        content=vrf_request,
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
        "channel": f"vrf_{request_id}",
        "time": content.time,
    }


@pytest.mark.asyncio
async def test_start_request(
    mock_ccn: str, executor_server: str, mock_coordinator_message: Dict[str, Any]
):
    async with aiohttp.ClientSession(mock_ccn) as ccn_client:
        resp = await ccn_client.post(
            "/api/v0/messages",
            json={"message": mock_coordinator_message, "sync": True},
        )
        assert resp.status == 200, await resp.text()

    async with aiohttp.ClientSession(executor_server) as executor_client:
        resp = await executor_client.post(
            f"/generate/{mock_coordinator_message['item_hash']}"
        )
        print(await resp.text())
