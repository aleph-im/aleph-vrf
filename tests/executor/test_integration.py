import datetime as dt
from hashlib import sha256
from typing import Dict, Any, Union, Tuple

import aiohttp
import pytest
import pytest_asyncio
from aleph.sdk import AlephClient
from aleph_message.models import (
    ItemType,
    MessageType,
    PostContent,
    Chain,
    ItemHash,
    PostMessage,
)

from aleph_vrf.models import (
    VRFRequest,
    VRFResponseHash,
    VRFResponse,
    VRFRandomBytes,
    PublishedVRFResponseHash,
    PublishedVRFRandomBytes,
)
from aleph_vrf.types import Nonce, RequestId
from aleph_vrf.utils import binary_to_bytes, verify


@pytest.mark.asyncio
async def test_mock(mock_ccn: str):
    """
    Sanity check test: verify that the mock CCN can be started.
    """

    async with aiohttp.ClientSession(mock_ccn) as ccn_client:
        messages = await ccn_client.get("/api/v0/messages")
        assert messages


MessageDict = Dict[str, Any]


def make_post_message(
    vrf_object: Union[VRFRequest, VRFResponse], sender: str
) -> MessageDict:
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
    request_id = RequestId("513eb52c-cb74-463a-b40e-0e2adedafb8b")

    vrf_request = VRFRequest(
        nb_bytes=32,
        nb_executors=4,
        nonce=Nonce(42),
        vrf_function=ItemHash("deca" * 16),
        request_id=request_id,
        node_list_hash="1234",
    )
    return vrf_request


@pytest_asyncio.fixture
async def published_vrf_request(
    mock_ccn_client: aiohttp.ClientSession, mock_vrf_request: VRFRequest
) -> Tuple[MessageDict, VRFRequest]:
    sender = "aleph_vrf_coordinator"
    message_dict = make_post_message(mock_vrf_request, sender=sender)

    resp = await mock_ccn_client.post(
        "/api/v0/messages",
        json={"message": message_dict, "sync": True},
    )
    assert resp.status == 200, await resp.text()
    return message_dict, mock_vrf_request


def assert_vrf_hash_matches_request(
    response_hash: PublishedVRFResponseHash,
    vrf_request: VRFRequest,
    request_item_hash: ItemHash,
):
    assert response_hash.nb_bytes == vrf_request.nb_bytes
    assert response_hash.nonce == vrf_request.nonce
    assert response_hash.request_id == vrf_request.request_id
    assert response_hash.execution_id  # This should be a UUID4
    assert response_hash.vrf_request == request_item_hash
    assert response_hash.random_bytes_hash


def assert_random_number_matches_request(
    random_bytes: VRFRandomBytes,
    response_hash: VRFResponseHash,
    vrf_request: VRFRequest,
):
    assert random_bytes.request_id == vrf_request.request_id
    assert random_bytes.execution_id == response_hash.execution_id
    assert random_bytes.vrf_request == response_hash.vrf_request
    assert random_bytes.random_bytes_hash == response_hash.random_bytes_hash

    assert verify(
        random_bytes=binary_to_bytes(random_bytes.random_bytes),
        nonce=vrf_request.nonce,
        random_hash=response_hash.random_bytes_hash,
    )


def assert_vrf_response_hash_equal(
    response_hash: VRFResponseHash, expected_response_hash: VRFResponseHash
):
    assert response_hash.nb_bytes == expected_response_hash.nb_bytes
    assert response_hash.nonce == expected_response_hash.nonce
    assert response_hash.request_id == expected_response_hash.request_id
    assert response_hash.execution_id == expected_response_hash.execution_id
    assert response_hash.vrf_request == expected_response_hash.vrf_request
    assert response_hash.random_bytes_hash == expected_response_hash.random_bytes_hash
    # We do not check message_hash as it can be None in the aleph message but set in the API response


async def assert_aleph_message_matches_response_hash(
    ccn_url: Any,  # aiohttp does not expose its URL type
    response_hash: PublishedVRFResponseHash,
) -> PostMessage:
    assert response_hash.message_hash

    async with AlephClient(api_server=ccn_url) as client:
        message = await client.get_message(
            response_hash.message_hash, message_type=PostMessage
        )

    message_response_hash = VRFResponseHash.parse_obj(message.content.content)
    assert_vrf_response_hash_equal(message_response_hash, response_hash)

    return message


def assert_vrf_random_bytes_equal(
    random_bytes: VRFRandomBytes, expected_random_bytes: VRFRandomBytes
):
    assert random_bytes.request_id == expected_random_bytes.request_id
    assert random_bytes.execution_id == expected_random_bytes.execution_id
    assert random_bytes.vrf_request == expected_random_bytes.vrf_request
    assert random_bytes.random_bytes == expected_random_bytes.random_bytes
    assert random_bytes.random_bytes_hash == expected_random_bytes.random_bytes_hash
    assert random_bytes.random_number == expected_random_bytes.random_number
    # We do not check message_hash as it can be None in the aleph message but set in the API response


async def assert_aleph_message_matches_random_bytes(
    ccn_url: Any,  # aiohttp does not expose its URL type
    random_bytes: PublishedVRFRandomBytes,
) -> PostMessage:
    async with AlephClient(api_server=ccn_url) as client:
        message = await client.get_message(
            random_bytes.message_hash, message_type=PostMessage
        )

    message_random_bytes = VRFRandomBytes.parse_obj(message.content.content)
    assert_vrf_random_bytes_equal(message_random_bytes, random_bytes)

    return message


@pytest.mark.asyncio
async def test_normal_request_flow(
    mock_ccn_client: aiohttp.ClientSession,
    executor_client: aiohttp.ClientSession,
    published_vrf_request: Tuple[MessageDict, VRFRequest],
):
    """
    Test that the executor works under normal circumstances:
    1. The coordinator publishes a request message
    2. The coordinator calls /generate
    3. The coordinator calls /publish.
    """

    message_dict, vrf_request = published_vrf_request
    item_hash = message_dict["item_hash"]

    resp = await executor_client.post(f"/generate/{item_hash}")
    assert resp.status == 200, await resp.text()
    response_json = await resp.json()

    response_hash = PublishedVRFResponseHash.parse_obj(response_json["data"])

    assert_vrf_hash_matches_request(response_hash, vrf_request, item_hash)
    random_hash_message = await assert_aleph_message_matches_response_hash(
        mock_ccn_client._base_url, response_hash
    )

    resp = await executor_client.post(f"/publish/{response_hash.message_hash}")
    assert resp.status == 200, await resp.text()
    response_json = await resp.json()

    random_bytes = PublishedVRFRandomBytes.parse_obj(response_json["data"])
    assert_random_number_matches_request(
        random_bytes=random_bytes,
        response_hash=response_hash,
        vrf_request=vrf_request,
    )
    random_number_message = await assert_aleph_message_matches_random_bytes(
        mock_ccn_client._base_url, random_bytes
    )

    # Sanity checks on the message
    assert random_number_message.sender == random_hash_message.sender
    assert random_number_message.chain == random_hash_message.chain


@pytest.mark.asyncio
async def test_call_publish_twice(
    executor_client: aiohttp.ClientSession,
    published_vrf_request: Tuple[MessageDict, VRFRequest],
):
    """
    Test that the executor will only return data the first time POST /publish is called.
    This feature makes it possible for the coordinator to detect that someone else already received
    the generated random number.
    """
    message_dict, vrf_request = published_vrf_request
    item_hash = message_dict["item_hash"]

    resp = await executor_client.post(f"/generate/{item_hash}")
    assert resp.status == 200, await resp.text()
    response_json = await resp.json()

    response_hash = PublishedVRFResponseHash.parse_obj(response_json["data"])

    # Call POST /publish a first time
    resp = await executor_client.post(f"/publish/{response_hash.message_hash}")
    assert resp.status == 200, await resp.text()

    # Call it a second time
    resp = await executor_client.post(f"/publish/{response_hash.message_hash}")
    assert resp.status == 404, await resp.text()


@pytest.mark.asyncio
async def test_call_generate_twice(
    executor_client: aiohttp.ClientSession,
    published_vrf_request: Tuple[MessageDict, VRFRequest],
):
    """
    Test that calling POST /generate twice with the same request does not generate a new random number.
    We expect the API to return the same random number.
    """
    message_dict, vrf_request = published_vrf_request
    item_hash = message_dict["item_hash"]

    # Call POST /generate a first time
    resp = await executor_client.post(f"/generate/{item_hash}")
    assert resp.status == 200, await resp.text()

    # Call POST /generate a second time
    resp = await executor_client.post(f"/generate/{item_hash}")
    assert resp.status == 409, await resp.text()


@pytest.mark.asyncio
async def test_call_generate_without_aleph_message(
    executor_client: aiohttp.ClientSession,
    mock_vrf_request: VRFRequest,
):
    """
    Test that calling POST /generate without an aleph message fails.
    """
    item_hash = "bad0" * 16

    # Call POST /generate with a nonexistent item hash
    resp = await executor_client.post(f"/generate/{item_hash}")
    assert resp.status == 404, await resp.text()
