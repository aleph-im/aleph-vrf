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
from hexbytes import HexBytes

from aleph_vrf.models import (
    VRFRequest,
    VRFRandomNumberHash,
    VRFResponse,
    VRFRandomNumber,
    PublishedVRFRandomNumberHash,
    PublishedVRFRandomNumber,
)
from aleph_vrf.types import Nonce, RequestId
from aleph_vrf.utils import verify


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
    random_number_hash: PublishedVRFRandomNumberHash,
    vrf_request: VRFRequest,
    request_item_hash: ItemHash,
):
    assert random_number_hash.nb_bytes == vrf_request.nb_bytes
    assert random_number_hash.nonce == vrf_request.nonce
    assert random_number_hash.request_id == vrf_request.request_id
    assert random_number_hash.execution_id  # This should be a UUID4
    assert random_number_hash.vrf_request == request_item_hash
    assert random_number_hash.random_number_hash


def assert_random_number_matches_request(
    random_number: VRFRandomNumber,
    random_number_hash: VRFRandomNumberHash,
    vrf_request: VRFRequest,
):
    assert random_number.request_id == vrf_request.request_id
    assert random_number.execution_id == random_number_hash.execution_id
    assert random_number.vrf_request == random_number_hash.vrf_request
    assert random_number.random_number_hash == random_number_hash.random_number_hash

    assert random_number.random_number.startswith("0x")
    assert (
        len(random_number.random_number) == vrf_request.nb_bytes * 2 + 2
    )  # Account for the "0x" prefix

    assert verify(
        random_number=HexBytes(random_number.random_number),
        nonce=vrf_request.nonce,
        random_hash=random_number_hash.random_number_hash,
    )


def assert_vrf_random_number_hash_equal(
    random_number_hash: VRFRandomNumberHash,
    expected_random_number_hash: VRFRandomNumberHash,
):
    assert random_number_hash.nb_bytes == expected_random_number_hash.nb_bytes
    assert random_number_hash.nonce == expected_random_number_hash.nonce
    assert random_number_hash.request_id == expected_random_number_hash.request_id
    assert random_number_hash.execution_id == expected_random_number_hash.execution_id
    assert random_number_hash.vrf_request == expected_random_number_hash.vrf_request
    assert (
        random_number_hash.random_number_hash
        == expected_random_number_hash.random_number_hash
    )
    # We do not check message_hash as it can be None in the aleph message but set in the API response


async def assert_aleph_message_matches_random_number_hash(
    ccn_url: Any,  # aiohttp does not expose its URL type
    random_number_hash: PublishedVRFRandomNumberHash,
) -> PostMessage:
    assert random_number_hash.message_hash

    async with AlephClient(api_server=ccn_url) as client:
        message = await client.get_message(
            random_number_hash.message_hash, message_type=PostMessage
        )

    message_random_number_hash = VRFRandomNumberHash.parse_obj(message.content.content)
    assert_vrf_random_number_hash_equal(message_random_number_hash, random_number_hash)

    return message


def assert_vrf_random_number_equal(
    random_number: VRFRandomNumber, expected_random_number: VRFRandomNumber
):
    assert random_number.request_id == expected_random_number.request_id
    assert random_number.execution_id == expected_random_number.execution_id
    assert random_number.vrf_request == expected_random_number.vrf_request
    assert random_number.random_number == expected_random_number.random_number
    assert random_number.random_number_hash == expected_random_number.random_number_hash
    # We do not check message_hash as it can be None in the aleph message but set in the API response


async def assert_aleph_message_matches_random_number(
    ccn_url: Any,  # aiohttp does not expose its URL type
    random_number: PublishedVRFRandomNumber,
) -> PostMessage:
    async with AlephClient(api_server=ccn_url) as client:
        message = await client.get_message(
            random_number.message_hash, message_type=PostMessage
        )

    message_random_number = VRFRandomNumber.parse_obj(message.content.content)
    assert_vrf_random_number_equal(message_random_number, random_number)

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

    random_number_hash = PublishedVRFRandomNumberHash.parse_obj(response_json["data"])

    assert_vrf_hash_matches_request(random_number_hash, vrf_request, item_hash)
    random_number_hash_message = await assert_aleph_message_matches_random_number_hash(
        mock_ccn_client._base_url, random_number_hash
    )

    resp = await executor_client.post(f"/publish/{random_number_hash.message_hash}")
    assert resp.status == 200, await resp.text()
    response_json = await resp.json()

    random_number = PublishedVRFRandomNumber.parse_obj(response_json["data"])
    assert_random_number_matches_request(
        random_number=random_number,
        random_number_hash=random_number_hash,
        vrf_request=vrf_request,
    )
    random_number_message = await assert_aleph_message_matches_random_number(
        mock_ccn_client._base_url, random_number
    )

    # Sanity checks on the message
    assert random_number_message.sender == random_number_hash_message.sender
    assert random_number_message.chain == random_number_hash_message.chain


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

    random_number_hash = PublishedVRFRandomNumberHash.parse_obj(response_json["data"])

    # Call POST /publish a first time
    resp = await executor_client.post(f"/publish/{random_number_hash.message_hash}")
    assert resp.status == 200, await resp.text()

    # Call it a second time
    resp = await executor_client.post(f"/publish/{random_number_hash.message_hash}")
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
