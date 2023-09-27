from typing import Tuple, Dict, List

import pytest
from aleph.sdk import AlephClient
from aleph.sdk.chains.common import generate_key
from aleph.sdk.chains.ethereum import ETHAccount
from aleph_message.models import PostMessage, ItemHash

from aleph_vrf.coordinator.executor_selection import UsePredeterminedExecutors
from aleph_vrf.coordinator.vrf import generate_vrf, post_node_vrf
from aleph_vrf.coordinator.vrf import send_generate_requests
from aleph_vrf.exceptions import RandomNumberPublicationFailed, HashValidationFailed, RandomNumberGenerationFailed
from aleph_vrf.models import (
    Executor,
    Node,
    VRFResponse,
    PublishedVRFResponse,
    PublishedVRFResponseHash,
    PublishedVRFRandomBytes,
)
from aleph_vrf.types import RequestId
from aleph_vrf.utils import xor_all, bytes_to_int, binary_to_bytes, verify


@pytest.fixture
def mock_account() -> ETHAccount:
    private_key = generate_key()
    return ETHAccount(private_key=private_key)


def assert_vrf_response_equal(
    vrf_response: VRFResponse, expected_vrf_response: VRFResponse
) -> None:
    assert vrf_response.nb_bytes == expected_vrf_response.nb_bytes
    assert vrf_response.nonce == expected_vrf_response.nonce
    assert vrf_response.request_id == expected_vrf_response.request_id
    assert vrf_response.vrf_function == expected_vrf_response.vrf_function
    assert vrf_response.request_id == expected_vrf_response.request_id
    assert vrf_response.random_number == expected_vrf_response.random_number

    assert len(vrf_response.nodes) == len(expected_vrf_response.nodes)
    expected_nodes_by_execution_id = {
        node.execution_id: node for node in expected_vrf_response.nodes
    }

    for executor_response in vrf_response.nodes:
        expected_executor_response = expected_nodes_by_execution_id[
            executor_response.execution_id
        ]
        assert executor_response.url == expected_executor_response.url
        assert executor_response.execution_id == expected_executor_response.execution_id
        assert (
            executor_response.random_number == expected_executor_response.random_number
        )
        assert (
            executor_response.random_bytes_hash
            == expected_executor_response.random_bytes_hash
        )
        assert (
            executor_response.generation_message_hash
            == expected_executor_response.generation_message_hash
        )
        assert (
            executor_response.publish_message_hash
            == expected_executor_response.publish_message_hash
        )


async def assert_aleph_message_matches_vrf_response(
    ccn_url: str,
    vrf_response: PublishedVRFResponse,
) -> PostMessage:
    assert vrf_response.message_hash

    async with AlephClient(api_server=ccn_url) as client:
        message = await client.get_message(
            vrf_response.message_hash, message_type=PostMessage
        )

    message_vrf_response = VRFResponse.parse_obj(message.content.content)
    assert_vrf_response_equal(message_vrf_response, vrf_response)

    return message


@pytest.mark.asyncio
@pytest.mark.parametrize("executor_servers", [3], indirect=True)
@pytest.mark.parametrize("nb_executors,nb_bytes", [(3, 32), (2, 16)])
async def test_normal_flow(
    mock_account: ETHAccount,
    mock_ccn: str,
    executor_servers: Tuple[str],
    nb_executors: int,
    nb_bytes: int,
):
    """
    Test that the coordinator can call executors to generate a random number.
    """

    executors = [Executor(node=Node(address=address)) for address in executor_servers]

    vrf_response = await generate_vrf(
        account=mock_account,
        nb_executors=nb_executors,
        nb_bytes=nb_bytes,
        aleph_api_server=mock_ccn,
        executor_selection_policy=UsePredeterminedExecutors(executors),
    )
    assert vrf_response.nb_executors == nb_executors
    assert len(vrf_response.nodes) == nb_executors
    assert vrf_response.nb_bytes == nb_bytes

    for executor_response in vrf_response.nodes:
        assert verify(
            random_bytes=binary_to_bytes(executor_response.random_bytes),
            nonce=vrf_response.nonce,
            random_hash=executor_response.random_bytes_hash,
        )

    # Check that we can rebuild the random number using the executor responses
    random_bytes = [
        binary_to_bytes(executor_response.random_bytes)
        for executor_response in vrf_response.nodes
    ]
    random_number_bytes = xor_all(random_bytes)
    random_number = bytes_to_int(random_number_bytes)
    assert int(vrf_response.random_number) == random_number

    # Check that VRF response posted on the network matches the API response
    await assert_aleph_message_matches_vrf_response(
        ccn_url=mock_ccn, vrf_response=vrf_response
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("executor_servers", [3], indirect=True)
@pytest.mark.parametrize("nb_bytes", [32])
async def test_unresponsive_executor(
    mock_account: ETHAccount,
    mock_ccn: str,
    executor_servers: Tuple[str],
    nb_bytes: int,
):
    """
    Test that requesting a VRF from an unresponsive executor fails.
    """

    executors = [Executor(node=Node(address=address)) for address in executor_servers]
    executors.append(Executor(node=Node(address="http://127.0.0.1:404")))

    with pytest.raises(RandomNumberGenerationFailed):
        _vrf_response = await generate_vrf(
            account=mock_account,
            nb_executors=len(executors),
            nb_bytes=len(executors),
            aleph_api_server=mock_ccn,
            executor_selection_policy=UsePredeterminedExecutors(executors),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("executor_servers", [3], indirect=True)
async def test_malicious_executor(
    mock_account: ETHAccount,
    mock_ccn: str,
    executor_servers: Tuple[str],
    malicious_executor: str,
):
    """
    Test that requesting a VRF from a malicious executor fails.
    In this case, the executor will send a random number that does not match the hash
    sent back in /generate.
    """

    nb_bytes = 32
    executors = [Executor(node=Node(address=address)) for address in executor_servers]
    executors.append(Executor(node=Node(address=malicious_executor)))

    with pytest.raises(HashValidationFailed):
        _vrf_response = await generate_vrf(
            account=mock_account,
            nb_executors=len(executors),
            nb_bytes=nb_bytes,
            aleph_api_server=mock_ccn,
            executor_selection_policy=UsePredeterminedExecutors(executors),
        )


async def send_generate_requests_and_call_publish(
    executors: List[Executor],
    request_item_hash: ItemHash,
    request_id: RequestId,
) -> Dict[Executor, PublishedVRFResponseHash]:
    generate_response = await send_generate_requests(
        executors=executors, request_item_hash=request_item_hash, request_id=request_id
    )

    for executor, response in generate_response.items():
        _random_number = await post_node_vrf(
            f"{executor.api_url}/publish/{response.message_hash}",
            PublishedVRFRandomBytes,
        )
        # We're only interested in one response for this test
        break

    return generate_response


@pytest.mark.asyncio
@pytest.mark.parametrize("executor_servers", [3], indirect=True)
@pytest.mark.parametrize("nb_bytes", [32])
async def test_call_publish_before_coordinator(
    mock_account: ETHAccount,
    mock_ccn: str,
    executor_servers: Tuple[str],
    nb_bytes: int,
    mocker,
):
    """
    Mocks an attack attempt where an outsider calls POST /publish on an executor before the coordinator does.
    This should result in the coordinator call failing and an exception to be raised.
    """

    # To simulate an attack, the simplest solution is to patch `send_generate_requests`
    # with a function that does exactly the same and then calls /publish right after generation.
    mocker.patch(
        "aleph_vrf.coordinator.vrf.send_generate_requests",
        send_generate_requests_and_call_publish,
    )
    executors = [Executor(node=Node(address=address)) for address in executor_servers]

    with pytest.raises(RandomNumberPublicationFailed):
        _vrf_response = await generate_vrf(
            account=mock_account,
            nb_executors=len(executors),
            nb_bytes=len(executors),
            aleph_api_server=mock_ccn,
            executor_selection_policy=UsePredeterminedExecutors(executors),
        )
