from typing import Any, Dict

import pytest
from aleph_message.models import ItemHash

from aleph_vrf.coordinator.executor_selection import ExecuteOnAleph
from aleph_vrf.exceptions import NotEnoughExecutors


@pytest.fixture
def fixture_nodes_aggregate() -> Dict[str, Any]:
    return {
        "address": "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10",
        "data": {
            "corechannel": {
                "resource_nodes": [
                    {
                        "hash": "fa5e90818bf50f358b642ed31361d83a0c6e94a1e07b055764d7e82789437f82",
                        "name": "tokenchain1",
                        "time": 1643038411.233,
                        "type": "compute",
                        "owner": "0x39Fbd6387Ec958FF9829385366AC2bD8DdF65Ef2",
                        "score": 0.9552090385081862,
                        "banner": "",
                        "locked": False,
                        "parent": "6c7578899ac475fbdc05c6a4711331c7590aa6b719f0c169941b99a10faf1136",
                        "reward": "0x39Fbd6387Ec958FF9829385366AC2bD8DdF65Ef2",
                        "status": "linked",
                        "address": "https://a-node-719754-y.tokenchain.network",
                        "manager": "",
                        "picture": "",
                        "authorized": "",
                        "description": "",
                        "performance": 0.9124243072477335,
                        "multiaddress": "",
                        "score_updated": True,
                        "decentralization": 0.9929203762386198,
                        "registration_url": "",
                    },
                    {
                        "hash": "a2d2903fc58f3f031644959226cd17d1a2ba09e4a21e8ef11bf77913fd83899d",
                        "name": "Azateus",
                        "time": 1643047441.046,
                        "type": "compute",
                        "owner": "0x7057C12A7E270B9Db0E4c0d87c23Ba75fC5D82B1",
                        "score": 0,
                        "banner": "",
                        "locked": "",
                        "parent": None,
                        "reward": "0x7057C12A7E270B9Db0E4c0d87c23Ba75fC5D82B1",
                        "status": "waiting",
                        "address": "https://Aleph.ufa.ru",
                        "manager": "",
                        "picture": "",
                        "authorized": "",
                        "description": "",
                        "performance": 0.0,
                        "multiaddress": "",
                        "score_updated": True,
                        "decentralization": 0.9929203762386198,
                        "registration_url": "",
                    },
                    {
                        "hash": "55697b7eefbcc1bdea4bed93b11932668025d5af82acdf17d350f02c9496245f",
                        "name": "ImAleph_0",
                        "time": 1643047955.517,
                        "type": "compute",
                        "owner": "0xB25C7ED25b854a036FE0D96a92059dE9C8391253",
                        "score": 0.9439967672476672,
                        "banner": "",
                        "locked": False,
                        "parent": "a07e5c9e324bfc73f6889202d3eb7b822c0f30490b5084e4ab9f3a49bbca0ad2",
                        "reward": "0xB25C7ED25b854a036FE0D96a92059dE9C8391253",
                        "status": "linked",
                        "address": "https://aleph0.serverrg.eu",
                        "manager": "",
                        "picture": "683b2e0a75dae42b5789da4d33bf959c1b04abe9ebeb3fe880bd839938fe5ac5",
                        "authorized": "",
                        "description": "",
                        "performance": 0.8911298965740464,
                        "multiaddress": "",
                        "score_updated": True,
                        "decentralization": 0.9235073688446255,
                        "registration_url": "",
                    },
                    {
                        "hash": "a653f4f3b2166f20a6bf9b2be9bf14985eeab7525bc66a1fc968bb53761b00d1",
                        "name": "ImAleph_1",
                        "time": 1643048120.789,
                        "type": "compute",
                        "owner": "0xB25C7ED25b854a036FE0D96a92059dE9C8391253",
                        "score": 0.9421971134284096,
                        "banner": "",
                        "locked": False,
                        "parent": "9cbecc86d502a99e710e485266e37b9edab625245c406bfe93d9505a2550bcf8",
                        "reward": "0xB25C7ED25b854a036FE0D96a92059dE9C8391253",
                        "status": "linked",
                        "address": "https://aleph1.serverrg.eu",
                        "manager": "",
                        "picture": "683b2e0a75dae42b5789da4d33bf959c1b04abe9ebeb3fe880bd839938fe5ac5",
                        "authorized": "",
                        "description": "",
                        "performance": 0.8877354005528273,
                        "multiaddress": "",
                        "score_updated": True,
                        "decentralization": 0.9235073688446255,
                        "registration_url": "",
                    },
                    {
                        "hash": "a653f4f3b2166f20a6bf9b2be9bf14985eeab7525bc66a1fc968bb53761b00d1",
                        "name": "ImAleph_1",
                        "time": 1643048120.789,
                        "type": "compute",
                        "owner": "0xB25C7ED25b854a036FE0D96a92059dE9C8391253",
                        "score": 0.9421971134284096,
                        "banner": "",
                        "locked": False,
                        "parent": "9cbecc86d502a99e710e485266e37b9edab625245c406bfe93d9505a2550bcf8",
                        "reward": "0xB25C7ED25b854a036FE0D96a92059dE9C8391253",
                        "status": "linked",
                        "address": "https://aleph2.serverrg.eu",
                        "manager": "",
                        "picture": "683b2e0a75dae42b5789da4d33bf959c1b04abe9ebeb3fe880bd839938fe5ac5",
                        "authorized": "",
                        "description": "",
                        "performance": 0.8877354005528273,
                        "multiaddress": "",
                        "score_updated": True,
                        "decentralization": 0.9235073688446255,
                        "registration_url": "",
                    },
                ],
            }
        },
    }


@pytest.mark.asyncio
async def test_select_random_nodes(fixture_nodes_aggregate: Dict[str, Any], mocker):
    """
    Checks that the ExecuteOnAleph policy is able to select CRNs at random.
    """
    network_fixture = mocker.patch(
        "aleph_vrf.coordinator.executor_selection._get_corechannel_aggregate",
        return_value=fixture_nodes_aggregate,
    )
    executor_selection_policy = ExecuteOnAleph(vm_function=ItemHash("cafe" * 16))

    executors = await executor_selection_policy.select_executors(3)
    # Sanity check, avoid network accesses
    network_fixture.assert_called_once()

    assert len(executors) == 3

    resource_nodes = fixture_nodes_aggregate["data"]["corechannel"]["resource_nodes"]
    with pytest.raises(NotEnoughExecutors) as exception:
        await executor_selection_policy.select_executors(len(resource_nodes))

    assert exception.value.available == 4
    assert exception.value.requested == len(resource_nodes)


@pytest.mark.asyncio
async def test_select_random_nodes_with_unauthorized(
    fixture_nodes_aggregate: Dict[str, Any], mocker
):
    """
    Checks that the blacklisting feature works during node selection.
    """

    network_fixture = mocker.patch(
        "aleph_vrf.coordinator.executor_selection._get_corechannel_aggregate",
        return_value=fixture_nodes_aggregate,
    )
    blacklist = ["https://aleph2.serverrg.eu"]
    executor_selection_policy = ExecuteOnAleph(vm_function=ItemHash("cafe" * 16))
    mocker.patch.object(
        executor_selection_policy, "_get_unauthorized_nodes", return_value=blacklist
    )
    executors = await executor_selection_policy.select_executors(3)
    # Sanity check, avoid network accesses
    network_fixture.assert_called_once()

    assert len(executors) == 3

    for blacklisted_node_address in blacklist:
        assert blacklisted_node_address not in [
            executor.node.address for executor in executors
        ]

    with pytest.raises(NotEnoughExecutors) as exception:
        resource_nodes = fixture_nodes_aggregate["data"]["corechannel"][
            "resource_nodes"
        ]
        _ = await executor_selection_policy.select_executors(len(resource_nodes) - 1)

    assert exception.value.available == 3
    assert exception.value.requested == 4
