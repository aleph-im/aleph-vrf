import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Tuple, List, Optional

from aleph.sdk.client import AuthenticatedAlephHttpClient
from aleph_message.models import ItemHash

from aleph_vrf.coordinator.executor_selection import ExecuteOnAleph
from aleph_vrf.coordinator.vrf import prepare_executor_api_request, prepare_executor_health_check
from aleph_vrf.models import Executor
from aleph_vrf.settings import settings


async def prepare_executor_nodes(executor_item_hash: ItemHash) -> Tuple[List[Executor], List[Executor]]:
    aleph_selection_policy = ExecuteOnAleph(vm_function=executor_item_hash)
    executors = await aleph_selection_policy.get_all_executors()
    print(f"Preparing VRF VMs for {len(executors)} nodes")
    prepare_tasks = [asyncio.create_task(prepare_executor_api_request(executor.api_url))
                     for executor in executors]

    await asyncio.gather(
        *prepare_tasks, return_exceptions=True
    )

    prepare_tasks = [asyncio.create_task(prepare_executor_health_check(executor.test_api_url))
                     for executor in executors]

    vrf_health_responses = await asyncio.gather(
        *prepare_tasks, return_exceptions=True
    )

    prepare_results = dict(zip(executors, vrf_health_responses))

    failed_nodes = []
    success_nodes = []
    for executor, result in prepare_results.items():
        if f"{result}" != "True":
            failed_nodes.append(executor)
        else:
            success_nodes.append(executor)

    return failed_nodes, success_nodes


def create_unauthorized_file(unauthorized_executors: List[Executor], destination_path: Optional[Path] = None):
    if not destination_path:
        source_dir = Path(__file__).parent.parent / "src"
        destination_path = source_dir / "aleph_vrf" / "coordinator"

    unauthorized_nodes = [executor.node.address for executor in unauthorized_executors]
    unauthorized_nodes_list_path = destination_path / "unauthorized_node_list.json"
    unauthorized_nodes_list_path.write_text(json.dumps(unauthorized_nodes))

    print(f"Unauthorized node list file created on {unauthorized_nodes_list_path}")


async def update_unauthorized_aggregate(unauthorized_executors: List[Executor]):
    account = settings.aleph_account()
    async with AuthenticatedAlephHttpClient(
            account=account,
            api_server=settings.API_HOST,
            # Avoid going through the VM connector on aleph.im CRNs
            allow_unix_sockets=False,
    ) as client:
        node_list = [executor.node.address for executor in unauthorized_executors]
        content = {
            "unauthorized_nodes": node_list
        }
        await client.create_aggregate(key="vrf", content=content)


async def main(args: argparse.Namespace):
    vrf_function = args.vrf_function

    source_dir = Path(__file__).parent.parent / "src"
    unauthorized_nodes_list_path = source_dir / "aleph_vrf" / "coordinator"

    failed_nodes, _ = await prepare_executor_nodes(vrf_function)

    print("Aleph.im VRF VMs nodes prepared.")

    if failed_nodes:
        print(f"{len(failed_nodes)} preload nodes failed.")
        create_unauthorized_file(failed_nodes, unauthorized_nodes_list_path)
        if args.save_in_aggregate:
            account = settings.aleph_account()
            await update_unauthorized_aggregate(failed_nodes)
            print(f"Unauthorized node list saved on an aggregate on address {account.get_address()}")


def parse_args(args) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="prepare_vrf_vms", description="Prepare executor VRF VMs on the aleph.im network."
    )
    parser.add_argument(
        "--vrf_function",
        dest="vrf_function",
        action="store",
        help="VRF VM ItemHash",
    )
    parser.add_argument(
        "--save-aggregate",
        dest="save_in_aggregate",
        action="store_true",
        help="Save node VRF list on an aggregate",
    )
    return parser.parse_args(args)


if __name__ == "__main__":
    asyncio.run(main(args=parse_args(sys.argv[1:])))
