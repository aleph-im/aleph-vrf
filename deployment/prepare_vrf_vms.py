import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Tuple, List

from aleph_message.models import ItemHash

from aleph_vrf.coordinator.executor_selection import ExecuteOnAleph
from aleph_vrf.coordinator.vrf import prepare_executor_api_request
from aleph_vrf.models import Executor


async def prepare_executor_nodes(executor_item_hash: ItemHash) -> Tuple[List[Executor], List[Executor]]:
    aleph_selection_policy = ExecuteOnAleph(vm_function=executor_item_hash)
    executors = await aleph_selection_policy.get_candidate_executors()
    prepare_tasks = [asyncio.create_task(prepare_executor_api_request(executor.api_url))
                     for executor in executors]

    vrf_prepare_responses = await asyncio.gather(
        *prepare_tasks, return_exceptions=True
    )
    prepare_results = dict(zip(executors, vrf_prepare_responses))

    failed_nodes = []
    success_nodes = []
    for executor, result in prepare_results.items():
        if f"{result}" != "True":
            failed_nodes.append(executor)
        else:
            success_nodes.append(executor)

    return failed_nodes, success_nodes


def create_unauthorized_file(unauthorized_executors: List[Executor]):
    source_dir = Path(__file__).parent.parent / "src"
    unauthorized_nodes = [executor.node.address for executor in unauthorized_executors]
    unauthorized_nodes_list_path = source_dir / "aleph_vrf" / "coordinator" / "unauthorized_node_list.json"
    unauthorized_nodes_list_path.write_text(json.dumps(unauthorized_nodes))

    print(f"Unauthorized node list file created on {unauthorized_nodes_list_path}")


async def main(args: argparse.Namespace):
    vrf_function = args.vrf_function

    failed_nodes, _ = await prepare_executor_nodes(vrf_function)

    print("Aleph.im VRF VMs nodes prepared.")

    if failed_nodes:
        print(f"{len(failed_nodes)} preload nodes failed.")
        create_unauthorized_file(failed_nodes)


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
    return parser.parse_args(args)


if __name__ == "__main__":
    asyncio.run(main(args=parse_args(sys.argv[1:])))
