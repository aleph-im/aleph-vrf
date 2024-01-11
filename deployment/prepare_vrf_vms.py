import argparse
import asyncio
import json
import subprocess
import sys
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional, Tuple, Dict, List

from aleph.sdk import AuthenticatedAlephClient
from aleph.sdk.chains.common import get_fallback_private_key
from aleph.sdk.chains.ethereum import ETHAccount
from aleph_message.models import ItemHash, ProgramMessage
from aleph_message.models.execution.volume import ImmutableVolume
from aleph_message.status import MessageStatus

from aleph_vrf.coordinator.executor_selection import ExecuteOnAleph
from aleph_vrf.coordinator.vrf import prepare_executor_api_request
from aleph_vrf.exceptions import ExecutorError
from aleph_vrf.models import Executor
from aleph_vrf.settings import settings


async def prepare_executor_nodes(executor_item_hash: ItemHash) -> Tuple[List[Executor], List[Executor]]:
    aleph_selection_policy = ExecuteOnAleph(vm_function=executor_item_hash)
    executors = await aleph_selection_policy.get_candidate_executors()
    prepare_tasks = []
    for executor in executors:
        prepare_tasks.append(
            asyncio.create_task(
                prepare_executor_api_request(executor.api_url)
            )
        )

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
    unauthorized_nodes = []
    for unauthorized_executor in unauthorized_executors:
        unauthorized_nodes.append(unauthorized_executor.node.address)

    unauthorized_nodes_list_path = source_dir / "aleph_vrf" / "coordinator" / "unauthorized_node_list.json"
    with open(unauthorized_nodes_list_path, mode="w") as unauthorized_file:
        unauthorized_file.write(json.dumps(unauthorized_nodes))

    print(f"Unauthorized node list file created on {unauthorized_nodes_list_path}")


async def main(args: argparse.Namespace):
    vrf_function = args.vrf_function

    failed_nodes, _ = await prepare_executor_nodes(vrf_function)

    print("Aleph.im VRF VMs nodes prepared.")

    if len(failed_nodes) > 0:
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
