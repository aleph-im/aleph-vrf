import argparse
import asyncio
import subprocess
import sys
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional, Tuple, Dict

from aleph.sdk.client import AuthenticatedAlephHttpClient
from aleph.sdk.chains.common import get_fallback_private_key
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.types import StorageEnum
from aleph_message.models import ItemHash, ProgramMessage
from aleph_message.models.execution.base import Encoding
from aleph_message.models.execution.volume import ImmutableVolume
from aleph_message.status import MessageStatus

from aleph_vrf.settings import settings
from prepare_vrf_vms import prepare_executor_nodes, create_unauthorized_file

# Debian 12 with Aleph SDK 0.9.1
DEBIAN12_RUNTIME = ItemHash(
    # "ed2c37ae857edaea1d36a43fdd0fb9fdb7a2c9394957e6b53d9c94bf67f32ac3" Old Debian 12 runtime with SDK 0.7.0
    "7041de41c6e3de6792b06f44ab4b698616981efde8d229da8d4fceaa43eb7479"
)


def build_venv(package_path: Path, destination: Path) -> None:
    subprocess.run(["pip", "install", "-t", str(destination), package_path], check=True)


def mksquashfs(path: Path, destination: Path) -> None:
    subprocess.run(["mksquashfs", str(path), str(destination)], check=True)


async def upload_dir_as_volume(
    aleph_client: AuthenticatedAlephHttpClient,
    dir_path: Path,
    channel: str,
    volume_path: Optional[Path] = None,
):
    volume_path = volume_path or Path(f"{dir_path}.squashfs")
    if volume_path.exists():
        print("Squashfs volume already exists, updating it...")
        volume_path.unlink()

    mksquashfs(dir_path, volume_path)

    store_message, status = await aleph_client.create_store(
        file_path=volume_path, sync=True, channel=channel, storage_engine=StorageEnum.ipfs
    )
    if status not in (MessageStatus.PENDING, MessageStatus.PROCESSED):
        raise RuntimeError(f"Could not upload venv volume: {status}")

    return store_message.item_hash


async def deploy_python_program(
    aleph_client: AuthenticatedAlephHttpClient,
    code_volume_hash: ItemHash,
    entrypoint: str,
    venv_hash: ItemHash,
    channel: str,
    environment: Optional[Dict[str, str]] = None,
    timeout_seconds: Optional[int] = None,
) -> ProgramMessage:
    program_message, status = await aleph_client.create_program(
        program_ref=code_volume_hash,
        entrypoint=entrypoint,
        encoding=Encoding.squashfs,
        runtime=DEBIAN12_RUNTIME,
        volumes=[
            ImmutableVolume(
                ref=venv_hash,
                use_latest=True,
                mount="/opt/packages",
                comment="Aleph.im VRF virtualenv",
            ).dict()
        ],
        memory=256,
        sync=True,
        environment_variables=environment,
        channel=channel,
        timeout_seconds=timeout_seconds,
    )

    if status == MessageStatus.REJECTED:
        raise RuntimeError("Could not upload program message")

    return program_message


deploy_executor_vm = partial(
    deploy_python_program,
    entrypoint="aleph_vrf.executor.main:app",
)

deploy_coordinator_vm = partial(
    deploy_python_program,
    entrypoint="aleph_vrf.coordinator.main:app",
    timeout_seconds=60,
)


async def deploy_vrf(
        source_dir: Path, venv_dir: Path, deploy_coordinator: bool = True, prepare_nodes: bool = False
) -> Tuple[ProgramMessage, Optional[ProgramMessage]]:
    private_key = get_fallback_private_key()
    account = ETHAccount(private_key)
    channel = "vrf-tests"
    unauthorized_list = False

    async with AuthenticatedAlephHttpClient(
        account=account, api_server=settings.API_HOST
    ) as aleph_client:
        # Upload the code and venv volumes
        print("Uploading code volume...")
        code_volume_hash = await upload_dir_as_volume(
            aleph_client=aleph_client,
            dir_path=source_dir,
            channel=channel,
            volume_path=Path("aleph-vrf-code.squashfs"),
        )
        print("Uploading virtualenv volume...")
        venv_hash = await upload_dir_as_volume(
            aleph_client=aleph_client,
            dir_path=venv_dir,
            channel=channel,
            volume_path=Path("aleph-vrf-venv.squashfs"),
        )

        # Upload the executor and coordinator VMs
        print("Creating executor VM...")
        executor_program_message = await deploy_executor_vm(
            aleph_client=aleph_client,
            code_volume_hash=code_volume_hash,
            venv_hash=venv_hash,
            channel=channel,
            environment={"PYTHONPATH": "/opt/packages"},
        )

        if prepare_nodes:
            failed_nodes, _ = await prepare_executor_nodes(executor_program_message.item_hash)

            if len(failed_nodes) > 0:
                create_unauthorized_file(failed_nodes)
                unauthorized_list = True

        if deploy_coordinator:
            if unauthorized_list:
                # Upload the code volume for coordinator with unauthorized list
                print("Uploading coordinator code volume...")
                code_volume_hash = await upload_dir_as_volume(
                    aleph_client=aleph_client,
                    dir_path=source_dir,
                    channel=channel,
                    volume_path=Path("aleph-vrf-coordinator.squashfs"),
                )

            print("Creating coordinator VM...")
            coordinator_program_message = await deploy_coordinator_vm(
                aleph_client=aleph_client,
                code_volume_hash=code_volume_hash,
                venv_hash=venv_hash,
                channel=channel,
                environment={
                    "PYTHONPATH": "/opt/packages",
                    "ALEPH_VRF_FUNCTION": executor_program_message.item_hash,
                },
            )
        else:
            coordinator_program_message = None

        return executor_program_message, coordinator_program_message


async def main(args: argparse.Namespace):
    deploy_coordinator = args.deploy_coordinator
    prepare_nodes = args.prepare_nodes
    root_dir = Path(__file__).parent.parent

    with TemporaryDirectory() as venv_dir_str:
        venv_dir = Path(venv_dir_str)
        build_venv(package_path=root_dir, destination=venv_dir)

        executor_program_message, coordinator_program_message = await deploy_vrf(
            source_dir=root_dir / "src",
            venv_dir=venv_dir,
            deploy_coordinator=deploy_coordinator,
            prepare_nodes=prepare_nodes,
        )

    print("Aleph.im VRF VMs were successfully deployed.")
    print(
        f"Executor VM: https://api2.aleph.im/api/v0/messages/{executor_program_message.item_hash}"
    )
    if coordinator_program_message:
        print(
            f"Coordinator VM: https://api2.aleph.im/api/v0/messages/{coordinator_program_message.item_hash}"
        )


def parse_args(args) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="deploy_vrf_vms", description="Deploys the VRF VMs on the aleph.im network."
    )
    parser.add_argument(
        "--no-coordinator",
        dest="deploy_coordinator",
        action="store_false",
        default=True,
        help="Deploy the coordinator as an aleph.im VM function",
    )
    parser.add_argument(
        "--prepare-nodes",
        dest="prepare_nodes",
        action="store_true",
        default=False,
        help="Preload executor VMs on Aleph CRNs",
    )
    return parser.parse_args(args)


if __name__ == "__main__":
    asyncio.run(main(args=parse_args(sys.argv[1:])))
