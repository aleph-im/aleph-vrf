"""
    Dummy conftest.py for aleph_vrf.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    - https://docs.pytest.org/en/stable/fixture.html
    - https://docs.pytest.org/en/stable/writing_plugins.html
"""

import multiprocessing
import os
import socket
from contextlib import AsyncExitStack, ExitStack, contextmanager
from time import sleep
from typing import ContextManager, Tuple, Union

import aiohttp
import fastapi.applications
import pytest
import pytest_asyncio
import uvicorn
from aleph.sdk.chains.common import generate_key
from hexbytes import HexBytes
from malicious_executor import app as malicious_executor_app
from mock_ccn import app as mock_ccn_app

from aleph_vrf.settings import settings


def wait_for_server(host: str, port: int, nb_retries: int = 10, wait_time: int = 0.1):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)

    retries = 0
    while retries < nb_retries:
        try:
            sock.connect((host, port))
        except ConnectionError:
            retries += 1
            if retries == nb_retries:
                raise

            sleep(wait_time)
            continue

        break


@contextmanager
def run_http_app(
    app: Union[str, fastapi.applications.ASGIApp], host: str, port: int
) -> ContextManager[multiprocessing.Process]:
    uvicorn_process = multiprocessing.Process(
        target=uvicorn.run, args=(app,), kwargs={"host": host, "port": port}
    )
    uvicorn_process.start()

    try:
        # Wait for uvicorn to start
        wait_for_server(host, port)
        yield uvicorn_process

    finally:
        uvicorn_process.terminate()
        uvicorn_process.join()


@pytest.fixture
def mock_ccn() -> str:
    host, port = "127.0.0.1", 4024
    url = f"http://{host}:{port}"

    default_api_host = settings.API_HOST

    # Configure the mock CCN as API host. Note that `settings` must be modified as the object is
    # already built when running all tests in the same run.
    os.environ["ALEPH_VRF_API_HOST"] = url
    settings.API_HOST = url

    with run_http_app(app=mock_ccn_app, host=host, port=port):
        yield url

    # Clean up settings for other tests
    del os.environ["ALEPH_VRF_API_HOST"]
    settings.API_HOST = default_api_host


@pytest_asyncio.fixture
async def mock_ccn_client(mock_ccn: str) -> aiohttp.ClientSession:
    async with aiohttp.ClientSession(mock_ccn) as client:
        yield client


@contextmanager
def _executor_servers(
    nb_executors: int, host: str = "127.0.0.1", start_port: int = 8081
) -> ContextManager[Tuple[str]]:
    ports = list(range(start_port, start_port + nb_executors))

    # Generate a private key for each executor, using `get_fallback_key()` several times
    # on the same host can cause concurrency problems. All the executors will have the same
    # private key because of this, but this should not be an issue for integration tests.
    settings.ETHEREUM_PRIVATE_KEY = HexBytes(generate_key()).hex()

    with ExitStack() as cm:
        _processes = [
            cm.enter_context(
                run_http_app(app="aleph_vrf.executor.main:app", host=host, port=port)
            )
            for port in ports
        ]
        yield tuple(f"http://{host}:{port}" for port in ports)


@pytest.fixture
def executor_server(mock_ccn: str) -> str:
    """
    Spawns one executor server, configured to use a fake CCN to read/post aleph messages.
    """

    assert mock_ccn, "The mock CCN server must be running"

    with _executor_servers(nb_executors=1) as executor_urls:
        yield executor_urls[0]


@pytest_asyncio.fixture
def executor_servers(mock_ccn: str, request) -> Tuple[str]:
    """
    Spawns N executor servers, using the port range [start_port, start_port + N - 1].
    """

    assert mock_ccn, "The mock CCN server must be running"

    nb_executors = request.param
    with _executor_servers(nb_executors=nb_executors) as executor_urls:
        yield executor_urls


@pytest_asyncio.fixture
async def executor_client(executor_server: str) -> aiohttp.ClientSession:
    """
    Spawns an executor server and provides an aiohttp client to communicate with it.
    """

    async with aiohttp.ClientSession(executor_server) as client:
        yield client


@pytest_asyncio.fixture
async def executor_clients(
    executor_servers: Tuple[str],
) -> Tuple[aiohttp.ClientSession]:
    """
    Spawns N executor servers and provides N aiohttp clients to communicate with them.
    """

    async with AsyncExitStack() as cm:
        clients = [
            cm.enter_async_context(aiohttp.ClientSession(executor_server))
            for executor_server in executor_servers
        ]
        yield clients


@pytest.fixture
def malicious_executor() -> str:
    """
    Spawns an executor that returns an incorrect random number.
    """
    host, port = "127.0.0.1", 9000
    url = f"http://{host}:{port}"

    with run_http_app(app=malicious_executor_app, host=host, port=port):
        yield url
