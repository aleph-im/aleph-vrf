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
from contextlib import contextmanager
from time import sleep
from typing import Union

import aiohttp
import fastapi.applications
import pytest
import pytest_asyncio
import uvicorn

from aleph_vrf.settings import settings
from mock_ccn import app as mock_ccn_app


def wait_for_server(host: str, port: int, nb_retries: int = 3, wait_time: int = 0.1):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)

    retries = 0
    while retries < nb_retries:
        try:
            sock.connect((host, port))
        except ConnectionError:
            retries += 1
            sleep(wait_time)
            continue

        break


@contextmanager
def run_http_app(
    app: Union[str, fastapi.applications.ASGIApp], host: str, port: int
) -> multiprocessing.Process:
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
async def mock_ccn_client(mock_ccn: str):
    async with aiohttp.ClientSession(mock_ccn) as client:
        yield client


@pytest.fixture
def executor_server(mock_ccn: str) -> str:
    assert mock_ccn, "The mock CCN server must be running"

    host, port = "127.0.0.1", 8081
    with run_http_app(app="aleph_vrf.executor.main:app", host=host, port=port):
        yield f"http://{host}:{port}"


@pytest_asyncio.fixture
async def executor_client(executor_server: str) -> aiohttp.ClientSession:
    async with aiohttp.ClientSession(executor_server) as client:
        yield client
