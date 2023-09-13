"""
    Dummy conftest.py for aleph_vrf.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    - https://docs.pytest.org/en/stable/fixture.html
    - https://docs.pytest.org/en/stable/writing_plugins.html
"""
import multiprocessing
import socket
import subprocess
from contextlib import contextmanager
from time import sleep

import fastapi.applications
import pytest
import uvicorn

from mock_ccn import app as mock_ccn_app
from aleph_vrf.executor.main import app as executor_app


def run_uvicorn_server(
    app_path: str, host: str = "127.0.0.1", port: int = 8000
) -> subprocess.Popen:
    return subprocess.Popen(["uvicorn", app_path, "--host", host, "--port", str(port)])


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
    app: fastapi.applications.ASGIApp, host: str, port: int
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


@pytest.fixture
def mock_ccn() -> str:
    host, port = "127.0.0.1", 4024
    with run_http_app(app=mock_ccn_app, host=host, port=port):
        yield f"http://{host}:{port}"


@pytest.fixture
def executor_server() -> str:
    host, port = "127.0.0.1", 8081
    with run_http_app(app=executor_app, host=host, port=port):
        yield f"http://{host}:{port}"
