from typing import Tuple

import aiohttp
import pytest


@pytest.mark.asyncio
async def test_mock(mock_ccn: str):
    """
    Sanity check test: verify that the mock CCN can be started.
    """

    async with aiohttp.ClientSession(mock_ccn) as ccn_client:
        messages = await ccn_client.get("/messages")
        assert messages


@pytest.mark.asyncio
async def test_start_request(mock_ccn: str, executor_server: str):
    async with aiohttp.ClientSession(mock_ccn) as ccn_client:
