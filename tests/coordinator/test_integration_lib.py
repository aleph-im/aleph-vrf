import pytest


@pytest.mark.parametrize("executor_servers", [2, 3], indirect=True)
def test_executors(executor_servers):
    ...
