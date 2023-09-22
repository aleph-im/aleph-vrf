from typing import NewType
from uuid import UUID

from pydantic import UUID4

Nonce = NewType("Nonce", int)
# TODO: use UUID once https://github.com/aleph-im/aleph-sdk-python/pull/61 is merged
RequestId = NewType("RequestId", str)
ExecutionId = NewType("ExecutionId", str)
