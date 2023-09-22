from typing import NewType
from uuid import UUID

from pydantic import UUID4

Nonce = NewType("Nonce", int)
RequestId = NewType("RequestId", UUID4)
ExecutionId = NewType("ExecutionId", UUID4)
