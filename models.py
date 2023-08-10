
from aleph_message.models import ItemHash, PostMessage
from pydantic import BaseModel
from typing import Any, List
from uuid import uuid4, UUID


class RandomCRN(BaseModel):
    node: str
    url: str


class VRFRequest(BaseModel):
    number_bytes: int
    requestId: UUID
    vrf_function: ItemHash
    nodes: List[RandomCRN]

    def __int__(self):
        super().__init__(self)
        self.requestId = uuid4()


class VRFGenerationRequest(BaseModel):
    num_bytes: int
    request_id: UUID
    execution_id: UUID
    vrf_function: ItemHash
    nonce: int

    def __int__(self):
        super().__init__(self)
        self.execution_id = uuid4()


def generate_request_from_message(message: PostMessage):
    content = message.content
    return VRFGenerationRequest(
        num_bytes=content.num_bytes,
        request_id=content.request_id,
        vrf_function=ItemHash(content.vrf_function),
        nonce=content.nonce,
    )


class VRFResponseHash(BaseModel):
    requestId: UUID
    execution_id: UUID
    random_bytes_hash: str


class VRFRandomBytes(BaseModel):
    requestId: UUID
    execution_id: UUID
    random_bytes: bytes


class APIResponse(BaseModel):
    error: bool
    data: Any
