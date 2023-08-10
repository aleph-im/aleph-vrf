
from aleph_message.models import ItemHash, PostMessage
from pydantic import BaseModel
from typing import Any, List
from uuid import uuid4, UUID


class RandomCRN(BaseModel):
    node: str
    url: str


class VRFRequest(BaseModel):
    number_bytes: int
    vrf_function: ItemHash
    requestId: UUID
    nodes: List[RandomCRN]

    def __int__(self):
        super().__init__(self)
        self.requestId = uuid4()


class VRFGenerationRequest(BaseModel):
    num_bytes: int
    nonce: int
    request_id: UUID
    execution_id: UUID
    vrf_function: ItemHash

    def __int__(self):
        super().__init__(self)
        self.execution_id = uuid4()


def generate_request_from_message(message: PostMessage) -> VRFGenerationRequest:
    content = message.content
    return VRFGenerationRequest(
        num_bytes=content.num_bytes,
        nonce=content.nonce,
        request_id=content.request_id,
        vrf_function=ItemHash(content.vrf_function),
    )


class VRFResponseHash(BaseModel):
    num_bytes: int
    nonce: int
    requestId: UUID
    execution_id: UUID
    vrf_request: ItemHash
    random_bytes_hash: str


def generate_response_hash_from_message(message: PostMessage) -> VRFResponseHash:
    content = message.content
    return VRFResponseHash(
        num_bytes=content.num_bytes,
        nonce=content.nonce,
        request_id=content.request_id,
        execution_id=content.execution_id,
        vrf_request=ItemHash(content.vrf_request),
        random_bytes_hash=content.random_bytes_hash,
    )


class VRFRandomBytes(BaseModel):
    requestId: UUID
    execution_id: UUID
    vrf_request: ItemHash
    random_bytes: bytes


class APIResponse(BaseModel):
    error: bool
    data: Any
