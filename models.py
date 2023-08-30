
from aleph_message.models import ItemHash, PostMessage
from pydantic import BaseModel
from typing import Any, List, Optional
from uuid import uuid4, UUID


class RandomCRN(BaseModel):
    node: str
    url: str


class VRFRequest(BaseModel):
    number_bytes: int
    nonce: int
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
    url: str
    requestId: UUID
    execution_id: UUID
    vrf_request: ItemHash
    random_bytes_hash: str
    message_hash: Optional[str] = None


def generate_response_hash_from_message(message: PostMessage) -> VRFResponseHash:
    content = message.content
    return VRFResponseHash(
        num_bytes=content.num_bytes,
        nonce=content.nonce,
        url=content.url,
        request_id=content.request_id,
        execution_id=content.execution_id,
        vrf_request=ItemHash(content.vrf_request),
        random_bytes_hash=content.random_bytes_hash,
    )


class VRFRandomBytes(BaseModel):
    url: str
    requestId: UUID
    execution_id: UUID
    vrf_request: ItemHash
    random_bytes: str
    random_number: int
    message_hash: Optional[str] = None


class CRNVRFResponse(BaseModel):
    url: str
    random_number: int
    random_bytes: str
    random_bytes_hash: str


class VRFResponse(BaseModel):
    number_bytes: int
    nonce: int
    vrf_function: ItemHash
    requestId: UUID
    nodes: List[CRNVRFResponse]
    random_number: int
    message_hash: Optional[str] = None


class APIResponse(BaseModel):
    error: bool
    data: Any
