from typing import Any, List, Optional
from uuid import UUID, uuid4

from aleph_message.models import ItemHash, PostMessage
from pydantic import BaseModel


class Node(BaseModel):
    hash: str
    address: str
    score: float


class VRFRequest(BaseModel):
    nb_bytes: int
    nonce: int
    vrf_function: ItemHash
    request_id: str
    node_list_hash: str


class VRFGenerationRequest(BaseModel):
    nb_bytes: int
    nonce: int
    request_id: str
    execution_id: str
    vrf_function: ItemHash


def generate_request_from_message(message: PostMessage) -> VRFGenerationRequest:
    content = message.content.content
    return VRFGenerationRequest(
        nb_bytes=content["nb_bytes"],
        nonce=content["nonce"],
        request_id=content["request_id"],
        execution_id=str(uuid4()),
        vrf_function=ItemHash(content["vrf_function"]),
    )


class VRFResponseHash(BaseModel):
    nb_bytes: int
    nonce: int
    request_id: str
    execution_id: str
    vrf_request: ItemHash
    random_bytes_hash: str
    message_hash: Optional[str] = None


def generate_response_hash_from_message(message: PostMessage) -> VRFResponseHash:
    content = message.content.content
    return VRFResponseHash(
        nb_bytes=content["nb_bytes"],
        nonce=content["nonce"],
        request_id=content["request_id"],
        execution_id=content["execution_id"],
        vrf_request=ItemHash(content["vrf_request"]),
        random_bytes_hash=content["random_bytes_hash"],
    )


class VRFRandomBytes(BaseModel):
    request_id: str
    execution_id: str
    vrf_request: ItemHash
    random_bytes: str
    random_bytes_hash: str
    random_number: int
    message_hash: Optional[str] = None


class CRNVRFResponse(BaseModel):
    url: str
    node_hash: str
    execution_id: UUID
    random_number: int
    random_bytes: str
    random_bytes_hash: str
    generation_message_hash: str
    publish_message_hash: str


class VRFResponse(BaseModel):
    nb_bytes: int
    nonce: int
    vrf_function: ItemHash
    request_id: str
    nodes: List[CRNVRFResponse]
    random_number: int
    message_hash: Optional[str] = None


class APIResponse(BaseModel):
    data: Any
