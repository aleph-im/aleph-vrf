from typing import List, Optional, TypeVar, Generic
from uuid import uuid4

import fastapi
from aleph_message.models import ItemHash, PostMessage
from pydantic import BaseModel, ValidationError, Field
from pydantic.generics import GenericModel


class Node(BaseModel):
    hash: str
    address: str
    score: float


class VRFRequest(BaseModel):
    nb_bytes: int
    nb_executors: int
    nonce: int
    vrf_function: ItemHash
    request_id: str
    node_list_hash: str


class VRFGenerationRequest(BaseModel):
    nb_bytes: int
    nonce: int
    request_id: str
    execution_id: str = Field(default_factory=lambda: str(uuid4()))
    vrf_function: ItemHash


def generate_request_from_message(message: PostMessage) -> VRFGenerationRequest:
    content = message.content.content
    try:
        return VRFGenerationRequest.parse_obj(content)
    except ValidationError as e:
        raise fastapi.HTTPException(
            status_code=422,
            detail=f"Could not parse content of {message.item_hash} as VRF request object: {e.json()}",
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
    try:
        response_hash = VRFResponseHash.parse_obj(content)
    except ValidationError as e:
        raise fastapi.HTTPException(
            422,
            detail=f"Could not parse content of {message.item_hash} as VRF response hash object: {e.json()}",
        )

    response_hash.message_hash = message.item_hash
    return response_hash


class VRFRandomBytes(BaseModel):
    request_id: str
    execution_id: str
    vrf_request: ItemHash
    random_bytes: str
    random_bytes_hash: str
    random_number: str
    message_hash: Optional[str] = None


class CRNVRFResponse(BaseModel):
    url: str
    execution_id: str
    random_number: str
    random_bytes: str
    random_bytes_hash: str
    generation_message_hash: str
    publish_message_hash: str


class VRFResponse(BaseModel):
    nb_bytes: int
    nb_executors: int
    nonce: int
    vrf_function: ItemHash
    request_id: str
    nodes: List[CRNVRFResponse]
    random_number: str
    message_hash: Optional[str] = None


M = TypeVar("M", bound=BaseModel)


class APIResponse(GenericModel, Generic[M]):
    data: M
