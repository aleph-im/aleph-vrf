from typing import List
from typing import TypeVar, Generic
from uuid import uuid4

import fastapi
from aleph_message.models import ItemHash, PostMessage
from aleph_message.models.abstract import HashableModel
from pydantic import BaseModel
from pydantic import ValidationError, Field
from pydantic.generics import GenericModel

from aleph_vrf.types import Nonce, RequestId, ExecutionId


class Node(HashableModel):
    address: str


class ComputeResourceNode(Node):
    hash: str
    score: float


class Executor(HashableModel):
    node: Node

    @property
    def api_url(self) -> str:
        return self.node.address


class AlephExecutor(Executor):
    node: ComputeResourceNode
    vm_function: str

    @property
    def api_url(self) -> str:
        return f"{self.node.address}/vm/{self.vm_function}"


class VRFRequest(BaseModel):
    nb_bytes: int
    nb_executors: int
    nonce: Nonce
    vrf_function: ItemHash
    request_id: RequestId
    node_list_hash: str


class VRFGenerationRequest(BaseModel):
    nb_bytes: int
    nonce: Nonce
    request_id: RequestId
    execution_id: ExecutionId = Field(default_factory=lambda: str(uuid4()))
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
    nonce: Nonce
    request_id: RequestId
    execution_id: ExecutionId
    vrf_request: ItemHash
    random_bytes_hash: str


class PublishedVRFResponseHash(VRFResponseHash):
    """
    A VRF response hash already published on aleph.im.
    Includes the hash of the message published on aleph.im.
    """

    message_hash: ItemHash

    @classmethod
    def from_vrf_response_hash(
        cls, vrf_response_hash: VRFResponseHash, message_hash: ItemHash
    ) -> "PublishedVRFResponseHash":
        return cls(
            nb_bytes=vrf_response_hash.nb_bytes,
            nonce=vrf_response_hash.nonce,
            request_id=vrf_response_hash.request_id,
            execution_id=vrf_response_hash.execution_id,
            vrf_request=vrf_response_hash.vrf_request,
            random_bytes_hash=vrf_response_hash.random_bytes_hash,
            message_hash=message_hash,
        )


def generate_response_hash_from_message(
    message: PostMessage,
) -> PublishedVRFResponseHash:
    content = message.content.content
    try:
        response_hash = VRFResponseHash.parse_obj(content)
    except ValidationError as e:
        raise fastapi.HTTPException(
            422,
            detail=f"Could not parse content of {message.item_hash} as VRF response hash object: {e.json()}",
        )

    return PublishedVRFResponseHash.from_vrf_response_hash(
        vrf_response_hash=response_hash, message_hash=message.item_hash
    )


class VRFRandomBytes(BaseModel):
    request_id: RequestId
    execution_id: ExecutionId
    vrf_request: ItemHash
    random_bytes: str
    random_bytes_hash: str
    random_number: str


class PublishedVRFRandomBytes(VRFRandomBytes):
    message_hash: ItemHash

    @classmethod
    def from_vrf_random_bytes(
        cls, vrf_random_bytes: VRFRandomBytes, message_hash: ItemHash
    ) -> "PublishedVRFRandomBytes":
        return cls(
            request_id=vrf_random_bytes.request_id,
            execution_id=vrf_random_bytes.execution_id,
            vrf_request=vrf_random_bytes.vrf_request,
            random_bytes=vrf_random_bytes.random_bytes,
            random_bytes_hash=vrf_random_bytes.random_bytes_hash,
            random_number=vrf_random_bytes.random_number,
            message_hash=message_hash,
        )


class CRNVRFResponse(BaseModel):
    url: str
    execution_id: ExecutionId
    random_number: str
    random_bytes: str
    random_bytes_hash: str
    generation_message_hash: ItemHash
    publish_message_hash: ItemHash


class VRFResponse(BaseModel):
    nb_bytes: int
    nb_executors: int
    nonce: Nonce
    vrf_function: ItemHash
    request_id: RequestId
    nodes: List[CRNVRFResponse]
    random_number: str


class PublishedVRFResponse(VRFResponse):
    message_hash: ItemHash

    @classmethod
    def from_vrf_response(
        cls, vrf_response: VRFResponse, message_hash: ItemHash
    ) -> "PublishedVRFResponse":
        return cls(
            nb_bytes=vrf_response.nb_bytes,
            nb_executors=vrf_response.nb_executors,
            nonce=vrf_response.nonce,
            vrf_function=vrf_response.vrf_function,
            request_id=vrf_response.request_id,
            nodes=vrf_response.nodes,
            random_number=vrf_response.random_number,
            message_hash=message_hash,
        )


M = TypeVar("M", bound=BaseModel)


class APIError(BaseModel):
    error: str


class APIResponse(GenericModel, Generic[M]):
    data: M
