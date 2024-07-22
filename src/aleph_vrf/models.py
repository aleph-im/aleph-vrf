from __future__ import annotations

from typing import Generic, List, TypeVar, Union

import fastapi
from aleph_message.models import ItemHash, PostMessage
from aleph_message.models.abstract import HashableModel
from pydantic import BaseModel, ValidationError
from pydantic.generics import GenericModel
from typing_extensions import TypeAlias

from aleph_vrf.types import ExecutionId, Nonce, RequestId


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

    @property
    def test_api_url(self) -> str:
        return f"{self.node.address}/vm/{self.vm_function}/health_check"


class VRFRequest(BaseModel):
    nb_bytes: int
    nb_executors: int
    nonce: Nonce
    vrf_function: ItemHash
    request_id: RequestId
    node_list_hash: str


VRFExecutor: TypeAlias = Union[Executor, AlephExecutor]


def get_vrf_request_from_message(message: PostMessage) -> VRFRequest:
    content = message.content.content
    try:
        return VRFRequest.parse_obj(content)
    except ValidationError as e:
        raise fastapi.HTTPException(
            status_code=422,
            detail=f"Could not parse content of {message.item_hash} as VRF request object: {e.json()}",
        )


class VRFRandomNumberHash(BaseModel):
    nb_bytes: int
    nonce: Nonce
    request_id: RequestId
    execution_id: ExecutionId
    vrf_request: ItemHash
    random_number_hash: str


class PublishedVRFRandomNumberHash(VRFRandomNumberHash):
    """
    A VRF response hash already published on aleph.im.
    Includes the hash of the message published on aleph.im.
    """

    message_hash: ItemHash

    @classmethod
    def from_vrf_response_hash(
        cls, vrf_response_hash: VRFRandomNumberHash, message_hash: ItemHash
    ) -> PublishedVRFRandomNumberHash:
        return cls(
            nb_bytes=vrf_response_hash.nb_bytes,
            nonce=vrf_response_hash.nonce,
            request_id=vrf_response_hash.request_id,
            execution_id=vrf_response_hash.execution_id,
            vrf_request=vrf_response_hash.vrf_request,
            random_number_hash=vrf_response_hash.random_number_hash,
            message_hash=message_hash,
        )

    @classmethod
    def from_published_message(
        cls, message: PostMessage
    ) -> PublishedVRFRandomNumberHash:
        vrf_response_hash = VRFRandomNumberHash.parse_obj(message.content.content)
        return cls(
            nb_bytes=vrf_response_hash.nb_bytes,
            nonce=vrf_response_hash.nonce,
            request_id=vrf_response_hash.request_id,
            execution_id=vrf_response_hash.execution_id,
            vrf_request=vrf_response_hash.vrf_request,
            random_number_hash=vrf_response_hash.random_number_hash,
            message_hash=message.item_hash,
        )


def get_random_number_hash_from_message(
    message: PostMessage,
) -> PublishedVRFRandomNumberHash:
    content = message.content.content
    try:
        response_hash = VRFRandomNumberHash.parse_obj(content)
    except ValidationError as e:
        raise fastapi.HTTPException(
            422,
            detail=f"Could not parse content of {message.item_hash} as VRF response hash object: {e.json()}",
        )

    return PublishedVRFRandomNumberHash.from_vrf_response_hash(
        vrf_response_hash=response_hash, message_hash=message.item_hash
    )


class VRFRandomNumber(BaseModel):
    request_id: RequestId
    execution_id: ExecutionId
    vrf_request: ItemHash
    random_number: str
    random_number_hash: str


class PublishedVRFRandomNumber(VRFRandomNumber):
    message_hash: ItemHash

    @classmethod
    def from_vrf_random_number(
        cls, vrf_random_number: VRFRandomNumber, message_hash: ItemHash
    ) -> PublishedVRFRandomNumber:
        return cls(
            request_id=vrf_random_number.request_id,
            execution_id=vrf_random_number.execution_id,
            vrf_request=vrf_random_number.vrf_request,
            random_number=vrf_random_number.random_number,
            random_number_hash=vrf_random_number.random_number_hash,
            message_hash=message_hash,
        )

    @classmethod
    def from_published_message(cls, message: PostMessage) -> PublishedVRFRandomNumber:
        vrf_random_number = VRFRandomNumber.parse_obj(message.content.content)
        return cls(
            request_id=vrf_random_number.request_id,
            execution_id=vrf_random_number.execution_id,
            vrf_request=vrf_random_number.vrf_request,
            random_number=vrf_random_number.random_number,
            random_number_hash=vrf_random_number.random_number_hash,
            message_hash=message.item_hash,
        )


class ExecutorVRFResponse(BaseModel):
    url: str
    execution_id: ExecutionId
    random_number: str
    random_number_hash: str
    generation_message_hash: ItemHash
    publication_message_hash: ItemHash


class VRFResponse(BaseModel):
    nb_bytes: int
    nb_executors: int
    nonce: Nonce
    vrf_function: ItemHash
    request_id: RequestId
    executors: List[ExecutorVRFResponse]
    random_number: str


class PublishedVRFResponse(VRFResponse):
    message_hash: ItemHash

    @classmethod
    def from_vrf_response(
        cls, vrf_response: VRFResponse, message_hash: ItemHash
    ) -> PublishedVRFResponse:
        return cls(
            nb_bytes=vrf_response.nb_bytes,
            nb_executors=vrf_response.nb_executors,
            nonce=vrf_response.nonce,
            vrf_function=vrf_response.vrf_function,
            request_id=vrf_response.request_id,
            executors=vrf_response.executors,
            random_number=vrf_response.random_number,
            message_hash=message_hash,
        )

    @classmethod
    def from_vrf_post_message(cls, post_message: PostMessage) -> PublishedVRFResponse:
        vrf_response = VRFResponse.parse_obj(post_message.content.content)
        return cls(
            nb_bytes=vrf_response.nb_bytes,
            nb_executors=vrf_response.nb_executors,
            nonce=vrf_response.nonce,
            vrf_function=vrf_response.vrf_function,
            request_id=vrf_response.request_id,
            executors=vrf_response.executors,
            random_number=vrf_response.random_number,
            message_hash=post_message.item_hash,
        )


M = TypeVar("M", bound=BaseModel)


class APIError(BaseModel):
    error: str


class APIResponse(GenericModel, Generic[M]):
    data: M
