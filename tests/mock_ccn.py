"""
A simplified core channel node used for testing to avoid relying on the aleph.im network.
"""

import json
import logging
from enum import Enum
from typing import Any, Dict, List, Optional

from aleph_message.models import ItemHash
from aleph_message.status import MessageStatus
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

app = FastAPI()


MESSAGES: Dict[ItemHash, Dict[str, Any]] = {}


@app.get("/api/v0/messages.json")
async def get_messages(hashes: Optional[str], page: int = 1, pagination: int = 20):
    """
    Mock messages.json endpoint
    """
    hashes = [ItemHash(h) for h in hashes.split(",")] if hashes is not None else []
    messages = [MESSAGES[item_hash] for item_hash in hashes if item_hash in MESSAGES]
    paginated_messages = messages[(page - 1) * pagination : page * pagination]

    return {
        "messages": paginated_messages,
        "pagination_page": page,
        "pagination_per_page": pagination,
        "pagination_total": len(messages),
        "pagination_item": "messages",
    }


class MessageResponse(BaseModel):
    message: Dict[str, Any]
    status: str


@app.get("/api/v0/messages/{item_hash}")
async def get_message(item_hash: str):
    """
    Mock individual view message endpoint
    """
    message = MESSAGES.get(ItemHash(item_hash))
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")

    return MessageResponse(message=message, status="processed")


class PubMessageRequest(BaseModel):
    sync: bool = False
    message_dict: Dict[str, Any] = Field(alias="message")


class Protocol(str, Enum):
    """P2P Protocol"""

    IPFS = "ipfs"
    P2P = "p2p"


class PublicationStatus(BaseModel):
    status: str
    failed: List[Protocol]


class PubMessageResponse(BaseModel):
    publication_status: PublicationStatus
    message_status: Optional[MessageStatus]


def format_message(message_dict: Dict[str, Any]):
    if message_dict["item_type"] == "inline":
        message_dict["content"] = json.loads(message_dict["item_content"])

    message_dict["size"] = len(message_dict["item_content"])


@app.post("/api/v0/messages", response_model=PubMessageResponse)
async def post_message(message_request: PubMessageRequest):
    global MESSAGES

    format_message(message_request.message_dict)
    MESSAGES[message_request.message_dict["item_hash"]] = message_request.message_dict

    return PubMessageResponse(
        publication_status=PublicationStatus(status="success", failed=[]),
        message_status=MessageStatus.PROCESSED,
    )
