import json
import logging
from enum import Enum
from typing import Optional, Dict, Any, List

from aleph_message.status import MessageStatus
from fastapi import FastAPI
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

app = FastAPI()


MESSAGES = {}


@app.get("/messages.json")
async def get_messages(hashes: Optional[str], page: int = 1, pagination: int = 20):
    hashes = hashes.split(",")
    messages = [MESSAGES[item_hash] for item_hash in hashes if item_hash in MESSAGES]
    paginated_messages = messages[(page - 1) * pagination : page * pagination]

    return {
        "messages": paginated_messages,
        "page": page,
        "pagination": pagination,
        "pagination_total": len(messages),
        "pagination_item": "messages",
    }


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


@app.post("/messages", response_model=PubMessageResponse)
async def post_message(message_request: PubMessageRequest):
    global MESSAGES

    format_message(message_request.message_dict)
    MESSAGES[message_request.message_dict["item_hash"]] = message_request.message_dict

    return PubMessageResponse(
        publication_status=PublicationStatus(status="success", failed=[]),
        message_status=MessageStatus.PROCESSED,
    )
