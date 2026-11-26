"""The handler interface and the registry the transformer looks handlers up in."""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Optional

from producers.config import ANALYTICS_TOPIC, DLQ_TOPIC, EMBEDDER_TOPIC


class Topic(str, Enum):
    """The Kafka topics the poller publishes to."""

    ADD = EMBEDDER_TOPIC  # casts and cast removes: read by both consumers
    OTHER = ANALYTICS_TOPIC  # everything else that is handled: read by analytics
    DLQ = DLQ_TOPIC  # unhandled or unusable events


class MessageHandler(ABC):
    """Turns one Snapchain message type into the type-specific part of an envelope.

    The transformer builds the fields every message shares (type, fid, timestamp,
    hash, raw event, removed messages) and asks the handler only for its own
    payload, stored under ``payload_key``, and for the topic to publish to.
    """

    message_type: int  # message_pb2.MESSAGE_TYPE_*
    topic: Topic  # where events of this type are published
    payload_key: str  # envelope field: "cast", "reaction", ...

    @abstractmethod
    def payload(self, data) -> dict:
        """The type-specific part of the envelope, from a ``MessageData``."""

    def validate(self, data) -> Optional[str]:
        """A dead-letter reason, or None if the message is usable."""
        return None


HANDLERS: Dict[int, MessageHandler] = {}


def register(cls):
    """Class decorator: route messages of ``cls.message_type`` to an instance of it."""
    if cls.message_type in HANDLERS:
        raise ValueError(
            f"{cls.__name__}: message type {cls.message_type} already has a handler,"
            f" {type(HANDLERS[cls.message_type]).__name__}"
        )
    HANDLERS[cls.message_type] = cls()
    return cls
