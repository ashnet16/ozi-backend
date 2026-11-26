from langdetect import detect
from proto import message_pb2

from .base import MessageHandler, Topic, register


@register
class CastAddHandler(MessageHandler):
    message_type = message_pb2.MESSAGE_TYPE_CAST_ADD
    topic = Topic.ADD
    payload_key = "cast"

    def payload(self, data):
        body = data.cast_add_body
        text = body.text
        return {
            "text": text,
            # langdetect raises on text with no letters (e.g. only a URL), which
            # dead-letters the cast. https://github.com/ashnet16/ozi-backend/issues/7
            "language": detect(text) if text and len(text) > 3 else None,
            "mentions": list(body.mentions),
            "embeds": [e.url for e in body.embeds if e.HasField("url")],
        }


@register
class CastRemoveHandler(MessageHandler):
    message_type = message_pb2.MESSAGE_TYPE_CAST_REMOVE
    topic = Topic.ADD  # same topic as the cast it removes, so it stays in order
    payload_key = "cast_remove"

    def validate(self, data):
        if not data.cast_remove_body.target_hash:
            return "CAST_REMOVE has no target hash"
        return None

    def payload(self, data):
        return {"target_hash": data.cast_remove_body.target_hash.hex()}
