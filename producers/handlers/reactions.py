from proto import message_pb2

from .base import MessageHandler, Topic, register


def reaction_target(body):
    """Type and target cast of a ``ReactionBody``. A reaction to a URL has no
    target cast, and gets fid 0 and an empty hash."""
    return {
        "type": message_pb2.ReactionType.Name(body.type),
        "target_fid": body.target_cast_id.fid,
        "target_hash": body.target_cast_id.hash.hex(),
    }


@register
class ReactionAddHandler(MessageHandler):
    message_type = message_pb2.MESSAGE_TYPE_REACTION_ADD
    topic = Topic.OTHER
    payload_key = "reaction"

    def payload(self, data):
        return reaction_target(data.reaction_body)


@register
class ReactionRemoveHandler(MessageHandler):
    """A remove names the reaction by fid, type and target, not by hash. Snapchain
    lists the add it displaced, if any, in the event's ``deleted_messages``,
    which the transformer copies into the envelope's ``removed`` field."""

    message_type = message_pb2.MESSAGE_TYPE_REACTION_REMOVE
    topic = Topic.OTHER  # same topic as the reaction it removes, so it stays in order
    payload_key = "reaction_remove"

    def payload(self, data):
        return reaction_target(data.reaction_body)
