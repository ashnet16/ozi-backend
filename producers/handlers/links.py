from proto import message_pb2

from .base import MessageHandler, Topic, register


@register
class LinkAddHandler(MessageHandler):
    message_type = message_pb2.MESSAGE_TYPE_LINK_ADD
    topic = Topic.OTHER
    payload_key = "link"

    def payload(self, data):
        body = data.link_body
        return {"type": body.type, "target_fid": body.target_fid}
